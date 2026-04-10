package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.DateInterval;
import com.jigyasa.dp.search.protocol.FacetBucket;
import com.jigyasa.dp.search.protocol.FacetRequest;
import com.jigyasa.dp.search.protocol.FacetResult;
import com.jigyasa.dp.search.protocol.FacetSortOrder;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Computes facets by iterating DocValues directly — no Lucene FacetsConfig or
 * SortedSetDocValuesFacetField needed. Reuses existing $o/$f DocValues fields.
 *
 * Two internal paths (transparent to caller):
 *   1. MatchAll (fc == null): full sequential DocValues column scan (skips deleted docs).
 *   2. Filtered (fc != null): iterate only matching docs from FacetsCollector.
 */
public class FacetExecutor {
    private static final Logger log = LoggerFactory.getLogger(FacetExecutor.class);
    private static final int DEFAULT_FACET_COUNT = 10;
    private static final int MAX_RANGE_BUCKETS = 10_000;

    /**
     * @param searcher the index searcher
     * @param fc       FacetsCollector with matching docs, or null for MatchAll path
     * @param schema   initialized schema for field lookup
     * @param requests facet requests from the query
     * @return map of field name → FacetResult
     */
    public Map<String, FacetResult> compute(IndexSearcher searcher,
                                             FacetsCollector fc,
                                             InitializedIndexSchema schema,
                                             java.util.List<FacetRequest> requests) throws IOException {
        Map<String, FacetResult> results = new LinkedHashMap<>();

        for (FacetRequest req : requests) {
            String fieldName = req.getField();
            SchemaField field = schema.getFieldLookupMap().get(fieldName);

            if (field == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
            if (!field.isFacetable()) {
                throw new IllegalArgumentException("Field '" + fieldName + "' is not facetable");
            }

            boolean hasInterval = req.getInterval() > 0;
            boolean hasValues = req.getValuesCount() > 0;
            boolean hasDateInterval = req.getDateInterval() != DateInterval.DATE_INTERVAL_UNSPECIFIED;
            if (hasInterval && hasValues) {
                throw new IllegalArgumentException(
                        "Field '" + fieldName + "': interval and values are mutually exclusive");
            }

            String dvFieldName = resolveDvFieldName(field);
            FieldDataType type = field.getType();

            try {
                FacetResult result;
                if (isStringOrBoolean(type)) {
                    result = computeStringTermsFacet(searcher, dvFieldName, type, fc, req);
                } else if (isDateTimeOffset(type) && hasDateInterval) {
                    result = computeDateIntervalFacet(searcher, dvFieldName, type, fc, req);
                } else if (isNumericOrDate(type) && hasInterval) {
                    result = computeNumericRangeFacet(searcher, dvFieldName, type, fc, req);
                } else if (isNumericOrDate(type)) {
                    result = computeNumericTermsFacet(searcher, dvFieldName, type, fc, req);
                } else {
                    throw new IllegalArgumentException(
                            "Faceting not supported for field type: " + type);
                }
                results.put(fieldName, result);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error("Facet computation failed for field '{}'", fieldName, e);
                results.put(fieldName, FacetResult.getDefaultInstance());
            }
        }
        return results;
    }

    // =====================================================================
    //  String / Boolean terms faceting
    // =====================================================================

    private FacetResult computeStringTermsFacet(IndexSearcher searcher, String dvFieldName,
                                                 FieldDataType type, FacetsCollector fc,
                                                 FacetRequest req) throws IOException {
        Map<String, Long> counts = new HashMap<>();
        boolean isCollection = FieldDataType.isCollection(type);

        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                if (isCollection) {
                    countAllSortedSet(ctx, dvFieldName, liveDocs, counts);
                } else {
                    countAllSorted(ctx, dvFieldName, liveDocs, counts);
                }
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                if (isCollection) {
                    countMatchingSortedSet(md, dvFieldName, counts);
                } else {
                    countMatchingSorted(md, dvFieldName, counts);
                }
            }
        }

        return buildTermsFacetResult(counts, req, false);
    }

    /** Full-scan single-valued string DV. Counts by ordinal then resolves. */
    private void countAllSorted(LeafReaderContext ctx, String dvFieldName,
                                 Bits liveDocs, Map<String, Long> globalCounts) throws IOException {
        SortedDocValues sdv = ctx.reader().getSortedDocValues(dvFieldName);
        if (sdv == null) return;

        int valueCount = sdv.getValueCount();
        int[] localCounts = new int[valueCount];

        for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
            if (liveDocs != null && !liveDocs.get(docId)) continue;
            if (sdv.advanceExact(docId)) {
                localCounts[sdv.ordValue()]++;
            }
        }

        for (int ord = 0; ord < valueCount; ord++) {
            if (localCounts[ord] > 0) {
                String term = sdv.lookupOrd(ord).utf8ToString();
                globalCounts.merge(term, (long) localCounts[ord], Long::sum);
            }
        }
    }

    /** Full-scan multi-valued string DV. Uses ordinal counting. */
    private void countAllSortedSet(LeafReaderContext ctx, String dvFieldName,
                                    Bits liveDocs, Map<String, Long> globalCounts) throws IOException {
        SortedSetDocValues ssdv = ctx.reader().getSortedSetDocValues(dvFieldName);
        if (ssdv == null) return;

        long valueCount = ssdv.getValueCount();
        // Ordinal array: up to 16MB (4M entries × 4 bytes). Beyond that, use long[] or HashMap.
        if (valueCount <= 4_000_000) {
            int[] localCounts = new int[(int) valueCount];
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                if (liveDocs != null && !liveDocs.get(docId)) continue;
                if (ssdv.advanceExact(docId)) {
                    for (int i = 0; i < ssdv.docValueCount(); i++) {
                        localCounts[(int) ssdv.nextOrd()]++;
                    }
                }
            }
            for (int ord = 0; ord < valueCount; ord++) {
                if (localCounts[ord] > 0) {
                    String term = ssdv.lookupOrd(ord).utf8ToString();
                    globalCounts.merge(term, (long) localCounts[ord], Long::sum);
                }
            }
        } else {
            // High cardinality: still count by ordinal but with long[] to avoid int overflow
            long[] localCounts = new long[(int) Math.min(valueCount, 50_000_000L)];
            boolean useFallback = valueCount > 50_000_000L;
            if (useFallback) {
                // Extreme cardinality (>50M): resolve per doc
                for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                    if (liveDocs != null && !liveDocs.get(docId)) continue;
                    if (ssdv.advanceExact(docId)) {
                        for (int i = 0; i < ssdv.docValueCount(); i++) {
                            String term = ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString();
                            globalCounts.merge(term, 1L, Long::sum);
                        }
                    }
                }
            } else {
                for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                    if (liveDocs != null && !liveDocs.get(docId)) continue;
                    if (ssdv.advanceExact(docId)) {
                        for (int i = 0; i < ssdv.docValueCount(); i++) {
                            localCounts[(int) ssdv.nextOrd()]++;
                        }
                    }
                }
                for (int ord = 0; ord < localCounts.length; ord++) {
                    if (localCounts[ord] > 0) {
                        String term = ssdv.lookupOrd(ord).utf8ToString();
                        globalCounts.merge(term, localCounts[ord], Long::sum);
                    }
                }
            }
        }
    }

    /** Filtered single-valued string DV. */
    private void countMatchingSorted(FacetsCollector.MatchingDocs md, String dvFieldName,
                                      Map<String, Long> counts) throws IOException {
        SortedDocValues sdv = md.context().reader().getSortedDocValues(dvFieldName);
        if (sdv == null) return;

        DocIdSetIterator iter = md.bits().iterator();
        int docId;
        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (sdv.advanceExact(docId)) {
                String term = sdv.lookupOrd(sdv.ordValue()).utf8ToString();
                counts.merge(term, 1L, Long::sum);
            }
        }
    }

    /** Filtered multi-valued string DV. */
    private void countMatchingSortedSet(FacetsCollector.MatchingDocs md, String dvFieldName,
                                         Map<String, Long> counts) throws IOException {
        SortedSetDocValues ssdv = md.context().reader().getSortedSetDocValues(dvFieldName);
        if (ssdv == null) return;

        DocIdSetIterator iter = md.bits().iterator();
        int docId;
        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (ssdv.advanceExact(docId)) {
                for (int i = 0; i < ssdv.docValueCount(); i++) {
                    String term = ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString();
                    counts.merge(term, 1L, Long::sum);
                }
            }
        }
    }

    // =====================================================================
    //  Numeric terms faceting (single-valued AND multi-valued)
    // =====================================================================

    private FacetResult computeNumericTermsFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        boolean isDouble = (type == FieldDataType.DOUBLE || type == FieldDataType.DOUBLE_COLLECTION);
        boolean isCollection = FieldDataType.isCollection(type);
        Map<String, Long> counts = new HashMap<>();

        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                if (isCollection) {
                    countAllSortedNumeric(ctx, dvFieldName, liveDocs, isDouble, counts);
                } else {
                    countAllNumeric(ctx, dvFieldName, liveDocs, isDouble, counts);
                }
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                if (isCollection) {
                    countMatchingSortedNumeric(md, dvFieldName, isDouble, counts);
                } else {
                    countMatchingNumeric(md, dvFieldName, isDouble, counts);
                }
            }
        }

        return buildTermsFacetResult(counts, req, !isDouble);
    }

    private void countAllNumeric(LeafReaderContext ctx, String dvFieldName,
                                  Bits liveDocs, boolean isDouble,
                                  Map<String, Long> counts) throws IOException {
        NumericDocValues ndv = ctx.reader().getNumericDocValues(dvFieldName);
        if (ndv == null) return;
        for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
            if (liveDocs != null && !liveDocs.get(docId)) continue;
            if (ndv.advanceExact(docId)) {
                counts.merge(formatNumericValue(ndv.longValue(), isDouble), 1L, Long::sum);
            }
        }
    }

    private void countAllSortedNumeric(LeafReaderContext ctx, String dvFieldName,
                                        Bits liveDocs, boolean isDouble,
                                        Map<String, Long> counts) throws IOException {
        SortedNumericDocValues sndv = ctx.reader().getSortedNumericDocValues(dvFieldName);
        if (sndv == null) return;
        for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
            if (liveDocs != null && !liveDocs.get(docId)) continue;
            if (sndv.advanceExact(docId)) {
                for (int i = 0; i < sndv.docValueCount(); i++) {
                    counts.merge(formatNumericValue(sndv.nextValue(), isDouble), 1L, Long::sum);
                }
            }
        }
    }

    private void countMatchingNumeric(FacetsCollector.MatchingDocs md, String dvFieldName,
                                       boolean isDouble, Map<String, Long> counts) throws IOException {
        NumericDocValues ndv = md.context().reader().getNumericDocValues(dvFieldName);
        if (ndv == null) return;
        DocIdSetIterator iter = md.bits().iterator();
        int docId;
        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (ndv.advanceExact(docId)) {
                counts.merge(formatNumericValue(ndv.longValue(), isDouble), 1L, Long::sum);
            }
        }
    }

    private void countMatchingSortedNumeric(FacetsCollector.MatchingDocs md, String dvFieldName,
                                             boolean isDouble, Map<String, Long> counts) throws IOException {
        SortedNumericDocValues sndv = md.context().reader().getSortedNumericDocValues(dvFieldName);
        if (sndv == null) return;
        DocIdSetIterator iter = md.bits().iterator();
        int docId;
        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (sndv.advanceExact(docId)) {
                for (int i = 0; i < sndv.docValueCount(); i++) {
                    counts.merge(formatNumericValue(sndv.nextValue(), isDouble), 1L, Long::sum);
                }
            }
        }
    }

    // =====================================================================
    //  Numeric range faceting (interval) — streaming two-pass, no OOM
    // =====================================================================

    private FacetResult computeNumericRangeFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        boolean isDouble = (type == FieldDataType.DOUBLE || type == FieldDataType.DOUBLE_COLLECTION);
        boolean isCollection = FieldDataType.isCollection(type);
        double interval = req.getInterval();

        // Pass 1: streaming min/max (no value storage)
        double[] minMax = {Double.MAX_VALUE, -Double.MAX_VALUE};
        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                streamMinMax(ctx, dvFieldName, isCollection, isDouble, liveDocs, minMax);
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                streamMinMaxMatching(md, dvFieldName, isCollection, isDouble, minMax);
            }
        }

        if (minMax[0] > minMax[1]) {
            return FacetResult.getDefaultInstance(); // no values found
        }

        // Build bucket boundaries
        double bucketStart = Math.floor(minMax[0] / interval) * interval;
        long numBuckets = (long) Math.ceil((minMax[1] - bucketStart) / interval);
        if (numBuckets > MAX_RANGE_BUCKETS) {
            throw new IllegalArgumentException(
                    "interval " + interval + " produces " + numBuckets
                            + " buckets (max " + MAX_RANGE_BUCKETS + "). Use a larger interval.");
        }

        long[] bucketCounts = new long[(int) numBuckets];

        // Pass 2: streaming bucket counting
        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                streamBucketCount(ctx, dvFieldName, isCollection, isDouble, liveDocs,
                        bucketStart, interval, bucketCounts);
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                streamBucketCountMatching(md, dvFieldName, isCollection, isDouble,
                        bucketStart, interval, bucketCounts);
            }
        }

        // Build result
        FacetResult.Builder result = FacetResult.newBuilder();
        for (int i = 0; i < bucketCounts.length; i++) {
            if (bucketCounts[i] > 0) {
                double from = bucketStart + i * interval;
                double to = from + interval;
                result.addBuckets(FacetBucket.newBuilder()
                        .setValue(formatNumber(from, isDouble) + "-" + formatNumber(to, isDouble))
                        .setCount(bucketCounts[i])
                        .setFrom(formatNumber(from, isDouble))
                        .setTo(formatNumber(to, isDouble))
                        .build());
            }
        }
        return result.build();
    }

    private void streamMinMax(LeafReaderContext ctx, String dvFieldName, boolean isCollection,
                               boolean isDouble, Bits liveDocs, double[] minMax) throws IOException {
        if (isCollection) {
            SortedNumericDocValues sndv = ctx.reader().getSortedNumericDocValues(dvFieldName);
            if (sndv == null) return;
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                if (liveDocs != null && !liveDocs.get(docId)) continue;
                if (sndv.advanceExact(docId)) {
                    for (int i = 0; i < sndv.docValueCount(); i++) {
                        double val = rawToDouble(sndv.nextValue(), isDouble);
                        if (val < minMax[0]) minMax[0] = val;
                        if (val > minMax[1]) minMax[1] = val;
                    }
                }
            }
        } else {
            NumericDocValues ndv = ctx.reader().getNumericDocValues(dvFieldName);
            if (ndv == null) return;
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                if (liveDocs != null && !liveDocs.get(docId)) continue;
                if (ndv.advanceExact(docId)) {
                    double val = rawToDouble(ndv.longValue(), isDouble);
                    if (val < minMax[0]) minMax[0] = val;
                    if (val > minMax[1]) minMax[1] = val;
                }
            }
        }
    }

    private void streamMinMaxMatching(FacetsCollector.MatchingDocs md, String dvFieldName,
                                       boolean isCollection, boolean isDouble,
                                       double[] minMax) throws IOException {
        if (isCollection) {
            SortedNumericDocValues sndv = md.context().reader().getSortedNumericDocValues(dvFieldName);
            if (sndv == null) return;
            DocIdSetIterator iter = md.bits().iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (sndv.advanceExact(docId)) {
                    for (int i = 0; i < sndv.docValueCount(); i++) {
                        double val = rawToDouble(sndv.nextValue(), isDouble);
                        if (val < minMax[0]) minMax[0] = val;
                        if (val > minMax[1]) minMax[1] = val;
                    }
                }
            }
        } else {
            NumericDocValues ndv = md.context().reader().getNumericDocValues(dvFieldName);
            if (ndv == null) return;
            DocIdSetIterator iter = md.bits().iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (ndv.advanceExact(docId)) {
                    double val = rawToDouble(ndv.longValue(), isDouble);
                    if (val < minMax[0]) minMax[0] = val;
                    if (val > minMax[1]) minMax[1] = val;
                }
            }
        }
    }

    private void streamBucketCount(LeafReaderContext ctx, String dvFieldName, boolean isCollection,
                                    boolean isDouble, Bits liveDocs,
                                    double bucketStart, double interval, long[] bucketCounts) throws IOException {
        if (isCollection) {
            SortedNumericDocValues sndv = ctx.reader().getSortedNumericDocValues(dvFieldName);
            if (sndv == null) return;
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                if (liveDocs != null && !liveDocs.get(docId)) continue;
                if (sndv.advanceExact(docId)) {
                    for (int i = 0; i < sndv.docValueCount(); i++) {
                        incrementBucket(rawToDouble(sndv.nextValue(), isDouble),
                                bucketStart, interval, bucketCounts);
                    }
                }
            }
        } else {
            NumericDocValues ndv = ctx.reader().getNumericDocValues(dvFieldName);
            if (ndv == null) return;
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                if (liveDocs != null && !liveDocs.get(docId)) continue;
                if (ndv.advanceExact(docId)) {
                    incrementBucket(rawToDouble(ndv.longValue(), isDouble),
                            bucketStart, interval, bucketCounts);
                }
            }
        }
    }

    private void streamBucketCountMatching(FacetsCollector.MatchingDocs md, String dvFieldName,
                                            boolean isCollection, boolean isDouble,
                                            double bucketStart, double interval,
                                            long[] bucketCounts) throws IOException {
        if (isCollection) {
            SortedNumericDocValues sndv = md.context().reader().getSortedNumericDocValues(dvFieldName);
            if (sndv == null) return;
            DocIdSetIterator iter = md.bits().iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (sndv.advanceExact(docId)) {
                    for (int i = 0; i < sndv.docValueCount(); i++) {
                        incrementBucket(rawToDouble(sndv.nextValue(), isDouble),
                                bucketStart, interval, bucketCounts);
                    }
                }
            }
        } else {
            NumericDocValues ndv = md.context().reader().getNumericDocValues(dvFieldName);
            if (ndv == null) return;
            DocIdSetIterator iter = md.bits().iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (ndv.advanceExact(docId)) {
                    incrementBucket(rawToDouble(ndv.longValue(), isDouble),
                            bucketStart, interval, bucketCounts);
                }
            }
        }
    }

    private static void incrementBucket(double val, double bucketStart, double interval,
                                         long[] bucketCounts) {
        int idx = (int) ((val - bucketStart) / interval);
        if (idx >= 0 && idx < bucketCounts.length) {
            bucketCounts[idx]++;
        }
    }

    // =====================================================================
    //  Date interval faceting (single-valued AND multi-valued)
    // =====================================================================

    private FacetResult computeDateIntervalFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        DateInterval dateInterval = req.getDateInterval();
        boolean isCollection = FieldDataType.isCollection(type);
        Map<String, Long> counts = new LinkedHashMap<>();

        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                if (isCollection) {
                    SortedNumericDocValues sndv = ctx.reader().getSortedNumericDocValues(dvFieldName);
                    if (sndv == null) continue;
                    for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                        if (liveDocs != null && !liveDocs.get(docId)) continue;
                        if (sndv.advanceExact(docId)) {
                            for (int i = 0; i < sndv.docValueCount(); i++) {
                                counts.merge(truncateToDateBucket(sndv.nextValue(), dateInterval),
                                        1L, Long::sum);
                            }
                        }
                    }
                } else {
                    NumericDocValues ndv = ctx.reader().getNumericDocValues(dvFieldName);
                    if (ndv == null) continue;
                    for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                        if (liveDocs != null && !liveDocs.get(docId)) continue;
                        if (ndv.advanceExact(docId)) {
                            counts.merge(truncateToDateBucket(ndv.longValue(), dateInterval),
                                    1L, Long::sum);
                        }
                    }
                }
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                if (isCollection) {
                    SortedNumericDocValues sndv = md.context().reader().getSortedNumericDocValues(dvFieldName);
                    if (sndv == null) continue;
                    DocIdSetIterator iter = md.bits().iterator();
                    int docId;
                    while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        if (sndv.advanceExact(docId)) {
                            for (int i = 0; i < sndv.docValueCount(); i++) {
                                counts.merge(truncateToDateBucket(sndv.nextValue(), dateInterval),
                                        1L, Long::sum);
                            }
                        }
                    }
                } else {
                    NumericDocValues ndv = md.context().reader().getNumericDocValues(dvFieldName);
                    if (ndv == null) continue;
                    DocIdSetIterator iter = md.bits().iterator();
                    int docId;
                    while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        if (ndv.advanceExact(docId)) {
                            counts.merge(truncateToDateBucket(ndv.longValue(), dateInterval),
                                    1L, Long::sum);
                        }
                    }
                }
            }
        }

        // Date buckets: sorted by key (chronological)
        int maxBuckets = req.getCount() > 0 ? req.getCount() : DEFAULT_FACET_COUNT;
        FacetResult.Builder result = FacetResult.newBuilder();
        counts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .limit(maxBuckets)
                .forEach(e -> result.addBuckets(FacetBucket.newBuilder()
                        .setValue(e.getKey())
                        .setCount(e.getValue())
                        .build()));
        return result.build();
    }

    // =====================================================================
    //  Result builders
    // =====================================================================

    /**
     * Builds a FacetResult with sorting and top-N trimming.
     * @param isIntegerNumeric true if keys should be compared numerically (for VALUE_ASC/DESC)
     */
    private FacetResult buildTermsFacetResult(Map<String, Long> counts, FacetRequest req,
                                               boolean isIntegerNumeric) {
        int maxBuckets = req.getCount() > 0 ? req.getCount() : DEFAULT_FACET_COUNT;
        FacetSortOrder sortOrder = req.getSort();

        // Apply explicit values filter
        if (req.getValuesCount() > 0) {
            Set<String> allowed = new HashSet<>(req.getValuesList());
            counts.entrySet().removeIf(e -> !allowed.contains(e.getKey()));
        }

        Comparator<Map.Entry<String, Long>> comparator = switch (sortOrder) {
            case COUNT_DESC -> Map.Entry.<String, Long>comparingByValue()
                    .reversed()
                    .thenComparing(e -> e.getKey(), numericAwareComparator(isIntegerNumeric));
            case COUNT_ASC -> Map.Entry.<String, Long>comparingByValue()
                    .thenComparing(e -> e.getKey(), numericAwareComparator(isIntegerNumeric));
            case VALUE_ASC -> Comparator.comparing(e -> e.getKey(), numericAwareComparator(isIntegerNumeric));
            case VALUE_DESC -> Comparator.comparing(
                    (Map.Entry<String, Long> e) -> e.getKey(), numericAwareComparator(isIntegerNumeric)).reversed();
            default -> Map.Entry.<String, Long>comparingByValue()
                    .reversed()
                    .thenComparing(e -> e.getKey(), numericAwareComparator(isIntegerNumeric));
        };

        FacetResult.Builder result = FacetResult.newBuilder();
        counts.entrySet().stream()
                .sorted(comparator)
                .limit(maxBuckets)
                .forEach(e -> result.addBuckets(FacetBucket.newBuilder()
                        .setValue(e.getKey())
                        .setCount(e.getValue())
                        .build()));
        return result.build();
    }

    /** Returns a comparator that sorts numerically when possible, lexicographically otherwise. */
    private static Comparator<String> numericAwareComparator(boolean isNumeric) {
        if (!isNumeric) return Comparator.naturalOrder();
        return (a, b) -> {
            try {
                return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
            } catch (NumberFormatException e) {
                return a.compareTo(b);
            }
        };
    }

    // =====================================================================
    //  Field name resolution
    // =====================================================================

    /**
     * Resolves the Lucene DocValues field name for faceting.
     * Priority: sortable ($o) → filterable ($f for numerics) → facetable-only ($o).
     */
    static String resolveDvFieldName(SchemaField field) {
        if (field.isSortable()) {
            return LuceneFieldType.SORTABLE.toLuceneFieldName(field.getName());
        }
        if (field.isFilterable() && isNumericOrDate(field.getType())
                && !FieldDataType.isCollection(field.getType())) {
            // Non-collection filterable numerics have DV at $f
            return LuceneFieldType.FILTERABLE.toLuceneFieldName(field.getName());
        }
        // Facetable-only or collection: mapper adds DV at $o
        return LuceneFieldType.SORTABLE.toLuceneFieldName(field.getName());
    }

    // =====================================================================
    //  Type helpers
    // =====================================================================

    private static boolean isStringOrBoolean(FieldDataType type) {
        return type == FieldDataType.STRING || type == FieldDataType.STRING_COLLECTION
                || type == FieldDataType.BOOLEAN || type == FieldDataType.BOOLEAN_COLLECTION;
    }

    private static boolean isNumericOrDate(FieldDataType type) {
        return type == FieldDataType.INT32 || type == FieldDataType.INT64
                || type == FieldDataType.DOUBLE || type == FieldDataType.DATE_TIME_OFFSET
                || type == FieldDataType.INT32_COLLECTION || type == FieldDataType.INT64_COLLECTION
                || type == FieldDataType.DOUBLE_COLLECTION
                || type == FieldDataType.DATE_TIME_OFFSET_COLLECTION;
    }

    private static boolean isDateTimeOffset(FieldDataType type) {
        return type == FieldDataType.DATE_TIME_OFFSET
                || type == FieldDataType.DATE_TIME_OFFSET_COLLECTION;
    }

    private static double rawToDouble(long rawValue, boolean isDouble) {
        return isDouble ? Double.longBitsToDouble(rawValue) : (double) rawValue;
    }

    private static String formatNumericValue(long rawValue, boolean isDouble) {
        if (isDouble) {
            double d = Double.longBitsToDouble(rawValue);
            return (d == Math.floor(d) && !Double.isInfinite(d))
                    ? String.valueOf((long) d) : String.valueOf(d);
        }
        return String.valueOf(rawValue);
    }

    private static String formatNumber(double val, boolean isDouble) {
        if (!isDouble && val == Math.floor(val) && !Double.isInfinite(val)) {
            return String.valueOf((long) val);
        }
        return String.valueOf(val);
    }

    private static String truncateToDateBucket(long epochMs, DateInterval interval) {
        ZonedDateTime dt = Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC);
        return switch (interval) {
            case MINUTE -> dt.truncatedTo(ChronoUnit.MINUTES).toString();
            case HOUR -> dt.truncatedTo(ChronoUnit.HOURS).toString();
            case DAY -> dt.toLocalDate().toString();
            case MONTH -> dt.getYear() + "-" + String.format("%02d", dt.getMonthValue());
            case YEAR -> String.valueOf(dt.getYear());
            default -> dt.toLocalDate().toString();
        };
    }
}
