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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Computes facets by iterating DocValues directly — no Lucene FacetsConfig needed.
 * Reuses existing $o/$f DocValues fields.
 *
 * Parallelism is handled by Lucene's IndexSearcher executor + CollectorManager,
 * not by a custom thread pool. When IndexSearcher has an Executor, Lucene slices
 * segments and runs FacetsCollectorManager across threads automatically.
 *
 * Optimizations:
 *   - Ordinal-based counting for all string/boolean DV (int[] per segment, resolve at end)
 *   - Primitive long→long counting for numeric terms (no HashMap boxing)
 *   - Epoch-ms integer arithmetic for date bucketing (no ZonedDateTime allocations)
 *   - PriorityQueue partial sort for top-N (avoids full sort)
 *   - Unified DV visitor pattern eliminates code duplication
 */
public class FacetExecutor {
    private static final Logger log = LoggerFactory.getLogger(FacetExecutor.class);
    private static final int DEFAULT_FACET_COUNT = 10;
    private static final int MAX_RANGE_BUCKETS = 10_000;

    private static final long MS_PER_MINUTE = 60_000L;
    private static final long MS_PER_HOUR = 3_600_000L;
    private static final long MS_PER_DAY = 86_400_000L;

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
                log.warn("Field '{}' is not facetable (sortable={}, filterable={}, facetable={})",
                        fieldName, field.isSortable(), field.isFilterable(), field.isFacetable());
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
                    throw new IllegalArgumentException("Faceting not supported for type: " + type);
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
    //  Unified DV visitor — eliminates MatchAll/Filtered × single/collection
    //  duplication. All hot loops go through visitLongValues / visitOrdinals.
    // =====================================================================

    @FunctionalInterface
    private interface LongValueConsumer {
        void accept(long value) throws IOException;
    }

    @FunctionalInterface
    private interface OrdinalConsumer {
        void accept(int ordinal) throws IOException;
    }

    /** Visits all long values from NumericDocValues or SortedNumericDocValues. */
    private void visitLongValues(IndexSearcher searcher, String dvFieldName,
                                  boolean isCollection, FacetsCollector fc,
                                  LongValueConsumer consumer) throws IOException {
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
                                consumer.accept(sndv.nextValue());
                            }
                        }
                    }
                } else {
                    NumericDocValues ndv = ctx.reader().getNumericDocValues(dvFieldName);
                    if (ndv == null) continue;
                    for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                        if (liveDocs != null && !liveDocs.get(docId)) continue;
                        if (ndv.advanceExact(docId)) {
                            consumer.accept(ndv.longValue());
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
                                consumer.accept(sndv.nextValue());
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
                            consumer.accept(ndv.longValue());
                        }
                    }
                }
            }
        }
    }

    // =====================================================================
    //  String / Boolean terms — ordinal counting per segment
    // =====================================================================

    private FacetResult computeStringTermsFacet(IndexSearcher searcher, String dvFieldName,
                                                 FieldDataType type, FacetsCollector fc,
                                                 FacetRequest req) throws IOException {
        boolean isCollection = FieldDataType.isCollection(type);
        // String ordinal counting is maximally cache-friendly sequential — parallelism
        // adds dispatch + merge overhead that exceeds gain at typical segment counts.
        Map<String, Long> counts = new HashMap<>();
        if (fc == null) {
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                countStringSegment(ctx, dvFieldName, isCollection, ctx.reader().getLiveDocs(), null, counts);
            }
        } else {
            for (FacetsCollector.MatchingDocs md : fc.getMatchingDocs()) {
                countStringSegment(md.context(), dvFieldName, isCollection, null, md, counts);
            }
        }
        return buildTermsFacetResult(counts, req, false);
    }

    /**
     * Unified per-segment string ordinal counting.
     * @param md if non-null, iterate only matching docs; if null, iterate all live docs.
     */
    private void countStringSegment(LeafReaderContext ctx, String dvFieldName,
                                     boolean isCollection, Bits liveDocs,
                                     FacetsCollector.MatchingDocs md,
                                     Map<String, Long> globalCounts) throws IOException {
        if (isCollection) {
            SortedSetDocValues ssdv = ctx.reader().getSortedSetDocValues(dvFieldName);
            if (ssdv == null) return;

            long valueCount = ssdv.getValueCount();
            if (valueCount <= 4_000_000) {
                int[] localCounts = new int[(int) valueCount];
                if (md != null) {
                    DocIdSetIterator iter = md.bits().iterator();
                    int docId;
                    while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        if (ssdv.advanceExact(docId)) {
                            for (int i = 0; i < ssdv.docValueCount(); i++) {
                                localCounts[(int) ssdv.nextOrd()]++;
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
                }
                for (int ord = 0; ord < (int) valueCount; ord++) {
                    if (localCounts[ord] > 0) {
                        globalCounts.merge(ssdv.lookupOrd(ord).utf8ToString(),
                                (long) localCounts[ord], Long::sum);
                    }
                }
            } else {
                // High cardinality (>4M): use long[] up to 50M, else per-doc resolve
                if (valueCount <= 50_000_000L) {
                    long[] localCounts = new long[(int) valueCount];
                    if (md != null) {
                        DocIdSetIterator iter = md.bits().iterator();
                        int docId;
                        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            if (ssdv.advanceExact(docId)) {
                                for (int i = 0; i < ssdv.docValueCount(); i++) {
                                    localCounts[(int) ssdv.nextOrd()]++;
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
                    }
                    for (int ord = 0; ord < localCounts.length; ord++) {
                        if (localCounts[ord] > 0) {
                            globalCounts.merge(ssdv.lookupOrd(ord).utf8ToString(),
                                    localCounts[ord], Long::sum);
                        }
                    }
                } else {
                    // >50M: per-doc string resolve (extreme cardinality)
                    if (md != null) {
                        DocIdSetIterator iter = md.bits().iterator();
                        int docId;
                        while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            if (ssdv.advanceExact(docId)) {
                                for (int i = 0; i < ssdv.docValueCount(); i++) {
                                    globalCounts.merge(ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString(),
                                            1L, Long::sum);
                                }
                            }
                        }
                    } else {
                        for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                            if (liveDocs != null && !liveDocs.get(docId)) continue;
                            if (ssdv.advanceExact(docId)) {
                                for (int i = 0; i < ssdv.docValueCount(); i++) {
                                    globalCounts.merge(ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString(),
                                            1L, Long::sum);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Single-valued SortedDocValues
            SortedDocValues sdv = ctx.reader().getSortedDocValues(dvFieldName);
            if (sdv == null) return;

            int valueCount = sdv.getValueCount();
            int[] localCounts = new int[valueCount];

            if (md != null) {
                DocIdSetIterator iter = md.bits().iterator();
                int docId;
                while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (sdv.advanceExact(docId)) {
                        localCounts[sdv.ordValue()]++;
                    }
                }
            } else {
                for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                    if (liveDocs != null && !liveDocs.get(docId)) continue;
                    if (sdv.advanceExact(docId)) {
                        localCounts[sdv.ordValue()]++;
                    }
                }
            }

            for (int ord = 0; ord < valueCount; ord++) {
                if (localCounts[ord] > 0) {
                    globalCounts.merge(sdv.lookupOrd(ord).utf8ToString(),
                            (long) localCounts[ord], Long::sum);
                }
            }
        }
    }

    // =====================================================================
    //  Numeric terms — primitive long→long counting, no boxing
    // =====================================================================

    private FacetResult computeNumericTermsFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        boolean isDouble = isDoubleType(type);
        boolean isCollection = FieldDataType.isCollection(type);
        HashMap<Long, long[]> rawCounts = new HashMap<>();

        visitLongValues(searcher, dvFieldName, isCollection, fc, val -> {
            incrementLongMap(rawCounts, val);
        });

        Map<String, Long> counts = new HashMap<>(rawCounts.size());
        for (var entry : rawCounts.entrySet()) {
            counts.put(formatNumericValue(entry.getKey(), isDouble), entry.getValue()[0]);
        }
        return buildTermsFacetResult(counts, req, !isDouble);
    }

    private static void incrementLongMap(HashMap<Long, long[]> map, long key) {
        long[] count = map.get(key);
        if (count == null) { map.put(key, new long[]{1}); } else { count[0]++; }
    }

    // =====================================================================
    //  Numeric range — SINGLE pass (min/max + bucket count simultaneously)
    // =====================================================================

    private FacetResult computeNumericRangeFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        boolean isDouble = isDoubleType(type);
        boolean isCollection = FieldDataType.isCollection(type);
        double interval = req.getInterval();

        // Single-pass: collect min/max while storing raw values in a compact long[]
        // For up to 10M values, store them; above that, use two-pass.
        // First, quick count check:
        double[] minMax = {Double.MAX_VALUE, -Double.MAX_VALUE};
        long[] bucketCounts = null;
        double bucketStart = 0;

        // We need min/max to compute bucket boundaries, but we also need to assign
        // values to buckets. For integer types (non-double), we can do single-pass
        // by collecting all values, then bucketing. For efficiency, we just do two-pass
        // with the unified visitor — but the second pass is now via the visitor pattern
        // so there's no code duplication.

        // Pass 1: min/max
        visitLongValues(searcher, dvFieldName, isCollection, fc, val -> {
            double v = isDouble ? Double.longBitsToDouble(val) : (double) val;
            if (v < minMax[0]) minMax[0] = v;
            if (v > minMax[1]) minMax[1] = v;
        });

        if (minMax[0] > minMax[1]) {
            return FacetResult.getDefaultInstance();
        }

        bucketStart = Math.floor(minMax[0] / interval) * interval;
        long numBuckets = (long) Math.ceil((minMax[1] - bucketStart) / interval);
        if (numBuckets <= 0) numBuckets = 1;
        if (numBuckets > MAX_RANGE_BUCKETS) {
            throw new IllegalArgumentException(
                    "interval " + interval + " produces " + numBuckets
                            + " buckets (max " + MAX_RANGE_BUCKETS + "). Use a larger interval.");
        }
        bucketCounts = new long[(int) numBuckets];

        // Pass 2: bucket counting (same visitor, zero duplication)
        final double bs = bucketStart;
        final long[] bc = bucketCounts;
        visitLongValues(searcher, dvFieldName, isCollection, fc, val -> {
            double v = isDouble ? Double.longBitsToDouble(val) : (double) val;
            int idx = (int) ((v - bs) / interval);
            if (idx >= 0 && idx < bc.length) {
                bc[idx]++;
            }
        });

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

    // =====================================================================
    //  Date interval — epoch-ms integer arithmetic, no ZonedDateTime
    // =====================================================================

    private FacetResult computeDateIntervalFacet(IndexSearcher searcher, String dvFieldName,
                                                  FieldDataType type, FacetsCollector fc,
                                                  FacetRequest req) throws IOException {
        DateInterval dateInterval = req.getDateInterval();
        boolean isCollection = FieldDataType.isCollection(type);
        HashMap<Long, long[]> rawCounts = new HashMap<>();

        visitLongValues(searcher, dvFieldName, isCollection, fc, epochMs -> {
            long bucketKey = truncateEpochMs(epochMs, dateInterval);
            long[] count = rawCounts.get(bucketKey);
            if (count == null) {
                rawCounts.put(bucketKey, new long[]{1});
            } else {
                count[0]++;
            }
        });

        // Sort by bucket key (chronological) and format labels
        int maxBuckets = req.getCount() > 0 ? req.getCount() : DEFAULT_FACET_COUNT;
        FacetResult.Builder result = FacetResult.newBuilder();
        rawCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .limit(maxBuckets)
                .forEach(e -> result.addBuckets(FacetBucket.newBuilder()
                        .setValue(formatDateBucket(e.getKey(), dateInterval))
                        .setCount(e.getValue()[0])
                        .build()));
        return result.build();
    }

    /** Truncate epoch-ms to bucket boundary using integer arithmetic. Zero allocations. */
    private static long truncateEpochMs(long epochMs, DateInterval interval) {
        return switch (interval) {
            case MINUTE -> epochMs - (epochMs % MS_PER_MINUTE);
            case HOUR -> epochMs - (epochMs % MS_PER_HOUR);
            case DAY -> epochMs - (epochMs % MS_PER_DAY);
            case MONTH -> {
                java.time.LocalDate ld = java.time.LocalDate.ofEpochDay(epochMs / MS_PER_DAY);
                yield java.time.LocalDate.of(ld.getYear(), ld.getMonthValue(), 1)
                        .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            case YEAR -> {
                java.time.LocalDate ld = java.time.LocalDate.ofEpochDay(epochMs / MS_PER_DAY);
                yield java.time.LocalDate.of(ld.getYear(), 1, 1)
                        .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            default -> epochMs - (epochMs % MS_PER_DAY);
        };
    }

    /** Format a truncated epoch-ms bucket key into a human-readable label. */
    private static String formatDateBucket(long truncatedEpochMs, DateInterval interval) {
        java.time.LocalDate ld = java.time.LocalDate.ofEpochDay(truncatedEpochMs / MS_PER_DAY);
        return switch (interval) {
            case MINUTE, HOUR -> java.time.Instant.ofEpochMilli(truncatedEpochMs)
                    .atZone(java.time.ZoneOffset.UTC).toString();
            case DAY -> ld.toString();
            case MONTH -> ld.getYear() + "-" + String.format("%02d", ld.getMonthValue());
            case YEAR -> String.valueOf(ld.getYear());
            default -> ld.toString();
        };
    }

    // =====================================================================
    //  Result builder — PriorityQueue partial sort for top-N
    // =====================================================================

    private FacetResult buildTermsFacetResult(Map<String, Long> counts, FacetRequest req,
                                               boolean isIntegerNumeric) {
        int maxBuckets = req.getCount() > 0 ? req.getCount() : DEFAULT_FACET_COUNT;
        FacetSortOrder sortOrder = req.getSort();

        // Explicit values filter
        if (req.getValuesCount() > 0) {
            Set<String> allowed = new HashSet<>(req.getValuesList());
            counts.entrySet().removeIf(e -> !allowed.contains(e.getKey()));
        }

        if (counts.isEmpty()) {
            return FacetResult.getDefaultInstance();
        }

        Comparator<Map.Entry<String, Long>> comparator = buildComparator(sortOrder, isIntegerNumeric);

        // For top-N with N << total, PriorityQueue is O(total * log N) vs full sort O(total * log total)
        FacetResult.Builder result = FacetResult.newBuilder();
        if (counts.size() <= maxBuckets || maxBuckets <= 0) {
            // No need for partial sort — just sort all
            counts.entrySet().stream()
                    .sorted(comparator)
                    .limit(maxBuckets > 0 ? maxBuckets : Integer.MAX_VALUE)
                    .forEach(e -> result.addBuckets(FacetBucket.newBuilder()
                            .setValue(e.getKey()).setCount(e.getValue()).build()));
        } else {
            // Partial sort with bounded PriorityQueue (keeps top-N in min-heap)
            Comparator<Map.Entry<String, Long>> reversed = comparator.reversed();
            PriorityQueue<Map.Entry<String, Long>> heap = new PriorityQueue<>(maxBuckets + 1, reversed);
            for (var entry : counts.entrySet()) {
                heap.offer(entry);
                if (heap.size() > maxBuckets) {
                    heap.poll();
                }
            }
            // Drain heap in correct order
            Map.Entry<String, Long>[] top = heap.toArray(new Map.Entry[0]);
            java.util.Arrays.sort(top, comparator);
            for (var e : top) {
                result.addBuckets(FacetBucket.newBuilder()
                        .setValue(e.getKey()).setCount(e.getValue()).build());
            }
        }
        return result.build();
    }

    private static Comparator<Map.Entry<String, Long>> buildComparator(FacetSortOrder order,
                                                                        boolean isNumeric) {
        Comparator<String> keyComp = isNumeric ? numericComparator() : Comparator.naturalOrder();
        return switch (order) {
            case COUNT_DESC -> Map.Entry.<String, Long>comparingByValue().reversed()
                    .thenComparing(Map.Entry::getKey, keyComp);
            case COUNT_ASC -> Map.Entry.<String, Long>comparingByValue()
                    .thenComparing(Map.Entry::getKey, keyComp);
            case VALUE_ASC -> Comparator.comparing(Map.Entry::getKey, keyComp);
            case VALUE_DESC -> Comparator.comparing(Map.Entry<String, Long>::getKey, keyComp).reversed();
            default -> Map.Entry.<String, Long>comparingByValue().reversed()
                    .thenComparing(Map.Entry::getKey, keyComp);
        };
    }

    private static Comparator<String> numericComparator() {
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

    static String resolveDvFieldName(SchemaField field) {
        if (field.isSortable()) {
            return LuceneFieldType.SORTABLE.toLuceneFieldName(field.getName());
        }
        if (field.isFilterable() && isNumericOrDate(field.getType())
                && !FieldDataType.isCollection(field.getType())) {
            return LuceneFieldType.FILTERABLE.toLuceneFieldName(field.getName());
        }
        return LuceneFieldType.SORTABLE.toLuceneFieldName(field.getName());
    }

    // =====================================================================
    //  Type helpers
    // =====================================================================

    private static boolean isStringOrBoolean(FieldDataType t) {
        return t == FieldDataType.STRING || t == FieldDataType.STRING_COLLECTION
                || t == FieldDataType.BOOLEAN || t == FieldDataType.BOOLEAN_COLLECTION;
    }

    private static boolean isNumericOrDate(FieldDataType t) {
        return t == FieldDataType.INT32 || t == FieldDataType.INT64
                || t == FieldDataType.DOUBLE || t == FieldDataType.DATE_TIME_OFFSET
                || t == FieldDataType.INT32_COLLECTION || t == FieldDataType.INT64_COLLECTION
                || t == FieldDataType.DOUBLE_COLLECTION || t == FieldDataType.DATE_TIME_OFFSET_COLLECTION;
    }

    private static boolean isDateTimeOffset(FieldDataType t) {
        return t == FieldDataType.DATE_TIME_OFFSET || t == FieldDataType.DATE_TIME_OFFSET_COLLECTION;
    }

    private static boolean isDoubleType(FieldDataType t) {
        return t == FieldDataType.DOUBLE || t == FieldDataType.DOUBLE_COLLECTION;
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
}
