package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.CompoundFilter;
import com.jigyasa.dp.search.protocol.ExistsFilter;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.GeoBoundingBoxFilter;
import com.jigyasa.dp.search.protocol.GeoDistanceFilter;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.TermFilter;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.List;

/**
 * Shared filter query construction used by QueryRequestHandler and DeleteByQueryRequestHandler.
 */
public final class FilterQueryBuilder {

    private FilterQueryBuilder() {}

    /**
     * Builds a BooleanQuery from filter clauses. All clauses are ANDed (FILTER occur).
     * Returns null if filters list is empty.
     */
    public static Query buildFilterQuery(List<FilterClause> filters, InitializedIndexSchema schema) {
        if (filters.isEmpty()) return null;

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (FilterClause fc : filters) {
            builder.add(buildSingleFilter(fc, schema), BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    /**
     * Builds a Lucene Query for a single FilterClause, dispatching by filter type.
     */
    static Query buildSingleFilter(FilterClause fc, InitializedIndexSchema schema) {
        if (fc.hasCompoundFilter()) {
            return buildCompoundQuery(fc.getCompoundFilter(), schema);
        }

        SchemaField sf = schema.getFieldLookupMap().get(fc.getField());
        if (sf == null) {
            throw new IllegalArgumentException("Unknown filter field: " + fc.getField());
        }
        if (!sf.isFilterable()) {
            throw new IllegalArgumentException("Field is not filterable: " + fc.getField());
        }

        String filterFieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(fc.getField());

        if (fc.hasTermFilter()) {
            return buildTermFilterQuery(filterFieldName, fc.getTermFilter(), sf.getType());
        } else if (fc.hasRangeFilter()) {
            return buildRangeFilterQuery(filterFieldName, fc.getRangeFilter(), sf.getType());
        } else if (fc.hasGeoDistanceFilter()) {
            return buildGeoDistanceQuery(filterFieldName, fc.getGeoDistanceFilter(), sf.getType());
        } else if (fc.hasGeoBoundingBoxFilter()) {
            return buildGeoBoundingBoxQuery(filterFieldName, fc.getGeoBoundingBoxFilter(), sf.getType());
        } else if (fc.hasExistsFilter()) {
            return buildExistsQuery(filterFieldName, fc.getExistsFilter(), sf);
        } else {
            throw new IllegalArgumentException("FilterClause for field '" + fc.getField() + "' has no filter predicate set");
        }
    }

    private static Query buildCompoundQuery(CompoundFilter cf, InitializedIndexSchema schema) {
        if (cf.getMustList().isEmpty() && cf.getShouldList().isEmpty() && cf.getMustNotList().isEmpty()) {
            throw new IllegalArgumentException("CompoundFilter must have at least one clause (must, should, or must_not)");
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (FilterClause clause : cf.getMustList()) {
            builder.add(buildSingleFilter(clause, schema), BooleanClause.Occur.FILTER);
        }
        for (FilterClause clause : cf.getShouldList()) {
            builder.add(buildSingleFilter(clause, schema), BooleanClause.Occur.SHOULD);
        }
        if (!cf.getShouldList().isEmpty()) {
            builder.setMinimumNumberShouldMatch(1);
        }
        if (!cf.getMustNotList().isEmpty()) {
            // must_not alone needs a MatchAll anchor
            if (cf.getMustList().isEmpty() && cf.getShouldList().isEmpty()) {
                builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            }
            for (FilterClause clause : cf.getMustNotList()) {
                builder.add(buildSingleFilter(clause, schema), BooleanClause.Occur.MUST_NOT);
            }
        }

        return builder.build();
    }

    private static Query buildExistsQuery(String filterFieldName, ExistsFilter ef, SchemaField sf) {
        Query existsQuery;
        if (sf.isSortable()) {
            String sortFieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(sf.getName());
            existsQuery = new FieldExistsQuery(sortFieldName);
        } else {
            existsQuery = switch (sf.getType()) {
                case INT32, INT32_COLLECTION ->
                        IntPoint.newRangeQuery(filterFieldName, Integer.MIN_VALUE, Integer.MAX_VALUE);
                case INT64, DATE_TIME_OFFSET, INT64_COLLECTION, DATE_TIME_OFFSET_COLLECTION ->
                        LongPoint.newRangeQuery(filterFieldName, Long.MIN_VALUE, Long.MAX_VALUE);
                case DOUBLE, DOUBLE_COLLECTION ->
                        DoublePoint.newRangeQuery(filterFieldName, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
                default -> throw new IllegalArgumentException(
                        "Exists filter requires field to be sortable or numeric filterable: " + sf.getName());
            };
        }

        if (ef.getMustExist()) {
            return existsQuery;
        } else {
            return new BooleanQuery.Builder()
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                    .add(existsQuery, BooleanClause.Occur.MUST_NOT)
                    .build();
        }
    }

    private static Query buildTermFilterQuery(String fieldName, TermFilter tf, FieldDataType type) {
        return switch (type) {
            case STRING, BOOLEAN, STRING_COLLECTION, BOOLEAN_COLLECTION ->
                    new TermQuery(new Term(fieldName, tf.getValue()));
            case INT32, INT32_COLLECTION ->
                    IntPoint.newExactQuery(fieldName, Integer.parseInt(tf.getValue()));
            case INT64, DATE_TIME_OFFSET, INT64_COLLECTION, DATE_TIME_OFFSET_COLLECTION ->
                    LongPoint.newExactQuery(fieldName, Long.parseLong(tf.getValue()));
            case DOUBLE, DOUBLE_COLLECTION ->
                    DoublePoint.newExactQuery(fieldName, Double.parseDouble(tf.getValue()));
            default -> throw new IllegalArgumentException("Term filter not supported for type: " + type);
        };
    }

    private static Query buildRangeFilterQuery(String fieldName, RangeFilter rf, FieldDataType type) {
        return switch (type) {
            case INT32, INT32_COLLECTION -> {
                try {
                    int min = rf.getMin().isEmpty() ? Integer.MIN_VALUE : Integer.parseInt(rf.getMin()) + (rf.getMinExclusive() ? 1 : 0);
                    int max = rf.getMax().isEmpty() ? Integer.MAX_VALUE : Integer.parseInt(rf.getMax()) - (rf.getMaxExclusive() ? 1 : 0);
                    yield IntPoint.newRangeQuery(fieldName, min, max);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value in range filter for field '" + fieldName + "': " + e.getMessage());
                }
            }
            case INT64, DATE_TIME_OFFSET, INT64_COLLECTION, DATE_TIME_OFFSET_COLLECTION -> {
                try {
                    long min = rf.getMin().isEmpty() ? Long.MIN_VALUE : Long.parseLong(rf.getMin()) + (rf.getMinExclusive() ? 1 : 0);
                    long max = rf.getMax().isEmpty() ? Long.MAX_VALUE : Long.parseLong(rf.getMax()) - (rf.getMaxExclusive() ? 1 : 0);
                    yield LongPoint.newRangeQuery(fieldName, min, max);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value in range filter for field '" + fieldName + "': " + e.getMessage());
                }
            }
            case DOUBLE, DOUBLE_COLLECTION -> {
                try {
                    double min = rf.getMin().isEmpty() ? Double.NEGATIVE_INFINITY : Double.parseDouble(rf.getMin());
                    double max = rf.getMax().isEmpty() ? Double.POSITIVE_INFINITY : Double.parseDouble(rf.getMax());
                    if (rf.getMinExclusive()) min = Math.nextUp(min);
                    if (rf.getMaxExclusive()) max = Math.nextDown(max);
                    yield DoublePoint.newRangeQuery(fieldName, min, max);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value in range filter for field '" + fieldName + "': " + e.getMessage());
                }
            }
            default -> throw new IllegalArgumentException("Range filter not supported for type: " + type);
        };
    }

    private static Query buildGeoDistanceQuery(String fieldName, GeoDistanceFilter gdf, FieldDataType type) {
        if (type != FieldDataType.GEO_POINT && type != FieldDataType.GEO_POINT_COLLECTION) {
            throw new IllegalArgumentException("Geo distance filter only supported for GEO_POINT, got: " + type);
        }
        validateLatLon(gdf.getLat(), gdf.getLon());
        if (gdf.getDistanceMeters() <= 0) {
            throw new IllegalArgumentException("Geo distance must be greater than zero, got: " + gdf.getDistanceMeters());
        }
        return LatLonPoint.newDistanceQuery(fieldName, gdf.getLat(), gdf.getLon(), gdf.getDistanceMeters());
    }

    private static Query buildGeoBoundingBoxQuery(String fieldName, GeoBoundingBoxFilter bbf, FieldDataType type) {
        if (type != FieldDataType.GEO_POINT && type != FieldDataType.GEO_POINT_COLLECTION) {
            throw new IllegalArgumentException("Geo bounding box filter only supported for GEO_POINT, got: " + type);
        }
        validateLatLon(bbf.getBottomLat(), bbf.getLeftLon());
        validateLatLon(bbf.getTopLat(), bbf.getRightLon());
        if (bbf.getBottomLat() > bbf.getTopLat()) {
            throw new IllegalArgumentException("Bottom latitude must be <= top latitude");
        }
        return LatLonPoint.newBoxQuery(fieldName, bbf.getBottomLat(), bbf.getTopLat(), bbf.getLeftLon(), bbf.getRightLon());
    }

    private static void validateLatLon(double lat, double lon) {
        if (lat < -90.0 || lat > 90.0) {
            throw new IllegalArgumentException("Latitude must be between -90 and 90, got: " + lat);
        }
        if (lon < -180.0 || lon > 180.0) {
            throw new IllegalArgumentException("Longitude must be between -180 and 180, got: " + lon);
        }
    }
}
