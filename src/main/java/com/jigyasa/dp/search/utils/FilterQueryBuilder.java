package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.TermFilter;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
            SchemaField sf = schema.getFieldLookupMap().get(fc.getField());
            if (sf == null) {
                throw new IllegalArgumentException("Unknown filter field: " + fc.getField());
            }
            if (!sf.isFilterable()) {
                throw new IllegalArgumentException("Field is not filterable: " + fc.getField());
            }

            String filterFieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(fc.getField());

            if (fc.hasTermFilter()) {
                builder.add(buildTermFilterQuery(filterFieldName, fc.getTermFilter(), sf.getType()), BooleanClause.Occur.FILTER);
            } else if (fc.hasRangeFilter()) {
                builder.add(buildRangeFilterQuery(filterFieldName, fc.getRangeFilter(), sf.getType()), BooleanClause.Occur.FILTER);
            } else {
                throw new IllegalArgumentException("FilterClause for field '" + fc.getField() + "' has no filter predicate set");
            }
        }
        return builder.build();
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
                int min = rf.getMin().isEmpty() ? Integer.MIN_VALUE : Integer.parseInt(rf.getMin()) + (rf.getMinExclusive() ? 1 : 0);
                int max = rf.getMax().isEmpty() ? Integer.MAX_VALUE : Integer.parseInt(rf.getMax()) - (rf.getMaxExclusive() ? 1 : 0);
                yield IntPoint.newRangeQuery(fieldName, min, max);
            }
            case INT64, DATE_TIME_OFFSET, INT64_COLLECTION, DATE_TIME_OFFSET_COLLECTION -> {
                long min = rf.getMin().isEmpty() ? Long.MIN_VALUE : Long.parseLong(rf.getMin()) + (rf.getMinExclusive() ? 1 : 0);
                long max = rf.getMax().isEmpty() ? Long.MAX_VALUE : Long.parseLong(rf.getMax()) - (rf.getMaxExclusive() ? 1 : 0);
                yield LongPoint.newRangeQuery(fieldName, min, max);
            }
            case DOUBLE, DOUBLE_COLLECTION -> {
                double min = rf.getMin().isEmpty() ? Double.NEGATIVE_INFINITY : Double.parseDouble(rf.getMin());
                double max = rf.getMax().isEmpty() ? Double.POSITIVE_INFINITY : Double.parseDouble(rf.getMax());
                if (rf.getMinExclusive()) min = Math.nextUp(min);
                if (rf.getMaxExclusive()) max = Math.nextDown(max);
                yield DoublePoint.newRangeQuery(fieldName, min, max);
            }
            default -> throw new IllegalArgumentException("Range filter not supported for type: " + type);
        };
    }
}
