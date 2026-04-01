package com.jigyasa.dp.search.query;

import org.apache.lucene.search.Query;

import java.util.List;

/**
 * Applies an ordered chain of QueryModifiers to a base query.
 * Order matters: filters should be applied before scoring modifiers.
 */
public class QueryPipeline {

    private final List<QueryModifier> modifiers;

    public QueryPipeline(List<QueryModifier> modifiers) {
        this.modifiers = List.copyOf(modifiers);
    }

    public Query apply(Query baseQuery, QueryContext context) {
        Query result = baseQuery;
        for (QueryModifier modifier : modifiers) {
            if (modifier.applies(context)) {
                result = modifier.modify(result, context);
            }
        }
        return result;
    }
}
