package com.jigyasa.dp.search.query;

import org.apache.lucene.search.Query;

/**
 * Transforms a Lucene Query before execution.
 * Each modifier handles one concern (SRP). New modifiers can be added
 * without modifying existing code (OCP).
 */
public interface QueryModifier {

    /**
     * @return true if this modifier should be applied for the given request
     */
    boolean applies(QueryContext context);

    /**
     * Wraps or transforms the query.
     */
    Query modify(Query query, QueryContext context);
}
