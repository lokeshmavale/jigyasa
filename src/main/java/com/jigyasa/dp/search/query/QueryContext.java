package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.QueryRequest;

/**
 * Immutable context carrying everything the query pipeline needs.
 * Avoids passing multiple parameters through every method.
 */
public record QueryContext(
        QueryRequest request,
        IndexSchema indexSchema,
        InitializedIndexSchema initializedSchema
) {}
