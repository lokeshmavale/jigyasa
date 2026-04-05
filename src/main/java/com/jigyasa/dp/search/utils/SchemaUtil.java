package com.jigyasa.dp.search.utils;

import com.google.gson.Gson;
import com.jigyasa.dp.search.models.IndexSchema;

public final class SchemaUtil {
    private static final Gson GSON = new Gson();

    private SchemaUtil() {}

    public static IndexSchema parseSchema(String schema) {
        return GSON.fromJson(schema, IndexSchema.class);
    }

    public static String toJson(IndexSchema schema) {
        return GSON.toJson(schema);
    }
}
