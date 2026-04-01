package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.IndexSchema;
import com.google.gson.Gson;

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
