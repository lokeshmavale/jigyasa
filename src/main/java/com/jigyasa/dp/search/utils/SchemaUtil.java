package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.IndexSchema;
import com.google.gson.Gson;

public class SchemaUtil {
    private static final Gson GSON = new Gson();

    public static IndexSchema parseSchema(String schema) {
        return GSON.fromJson(schema, IndexSchema.class);
    }
}
