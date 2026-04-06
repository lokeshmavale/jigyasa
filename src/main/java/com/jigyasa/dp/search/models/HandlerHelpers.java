package com.jigyasa.dp.search.models;

import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import com.jigyasa.dp.search.handlers.IndexWriterManager;
import com.jigyasa.dp.search.handlers.TranslogAppenderManager;

/**
 * Placeholder to store all the instances required for all the handlers
 */

public record HandlerHelpers(IndexSchemaManager indexSchemaManager, IndexSearcherManager indexSearcherManager,
                             IndexWriterManager indexWriterManager, TranslogAppenderManager translogAppenderManager) {}
