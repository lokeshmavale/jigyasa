package com.jigyasa.dp.search.models;

import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.IndexWriterManagerISCH;
import com.jigyasa.dp.search.handlers.TranslogAppenderManager;

/**
 * Placeholder to store all the instances required for all the handlers
 */

public record HandlerHelpers(IndexSchemaManager indexSchemaManager, IndexSearcherManagerISCH indexSearcherManager,
                             IndexWriterManagerISCH indexWriterManager, TranslogAppenderManager translogAppenderManager) {}
