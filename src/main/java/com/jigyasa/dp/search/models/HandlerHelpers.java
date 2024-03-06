package com.jigyasa.dp.search.models;

import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.IndexWriterManagerISCH;
import com.jigyasa.dp.search.handlers.TranslogAppenderManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Placeholder to store all the instances required for all the handlers
 */

@Getter
@RequiredArgsConstructor
public class HandlerHelpers {
    private final IndexSchemaManager indexSchemaManager;
    private final IndexSearcherManagerISCH indexSearcherManager;
    private final IndexWriterManagerISCH indexWriterManager;
    private final TranslogAppenderManager translogAppenderManager;
}
