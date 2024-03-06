package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.handlers.translog.FileAppender;
import com.jigyasa.dp.search.handlers.translog.TranslogAppender;
import com.jigyasa.dp.search.models.IndexSchema;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TranslogAppenderManager implements IndexSchemaChangeHandler {

    @Getter
    private TranslogAppender appender;
    final private String translogInitParam;

    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        //No schema change handling required for translog
    }

    /**
     * This method should be called before Writer Init.
     */
    @Override
    public void initService() {
        this.appender = new FileAppender(translogInitParam);
    }
}
