package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.configs.EnvironmentVariables;
import com.jigyasa.dp.search.handlers.translog.FileAppender;
import com.jigyasa.dp.search.handlers.translog.TranslogAppender;
import com.jigyasa.dp.search.models.IndexSchema;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TranslogAppenderManager implements IndexSchemaChangeHandler {

    @Getter
    private volatile TranslogAppender appender;
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
        String durabilityStr = EnvironmentVariables.TRANSLOG_DURABILITY.defaultIfEmpty();
        FileAppender.Durability durability = "async".equalsIgnoreCase(durabilityStr)
                ? FileAppender.Durability.ASYNC
                : FileAppender.Durability.REQUEST;
        int flushIntervalMs = Integer.parseInt(EnvironmentVariables.TRANSLOG_FLUSH_INTERVAL_MS.defaultIfEmpty());
        this.appender = new FileAppender(translogInitParam, durability, flushIntervalMs);
    }
}
