package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchema;

public interface IndexSchemaChangeHandler {

    /**
     * Callback executed while updating index schema
     * This method should break and throw exception in case of any failure. Should not allow partial success.
     */
    void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema);

    /**
     * Called once, will be called after handle()
     */
    default void initService() {
    }
}
