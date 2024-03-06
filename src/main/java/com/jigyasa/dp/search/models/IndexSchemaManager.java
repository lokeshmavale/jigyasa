package com.jigyasa.dp.search.models;

import com.jigyasa.dp.search.handlers.IndexSchemaChangeHandler;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages Index schema. You can add schema observers
 */
public class IndexSchemaManager {

    @Getter
    private volatile IndexSchema indexSchema;

    private final List<IndexSchemaChangeHandler> beforeHandlers = new ArrayList<>();

    public void updateIndexSchema(final IndexSchema indexSchema) {
        for (IndexSchemaChangeHandler indexSchemaChangeObservers : beforeHandlers) {
            indexSchemaChangeObservers.handle(indexSchema, this.indexSchema);
        }

        //With this change, all the API handlers will be able to see updated index schema
        this.indexSchema = indexSchema;
    }

    public void initServices() {
        for (IndexSchemaChangeHandler indexSchemaChangeObservers : beforeHandlers) {
            indexSchemaChangeObservers.initService();
        }
    }

    public void addHandler(IndexSchemaChangeHandler handler) {
        this.beforeHandlers.add(handler);
    }
}
