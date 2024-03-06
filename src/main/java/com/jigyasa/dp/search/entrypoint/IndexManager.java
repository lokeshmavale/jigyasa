package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.models.IndexSchemaManager;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class IndexManager {

    private final IndexSchemaManager manager;
    private final IndexSchemaReader indexSchemaReader;

    public void initialize() {
        try {
            this.manager.updateIndexSchema(indexSchemaReader.readSchema());
            this.manager.initServices();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
