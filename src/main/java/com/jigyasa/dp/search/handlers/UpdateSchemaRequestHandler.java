package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.protocol.UpdateSchemaRequest;
import com.jigyasa.dp.search.protocol.UpdateSchemaResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.SchemaUtil;
import io.grpc.stub.StreamObserver;

public class UpdateSchemaRequestHandler extends RequestHandlerBase<UpdateSchemaRequest, UpdateSchemaResponse> {
    private final IndexSchemaManager manager;

    public UpdateSchemaRequestHandler(IndexSchemaManager manager) {
        super("UpdateSchema");
        this.manager = manager;
    }

    @Override
    public void internalHandle(UpdateSchemaRequest req, StreamObserver<UpdateSchemaResponse> observer) {
        IndexSchema indexSchema = SchemaUtil.parseSchema(req.getIndexSchema());
        manager.updateIndexSchema(indexSchema);
    }
}
