package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.protocol.UpdateSchemaRequest;
import com.jigyasa.dp.search.protocol.UpdateSchemaResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.SchemaUtil;
import io.grpc.stub.StreamObserver;

public class UpdateSchemaRequestHandler extends RequestHandlerBase<UpdateSchemaRequest, UpdateSchemaResponse> {
    private final CollectionRegistry registry;

    public UpdateSchemaRequestHandler(CollectionRegistry registry) {
        super("UpdateSchema");
        this.registry = registry;
    }

    @Override
    public void internalHandle(UpdateSchemaRequest req, StreamObserver<UpdateSchemaResponse> observer) {
        try {
            IndexSchema indexSchema = SchemaUtil.parseSchema(req.getIndexSchema());
            IndexSchemaManager manager = registry.resolveSchemaManager(req.getCollection());
            manager.updateIndexSchema(indexSchema);
            observer.onNext(UpdateSchemaResponse.newBuilder().build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }
}
