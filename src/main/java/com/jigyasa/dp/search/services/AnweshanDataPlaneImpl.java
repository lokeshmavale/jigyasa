package com.jigyasa.dp.search.services;

import com.jigyasa.dp.search.protocol.*;
import com.google.inject.Inject;
import com.jigyasa.dp.search.protocol.*;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC, onConstructor = @__({@Inject}))
public class AnweshanDataPlaneImpl extends JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceImplBase {

    private final RequestHandlerBase<IndexRequest, IndexResponse> indexHandler;
    private final RequestHandlerBase<LookupRequest, LookupResponse> lookupHandler;
    private final RequestHandlerBase<QueryRequest, QueryResponse> queryHandler;
    private final RequestHandlerBase<UpdateSchemaRequest, UpdateSchemaResponse> updateSchemaResponseHandler;

    @Override
    public void index(IndexRequest request, StreamObserver<IndexResponse> responseObserver) {
        indexHandler.handle(request, responseObserver);
    }

    @Override
    public void lookup(LookupRequest request, StreamObserver<LookupResponse> responseObserver) {
        lookupHandler.handle(request, responseObserver);
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        queryHandler.handle(request, responseObserver);
    }

    @Override
    public void updateSchema(UpdateSchemaRequest request, StreamObserver<UpdateSchemaResponse> responseObserver) {
        updateSchemaResponseHandler.handle(request, responseObserver);
    }
}
