package com.jigyasa.dp.search.services;

import com.jigyasa.dp.search.entrypoint.IndexManager;
import com.jigyasa.dp.search.protocol.*;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;

public class AnweshanDataPlaneImpl extends JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceImplBase {

    private final RequestHandlerBase<IndexRequest, IndexResponse> indexHandler;
    private final RequestHandlerBase<LookupRequest, LookupResponse> lookupHandler;
    private final RequestHandlerBase<QueryRequest, QueryResponse> queryHandler;
    private final RequestHandlerBase<DeleteByQueryRequest, DeleteByQueryResponse> deleteByQueryHandler;
    private final RequestHandlerBase<UpdateSchemaRequest, UpdateSchemaResponse> updateSchemaResponseHandler;
    private final IndexManager indexManager;

    @Inject
    public AnweshanDataPlaneImpl(
            RequestHandlerBase<IndexRequest, IndexResponse> indexHandler,
            RequestHandlerBase<LookupRequest, LookupResponse> lookupHandler,
            RequestHandlerBase<QueryRequest, QueryResponse> queryHandler,
            RequestHandlerBase<DeleteByQueryRequest, DeleteByQueryResponse> deleteByQueryHandler,
            RequestHandlerBase<UpdateSchemaRequest, UpdateSchemaResponse> updateSchemaResponseHandler,
            IndexManager indexManager) {
        this.indexHandler = indexHandler;
        this.lookupHandler = lookupHandler;
        this.queryHandler = queryHandler;
        this.deleteByQueryHandler = deleteByQueryHandler;
        this.updateSchemaResponseHandler = updateSchemaResponseHandler;
        this.indexManager = indexManager;
    }

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

    @Override
    public void deleteByQuery(DeleteByQueryRequest request, StreamObserver<DeleteByQueryResponse> responseObserver) {
        deleteByQueryHandler.handle(request, responseObserver);
    }

    @Override
    public void createCollection(CreateCollectionRequest request, StreamObserver<CreateCollectionResponse> responseObserver) {
        indexManager.createCollection(request, responseObserver);
    }

    @Override
    public void closeCollection(CloseCollectionRequest request, StreamObserver<CloseCollectionResponse> responseObserver) {
        indexManager.closeCollection(request, responseObserver);
    }

    @Override
    public void openCollection(OpenCollectionRequest request, StreamObserver<OpenCollectionResponse> responseObserver) {
        indexManager.openCollection(request, responseObserver);
    }

    @Override
    public void listCollections(ListCollectionsRequest request, StreamObserver<ListCollectionsResponse> responseObserver) {
        indexManager.listCollections(request, responseObserver);
    }

    @Override
    public void health(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
        indexManager.health(request, responseObserver);
    }

    @Override
    public void forceMerge(ForceMergeRequest request, StreamObserver<ForceMergeResponse> responseObserver) {
        indexManager.forceMerge(request, responseObserver);
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        indexManager.count(request, responseObserver);
    }
}
