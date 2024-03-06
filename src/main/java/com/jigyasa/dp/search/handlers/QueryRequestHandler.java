package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import io.grpc.stub.StreamObserver;

public class QueryRequestHandler extends RequestHandlerBase<QueryRequest, QueryResponse> {

    public QueryRequestHandler() {
        super("Query");
    }

    @Override
    public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {

    }
}
