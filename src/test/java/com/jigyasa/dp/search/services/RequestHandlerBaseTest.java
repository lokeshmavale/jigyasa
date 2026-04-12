package com.jigyasa.dp.search.services;

import com.jigyasa.dp.search.metrics.MetricsRecorder;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.IndexResponse;
import com.jigyasa.dp.search.protocol.DeleteByQueryRequest;
import com.jigyasa.dp.search.protocol.DeleteByQueryResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RequestHandlerBaseTest {

    private MetricsRecorder originalMetrics;

    @BeforeEach
    void setUp() {
        originalMetrics = RequestHandlerBase.getMetricsRecorder();
    }

    @AfterEach
    void tearDown() {
        RequestHandlerBase.setMetricsRecorder(originalMetrics);
    }

    @Test
    @DisplayName("extractCollection reads proto 'collection' field via descriptor")
    void extractCollectionFromProto() {
        var handler = new TestHandler("Test");

        QueryRequest withCollection = QueryRequest.newBuilder()
                .setCollection("products").build();
        assertThat(handler.testExtractCollection(withCollection)).isEqualTo("products");

        QueryRequest empty = QueryRequest.newBuilder().build();
        assertThat(handler.testExtractCollection(empty)).isEqualTo("default");
    }

    @Test
    @DisplayName("extractCollection works for different proto request types")
    void extractCollectionDifferentTypes() {
        var indexHandler = new RequestHandlerBase<IndexRequest, IndexResponse>("Index") {
            @Override
            public void internalHandle(IndexRequest req, StreamObserver<IndexResponse> obs) {}
        };
        var deleteHandler = new RequestHandlerBase<DeleteByQueryRequest, DeleteByQueryResponse>("Delete") {
            @Override
            public void internalHandle(DeleteByQueryRequest req, StreamObserver<DeleteByQueryResponse> obs) {}
        };

        IndexRequest ir = IndexRequest.newBuilder().setCollection("logs").build();
        assertThat(indexHandler.extractCollection(ir)).isEqualTo("logs");

        DeleteByQueryRequest dr = DeleteByQueryRequest.newBuilder().setCollection("events").build();
        assertThat(deleteHandler.extractCollection(dr)).isEqualTo("events");
    }

    @Test
    @DisplayName("StatusTrackingObserver detects onError and records error status")
    void statusTrackingDetectsOnError() {
        SpyMetrics spy = new SpyMetrics();
        RequestHandlerBase.setMetricsRecorder(spy);

        var handler = new RequestHandlerBase<QueryRequest, QueryResponse>("Query") {
            @Override
            public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
                observer.onError(Status.INTERNAL
                        .withDescription("something broke").asRuntimeException());
            }
        };

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.handle(QueryRequest.newBuilder().build(), observer);

        assertThat(spy.statuses).containsExactly("error");
    }

    @Test
    @DisplayName("StatusTrackingObserver records 'invalid' for INVALID_ARGUMENT")
    void statusTrackingInvalidArgument() {
        SpyMetrics spy = new SpyMetrics();
        RequestHandlerBase.setMetricsRecorder(spy);

        var handler = new RequestHandlerBase<QueryRequest, QueryResponse>("Query") {
            @Override
            public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription("bad field").asRuntimeException());
            }
        };

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.handle(QueryRequest.newBuilder().build(), observer);

        assertThat(spy.statuses).containsExactly("invalid");
    }

    @Test
    @DisplayName("Successful request records 'ok' status")
    void successfulRequestRecordsOk() {
        SpyMetrics spy = new SpyMetrics();
        RequestHandlerBase.setMetricsRecorder(spy);

        var handler = new RequestHandlerBase<QueryRequest, QueryResponse>("Query") {
            @Override
            public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
                observer.onNext(QueryResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.handle(QueryRequest.newBuilder().build(), observer);

        assertThat(spy.statuses).containsExactly("ok");
    }

    @Test
    @DisplayName("Active requests are incremented then decremented")
    void activeRequestsTracked() {
        SpyMetrics spy = new SpyMetrics();
        RequestHandlerBase.setMetricsRecorder(spy);

        var handler = new RequestHandlerBase<QueryRequest, QueryResponse>("Query") {
            @Override
            public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
                observer.onNext(QueryResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.handle(QueryRequest.newBuilder().build(), observer);

        assertThat(spy.increments).containsExactly("Query");
        assertThat(spy.decrements).containsExactly("Query");
    }

    /** Test handler that exposes extractCollection for verification. */
    private static class TestHandler extends RequestHandlerBase<QueryRequest, QueryResponse> {
        TestHandler(String name) { super(name); }

        @Override
        public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
            observer.onNext(QueryResponse.getDefaultInstance());
            observer.onCompleted();
        }

        String testExtractCollection(QueryRequest req) {
            return extractCollection(req);
        }
    }

    /** Captures all metrics calls for assertion. */
    private static class SpyMetrics implements MetricsRecorder {
        final List<String> statuses = new ArrayList<>();
        final List<String> increments = new ArrayList<>();
        final List<String> decrements = new ArrayList<>();

        @Override public void recordRequest(String rpc, String col, String status, long d) { statuses.add(status); }
        @Override public void incrementActiveRequests(String rpc) { increments.add(rpc); }
        @Override public void decrementActiveRequests(String rpc) { decrements.add(rpc); }
        @Override public void recordFacet(String col, String type, long d) {}
        @Override public void recordIndexDuration(String col, long d) {}
        @Override public void recordIndexedDocs(String col, long count) {}
    }
}
