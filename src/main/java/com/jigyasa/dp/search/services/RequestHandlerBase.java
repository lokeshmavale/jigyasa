package com.jigyasa.dp.search.services;

import com.google.common.base.Stopwatch;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import com.jigyasa.dp.search.metrics.MetricsRecorder;
import com.jigyasa.dp.search.metrics.NoopMetricsService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RequestHandlerBase<T, K> {
    private static final Logger log = LoggerFactory.getLogger(RequestHandlerBase.class);
    private static final MemoryCircuitBreaker CIRCUIT_BREAKER = new MemoryCircuitBreaker();
    private static volatile MetricsRecorder metrics = new NoopMetricsService();

    private final String handlerName;

    protected RequestHandlerBase(String handlerName) {
        this.handlerName = handlerName;
    }

    public static void setMetricsRecorder(MetricsRecorder recorder) {
        metrics = recorder;
    }

    public static MetricsRecorder getMetricsRecorder() {
        return metrics;
    }

    public static MemoryCircuitBreaker getCircuitBreaker() {
        return CIRCUIT_BREAKER;
    }

    protected final void handle(T req, StreamObserver<K> observer) {
        if (CIRCUIT_BREAKER.isTripped()) {
            metrics.recordRequest(handlerName, extractCollection(req), "rejected", 0);
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Memory circuit breaker tripped [#" + CIRCUIT_BREAKER.getTripCount()
                            + "]: heap usage above " + (int) (CIRCUIT_BREAKER.getThreshold() * 100)
                            + "%. Reduce request rate or increase heap (-Xmx).")
                    .asRuntimeException());
            return;
        }

        metrics.incrementActiveRequests(handlerName);
        Stopwatch sw = Stopwatch.createStarted();
        StatusTrackingObserver<K> tracker = new StatusTrackingObserver<>(observer);
        try {
            internalHandle(req, tracker);
        } catch (IllegalArgumentException e) {
            tracker.setStatus("invalid");
            throw e;
        } catch (Exception e) {
            tracker.setStatus("error");
            throw e;
        } finally {
            long durationMs = sw.elapsed().toMillis();
            metrics.decrementActiveRequests(handlerName);
            metrics.recordRequest(handlerName, extractCollection(req), tracker.getStatus(), durationMs);
            log.debug("{} completed in {}ms status={}", handlerName, durationMs, tracker.getStatus());
        }
    }

    protected String extractCollection(T req) {
        if (req instanceof MessageOrBuilder msg) {
            Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName("collection");
            if (fd != null) {
                Object val = msg.getField(fd);
                if (val instanceof String s && !s.isEmpty()) return s;
            }
        }
        return "default";
    }

    public abstract void internalHandle(T req, StreamObserver<K> observer);
}
