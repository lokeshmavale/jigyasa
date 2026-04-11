package com.jigyasa.dp.search.services;

import com.google.common.base.Stopwatch;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class RequestHandlerBase<T, K> {
    private static final Logger log = LoggerFactory.getLogger(RequestHandlerBase.class);
    private static final MemoryCircuitBreaker CIRCUIT_BREAKER = new MemoryCircuitBreaker();

    private final String handlerName;

    protected RequestHandlerBase(String handlerName) {
        this.handlerName = handlerName;
    }

    /** Exposed for Health API to report circuit breaker state. */
    public static MemoryCircuitBreaker getCircuitBreaker() {
        return CIRCUIT_BREAKER;
    }

    protected final void handle(T req, StreamObserver<K> observer) {
        if (CIRCUIT_BREAKER.isTripped()) {
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Memory circuit breaker tripped [#" + CIRCUIT_BREAKER.getTripCount()
                            + "]: heap usage above " + (int) (CIRCUIT_BREAKER.getThreshold() * 100)
                            + "%. Reduce request rate or increase heap (-Xmx).")
                    .asRuntimeException());
            return;
        }
        Stopwatch sw = Stopwatch.createStarted();
        internalHandle(req, observer);
        log.debug("{} completed in {}ms", handlerName, sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public abstract void internalHandle(T req, StreamObserver<K> observer);
}
