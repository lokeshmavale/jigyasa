package com.jigyasa.dp.search.services;

import com.google.common.base.Stopwatch;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public abstract class RequestHandlerBase<T, K> {
    private static final Logger log = LoggerFactory.getLogger(RequestHandlerBase.class);
    private final String handlerName;

    protected final void handle(T req, StreamObserver<K> observer) {
        Stopwatch sw = Stopwatch.createStarted();
        internalHandle(req, observer);
        log.debug("{} completed in {}ms", handlerName, sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public abstract void internalHandle(T req, StreamObserver<K> observer);
}
