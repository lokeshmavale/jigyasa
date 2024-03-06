package com.jigyasa.dp.search.services;

import com.google.common.base.Stopwatch;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public abstract class RequestHandlerBase<T, K> {
    private final String handlerName;

    protected final void handle(T req, StreamObserver<K> observer) {
        //Todo: Update with sophisticated logging
        Stopwatch sw = Stopwatch.createStarted();
        internalHandle(req, observer);
        System.out.println("Total Time Take to execute " + handlerName + " : " + sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public abstract void internalHandle(T req, StreamObserver<K> observer);
}
