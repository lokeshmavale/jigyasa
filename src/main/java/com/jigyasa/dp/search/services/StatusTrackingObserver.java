package com.jigyasa.dp.search.services;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Wraps a {@link StreamObserver} to detect {@code onError} calls, ensuring
 * metrics correctly record error status even when handlers catch exceptions
 * internally and call {@code observer.onError()} without re-throwing.
 */
final class StatusTrackingObserver<K> implements StreamObserver<K> {
    private final StreamObserver<K> delegate;
    private volatile String status = "ok";

    StatusTrackingObserver(StreamObserver<K> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(K value) {
        delegate.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
        if (status.equals("ok")) {
            status = statusFromThrowable(t);
        }
        delegate.onError(t);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }

    String getStatus() { return status; }
    void setStatus(String s) { this.status = s; }

    private static String statusFromThrowable(Throwable t) {
        if (t instanceof StatusRuntimeException sre) {
            return switch (sre.getStatus().getCode()) {
                case INVALID_ARGUMENT -> "invalid";
                case RESOURCE_EXHAUSTED -> "rejected";
                default -> "error";
            };
        }
        return "error";
    }
}
