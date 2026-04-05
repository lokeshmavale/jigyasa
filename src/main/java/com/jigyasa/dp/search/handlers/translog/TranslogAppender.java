package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexRequest;

import java.io.IOException;
import java.util.List;

public interface TranslogAppender {

    void append(IndexRequest request) throws IOException;

    void reset();

    List<IndexRequest> getData();

    /** Release resources (file handles, flush threads). Called during collection shutdown. */
    default void shutdown() {}
}
