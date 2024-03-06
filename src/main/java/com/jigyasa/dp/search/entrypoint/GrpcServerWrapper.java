package com.jigyasa.dp.search.entrypoint;

import com.google.inject.Inject;
import io.grpc.Server;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor(access = AccessLevel.PUBLIC, onConstructor = @__({@Inject}))
public class GrpcServerWrapper {
    private final Server server;
    private final IndexManager manager;

    public void start() {
        try {
            manager.initialize();
            log.info("Index initialized");
            server.start();
            log.info("Server Started at port: " + server.getPort());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void waitTillShutdown() throws InterruptedException {
        server.awaitTermination();
    }
}
