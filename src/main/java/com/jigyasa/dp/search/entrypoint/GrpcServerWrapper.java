package com.jigyasa.dp.search.entrypoint;

import com.google.inject.Inject;
import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.IndexSchema;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class GrpcServerWrapper {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerWrapper.class);

    private final Server server;
    private final CollectionRegistry registry;
    private final IndexSchemaReader schemaReader;

    @Inject
    public GrpcServerWrapper(Server server, CollectionRegistry registry,
                             IndexSchemaReader schemaReader) {
        this.server = server;
        this.registry = registry;
        this.schemaReader = schemaReader;
    }

    public void start() {
        try {
            // Read default schema and initialize the default collection
            IndexSchema defaultSchema = schemaReader.readSchema();
            registry.initialize(defaultSchema);
            server.start();
            log.info("gRPC server started on port {}", server.getPort());
            registerShutdownHook();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start server", e);
        }
    }

    public void waitTillShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping server gracefully");
            shutdown();
        }, "jigyasa-shutdown"));
    }

    public void shutdown() {
        try {
            server.shutdown();
            if (!server.awaitTermination(15, TimeUnit.SECONDS)) {
                log.warn("gRPC server did not terminate in 15s, forcing shutdown");
                server.shutdownNow();
                server.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown interrupted, forcing immediate shutdown");
            server.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error during shutdown", e);
            server.shutdownNow();
        } finally {
            // Always close collections even if server shutdown fails/is interrupted
            try {
                registry.shutdownAll();
            } catch (Exception e) {
                log.error("Error shutting down collection registry", e);
            }
        }
        log.info("Server shutdown complete");
    }
}
