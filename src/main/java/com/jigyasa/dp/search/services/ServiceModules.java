package com.jigyasa.dp.search.services;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.entrypoint.FileBasedSchemaReader;
import com.jigyasa.dp.search.entrypoint.GrpcServerWrapper;
import com.jigyasa.dp.search.entrypoint.IndexManager;
import com.jigyasa.dp.search.entrypoint.IndexSchemaReader;
import com.jigyasa.dp.search.handlers.DeleteByQueryRequestHandler;
import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.LookupRequestHandler;
import com.jigyasa.dp.search.handlers.QueryRequestHandler;
import com.jigyasa.dp.search.handlers.UpdateSchemaRequestHandler;
import com.jigyasa.dp.search.models.ServerMode;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServiceModules extends AbstractModule {

    // Max queued requests before rejecting. ES uses 1000 for search.
    // At ~5ms per request, 1000 queued = ~5s max wait time.
    private static final int HANDLER_QUEUE_CAPACITY = 1000;

    @Provides
    @Singleton
    public Server provideGrpcServer(AnweshanDataPlaneImpl anweshanDataPlaneService,
                                    @Named("GrpcServerPort") String port) {
        int cpus = Runtime.getRuntime().availableProcessors();

        // Netty event loop for I/O only (accept + read/write) — 2 threads is sufficient
        // since Jigyasa is single-node and gRPC multiplexes over few TCP connections.
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(2,
                new DefaultThreadFactory("jigyasa-io", true));

        // Dedicated handler thread pool — bounded queue (ES-style) prevents OOM under burst.
        // Queue capacity 1000: at ~5ms/request, this is ~5s of buffered work.
        // When full, ThreadPoolExecutor.CallerRunsPolicy executes on the gRPC I/O thread,
        // applying natural backpressure without dropping requests.
        ExecutorService handlerExecutor = new ThreadPoolExecutor(
                cpus, cpus, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(HANDLER_QUEUE_CAPACITY),
                new DefaultThreadFactory("jigyasa-handler", true),
                new ThreadPoolExecutor.CallerRunsPolicy());

        return NettyServerBuilder.forPort(Integer.parseInt(port))
                .executor(handlerExecutor)
                .bossEventLoopGroup(eventLoopGroup)
                .workerEventLoopGroup(eventLoopGroup)
                .channelType(NioServerSocketChannel.class)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .keepAliveTime(5, TimeUnit.MINUTES)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .addService(ProtoReflectionService.newInstance())
                .addService(anweshanDataPlaneService)
                .build();
    }

    @Provides
    @Singleton
    public GrpcServerWrapper provideGrpcServerWrapper(Server server,
                                                       CollectionRegistry registry,
                                                       IndexSchemaReader schemaReader) {
        return new GrpcServerWrapper(server, registry, schemaReader);
    }

    @Provides
    @Singleton
    public CollectionRegistry provideCollectionRegistry(
            @Named("IndexCacheDirectory") String indexCacheDirectory,
            @Named("TranslogDirectory") String translogDirectory,
            @Named("ServerMode") ServerMode serverMode) {
        return new CollectionRegistry(indexCacheDirectory, translogDirectory, serverMode);
    }

    @Provides
    @Singleton
    public IndexSchemaReader provideIndexSchemaReader(@Named("IndexSchemaPath") Optional<Path> schemaPath) {
        return new FileBasedSchemaReader(schemaPath);
    }

    @Provides
    @Singleton
    public AnweshanDataPlaneImpl providesAnweshanDataPlane(IndexRequestHandler indexRequestHandler,
                                                           QueryRequestHandler queryRequestHandler,
                                                           LookupRequestHandler lookupRequestHandler,
                                                           DeleteByQueryRequestHandler deleteByQueryRequestHandler,
                                                           UpdateSchemaRequestHandler updateSchemaRequestHandler,
                                                           IndexManager indexManager) {
        return new AnweshanDataPlaneImpl(indexRequestHandler, lookupRequestHandler, queryRequestHandler,
                deleteByQueryRequestHandler, updateSchemaRequestHandler, indexManager);
    }

    @Provides
    @Singleton
    public IndexManager provideIndexManager(CollectionRegistry registry) {
        return new IndexManager(registry);
    }

    @Provides
    public DeleteByQueryRequestHandler provideDeleteByQueryRequestHandler(CollectionRegistry registry) {
        return new DeleteByQueryRequestHandler(registry);
    }

    @Provides
    public IndexRequestHandler provideIndexRequestHandler(DocIdOverlapLock lock, CollectionRegistry registry) {
        return new IndexRequestHandler(lock, registry);
    }

    @Provides
    @Singleton
    public DocIdOverlapLock provideDocIdOverlapLock(@Named("DocIdOverlapLockTimeoutMillis") long timeoutMillis) {
        return new DocIdOverlapLock(timeoutMillis);
    }

    @Provides
    public QueryRequestHandler provideQueryRequestHandler(CollectionRegistry registry) {
        return new QueryRequestHandler(registry);
    }

    @Provides
    public LookupRequestHandler provideLookupRequestHandler(CollectionRegistry registry) {
        return new LookupRequestHandler(registry);
    }

    @Provides
    public UpdateSchemaRequestHandler provideUpdateSchemaRequestHandler(CollectionRegistry registry) {
        return new UpdateSchemaRequestHandler(registry);
    }
}
