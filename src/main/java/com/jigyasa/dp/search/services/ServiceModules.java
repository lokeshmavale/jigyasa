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
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.nio.file.Path;
import java.util.Optional;

public class ServiceModules extends AbstractModule {

    @Provides
    @Singleton
    public Server provideGrpcServer(AnweshanDataPlaneImpl anweshanDataPlaneService,
                                    @Named("GrpcServerPort") String port) {
        return ServerBuilder.forPort(Integer.parseInt(port))
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
