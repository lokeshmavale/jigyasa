package com.jigyasa.dp.search.services;

import com.jigyasa.dp.search.entrypoint.FileBasedSchemaReader;
import com.jigyasa.dp.search.entrypoint.GrpcServerWrapper;
import com.jigyasa.dp.search.entrypoint.IndexManager;
import com.jigyasa.dp.search.entrypoint.IndexSchemaReader;
import com.jigyasa.dp.search.handlers.*;
import com.jigyasa.dp.search.handlers.*;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.ServerMode;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Executors;

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
    public GrpcServerWrapper provideGrpcServerWrapper(Server server, IndexManager manager) {
        return new GrpcServerWrapper(server, manager);
    }

    @Provides
    @Singleton
    public IndexManager provideIndexManager(IndexSchemaManager manager,
                                            IndexSchemaReader indexSchemaReader) {
        return new IndexManager(manager, indexSchemaReader);
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
                                                           UpdateSchemaRequestHandler updateSchemaRequestHandler) {
        return new AnweshanDataPlaneImpl(indexRequestHandler, lookupRequestHandler, queryRequestHandler, updateSchemaRequestHandler);
    }

    @Provides
    public IndexRequestHandler provideIndexRequestHandler(DocIdOverlapLock lock, HandlerHelpers handlerHelpers) {
        return new IndexRequestHandler(lock, handlerHelpers);
    }

    @Provides
    @Singleton
    public HandlerHelpers provideHandlerHelpers(IndexSchemaManager schemaManager,
                                                IndexSearcherManagerISCH indexSearcherManager,
                                                IndexWriterManagerISCH indexWriterManager,
                                                TranslogAppenderManager translogAppenderManager) {
        return new HandlerHelpers(schemaManager, indexSearcherManager, indexWriterManager, translogAppenderManager);
    }

    @Provides
    @Singleton
    public TranslogAppenderManager provideTranslogAppenderManager(@Named("TranslogDirectory") String translogDirectory) {
        return new TranslogAppenderManager(translogDirectory);
    }

    @Provides
    @Singleton
    public DocIdOverlapLock provideDocIdOverlapLock(@Named("DocIdOverlapLockTimeoutMillis") long timeoutMillis) {
        return new DocIdOverlapLock(timeoutMillis);
    }

    @Provides
    public QueryRequestHandler provideQueryRequestHandler() {
        return new QueryRequestHandler();
    }

    @Provides
    public LookupRequestHandler provideLookupRequestHandler(HandlerHelpers helpers) {
        return new LookupRequestHandler(helpers);
    }

    @Provides
    public UpdateSchemaRequestHandler provideUpdateSchemaRequestHandler(IndexSchemaManager manager) {
        return new UpdateSchemaRequestHandler(manager);
    }

    @Provides
    @Singleton
    public IndexSchemaManager provideIndexSchemaManager(@Named("ServerMode") ServerMode serverMode,
                                                        InitializedSchemaISCH initializedSchemaISCH,
                                                        IndexWriterManagerISCH indexWriterManagerISCH,
                                                        IndexSearcherManagerISCH indexSearcherManagerISCH,
                                                        TranslogAppenderManager translogAppenderManager,
                                                        RecoveryCommitServiceISCH recoveryCommitService) {

        IndexSchemaManager indexSchemaManager = new IndexSchemaManager();
        indexSchemaManager.addHandler(initializedSchemaISCH);

        if (serverMode == ServerMode.WRITE) {
            indexSchemaManager.addHandler(translogAppenderManager);
            indexSchemaManager.addHandler(indexWriterManagerISCH);
            indexSchemaManager.addHandler(recoveryCommitService);
        }

        if (serverMode == ServerMode.READ_WRITE || serverMode == ServerMode.READ) {
            indexSchemaManager.addHandler(indexSearcherManagerISCH);
        }

        return indexSchemaManager;
    }

    @Provides
    @Singleton
    public RecoveryCommitServiceISCH provideRecoveryCommitServiceISCH(IndexWriterManagerISCH indexWriterManagerISCH,
                                                                      TranslogAppenderManager translogAppenderManager) {
        return new RecoveryCommitServiceISCH(indexWriterManagerISCH, translogAppenderManager, Executors.newSingleThreadScheduledExecutor());
    }


    @Provides
    @Singleton
    public IndexWriterManagerISCH provideIndexWriterManagerISCH(@Named("IndexCacheDirectory") String indexCacheDirectory) {
        return new IndexWriterManagerISCH(indexCacheDirectory);
    }

    @Provides
    @Singleton
    public IndexSearcherManagerISCH provideIndexSearcherManagerISCH(@Named("ServerMode") ServerMode serverMode,
                                                                    IndexWriterManagerISCH writerManagerISCH,
                                                                    @Named("IndexCacheDirectory") String indexCacheDirectory) {
        return new IndexSearcherManagerISCH(serverMode, writerManagerISCH, indexCacheDirectory);
    }

    @Provides
    @Singleton
    public InitializedSchemaISCH provideInitializedSchemaISCH() {
        return new InitializedSchemaISCH();
    }

}
