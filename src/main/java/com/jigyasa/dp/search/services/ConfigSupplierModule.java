package com.jigyasa.dp.search.services;

import com.jigyasa.dp.search.configs.EnvironmentVariables;
import com.jigyasa.dp.search.models.ServerMode;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.nio.file.Path;
import java.util.Optional;

public class ConfigSupplierModule extends AbstractModule {

    @Provides
    @Singleton
    @Named("IndexCacheDirectory")
    public String provideIndexCacheDir() {
        return EnvironmentVariables.INDEX_CACHE_DIR.defaultIfEmpty();
    }

    @Provides
    @Singleton
    @Named("GrpcServerPort")
    public String provideGrpcServerPort() {
        return EnvironmentVariables.GRPC_SERVER_PORT.defaultIfEmpty();
    }

    @Provides
    @Singleton
    @Named("ServerMode")
    public ServerMode provideServerMode() {
        return ServerMode.valueOf(EnvironmentVariables.SERVER_MODE.defaultIfEmpty());
    }

    @Provides
    @Singleton
    @Named("IndexSchemaPath")
    public Optional<Path> provideIndexSchemaPath() {
        if (null != EnvironmentVariables.INDEX_SCHEMA_PATH.get()) {
            return Optional.of(Path.of(EnvironmentVariables.INDEX_SCHEMA_PATH.get()));
        }
        return Optional.empty();
    }

    @Provides
    @Singleton
    @Named("DocIdOverlapLockTimeoutMillis")
    public long provideDocIdOverlapTimeout() {
        return Long.parseLong(EnvironmentVariables.DOCID_OVERLAP_TIMEOUT_MS.defaultIfEmpty());
    }

    @Provides
    @Singleton
    @Named("TranslogDirectory")
    public String provideTranslogDirectory() {
        return EnvironmentVariables.TRANSLOG_DIRECTORY.defaultIfEmpty();
    }
}
