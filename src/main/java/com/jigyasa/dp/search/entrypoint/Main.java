package com.jigyasa.dp.search.entrypoint;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.jigyasa.dp.search.services.ConfigSupplierModule;
import com.jigyasa.dp.search.services.ServiceModules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(new ServiceModules(), new ConfigSupplierModule());
            GrpcServerWrapper server = injector.getInstance(GrpcServerWrapper.class);
            server.start();
            log.info("Server started successfully");
            server.waitTillShutdown();
        } catch (Exception e) {
            log.error("Fatal error during server startup", e);
            System.exit(1);
        }
    }
}