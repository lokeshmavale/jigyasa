package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.services.ConfigSupplierModule;
import com.jigyasa.dp.search.services.ServiceModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Main {

    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(new ServiceModules(), new ConfigSupplierModule());
            GrpcServerWrapper server = injector.getInstance(GrpcServerWrapper.class);
            server.start();
            System.out.println("Server Started Successfully");
            server.waitTillShutdown();
        } catch (Exception e) {
            //Todo: Add proper exception handling
            e.printStackTrace();
        }
    }
}