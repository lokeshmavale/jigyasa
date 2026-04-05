package com.jigyasa.dp.search.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ShutdownUtils {
    private static final Logger log = LoggerFactory.getLogger(ShutdownUtils.class);

    private ShutdownUtils() {}

    public static void shutdownAndAwait(ExecutorService executor, String name, long timeoutSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("{} did not terminate in {}s, forcing shutdown", name, timeoutSeconds);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
