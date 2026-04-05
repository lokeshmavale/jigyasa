package com.jigyasa.dp.search.entrypoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Bootstrap checks run at startup to warn about suboptimal production configurations.
 * Inspired by Elasticsearch's bootstrap checks (memory_lock, max file descriptors, etc.).
 */
public final class BootstrapChecks {
    private static final Logger log = LoggerFactory.getLogger(BootstrapChecks.class);

    private BootstrapChecks() {}

    public static void run() {
        checkHeapSize();
        checkMemoryLock();
        checkAlwaysPreTouch();
        checkVectorSIMD();
    }

    /**
     * Warn if heap is less than 512MB — Lucene benefits from large heaps for
     * segment merges and NRT searcher refreshes.
     */
    private static void checkHeapSize() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        long maxHeap = mem.getHeapMemoryUsage().getMax();
        if (maxHeap > 0 && maxHeap < 512 * 1024 * 1024L) {
            log.warn("BOOTSTRAP CHECK: Heap size is {}MB — recommend at least 512MB for production (-Xmx512m)",
                    maxHeap / (1024 * 1024));
        }
    }

    /**
     * Attempt to lock JVM memory into physical RAM via mlockall() (Linux/macOS)
     * or VirtualLock (Windows). Prevents OS from swapping heap pages to disk,
     * which would cause GC pauses to spike from milliseconds to seconds.
     *
     * Enabled by setting BOOTSTRAP_MEMORY_LOCK=true.
     * Requires: ulimit -l unlimited on Linux.
     */
    private static void checkMemoryLock() {
        String lockEnv = System.getenv("BOOTSTRAP_MEMORY_LOCK");
        if ("true".equalsIgnoreCase(lockEnv)) {
            boolean locked = NativeMemoryLock.tryLockMemory();
            if (!locked) {
                log.warn("BOOTSTRAP CHECK: BOOTSTRAP_MEMORY_LOCK=true but memory lock failed. " +
                        "JVM heap may be swapped to disk under memory pressure, degrading p99 latency. " +
                        "See log above for fix instructions, or use -XX:+AlwaysPreTouch as partial mitigation.");
            }
        }
    }

    /**
     * Check if -XX:+AlwaysPreTouch is set. This pre-faults heap pages at startup,
     * ensuring they're resident in physical RAM. Not a substitute for mlockall
     * (pages can still be swapped out later), but better than nothing.
     */
    private static void checkAlwaysPreTouch() {
        boolean found = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .anyMatch(arg -> arg.contains("AlwaysPreTouch"));
        if (!found) {
            log.warn("BOOTSTRAP CHECK: -XX:+AlwaysPreTouch not set — heap pages may be swapped out under memory pressure. " +
                    "Add '-XX:+AlwaysPreTouch' to JVM args for production deployments.");
        }
    }

    /**
     * Check if Lucene's SIMD vectorization is active via jdk.incubator.vector.
     * Without this, HNSW distance computations (dot product, cosine similarity)
     * fall back to scalar math — up to 4-8x slower for vector search.
     *
     * Also checks FMA (Fused Multiply-Add) settings:
     * - lucene.useScalarFMA: FMA for scalar operations
     * - lucene.useVectorFMA: FMA for SIMD operations
     * Both default to "auto" which is correct for most CPUs.
     */
    private static void checkVectorSIMD() {
        boolean simdAvailable = false;
        try {
            // Lucene's VectorizationProvider logs a WARNING if the module is not readable.
            // We check the module system directly.
            Module vectorModule = ModuleLayer.boot().findModule("jdk.incubator.vector").orElse(null);
            simdAvailable = vectorModule != null;
        } catch (Exception e) {
            // Module API not available or other issue
        }

        if (!simdAvailable) {
            log.warn("BOOTSTRAP CHECK: jdk.incubator.vector module NOT enabled. " +
                    "Vector search (HNSW dot product, cosine similarity) will use scalar math — up to 4-8x slower. " +
                    "Add '--add-modules jdk.incubator.vector' to JVM args. " +
                    "Example: java --add-modules jdk.incubator.vector -jar jigyasa.jar");
        } else {
            log.info("BOOTSTRAP CHECK: SIMD vectorization enabled (jdk.incubator.vector module active)");

            // Log FMA settings for visibility
            String scalarFMA = System.getProperty("lucene.useScalarFMA", "auto");
            String vectorFMA = System.getProperty("lucene.useVectorFMA", "auto");
            log.info("BOOTSTRAP CHECK: Lucene FMA settings — scalar={}, vector={} (auto = CPU-optimal)",
                    scalarFMA, vectorFMA);
        }
    }
}
