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
}
