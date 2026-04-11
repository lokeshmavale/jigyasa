package com.jigyasa.dp.search.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-grade memory circuit breaker inspired by Elasticsearch's
 * HierarchyCircuitBreakerService (indices.breaker.total.use_real_memory=true).
 *
 * <h3>How it works:</h3>
 * <ol>
 *   <li>Checks real JVM heap usage via {@link MemoryMXBean#getHeapMemoryUsage()}</li>
 *   <li>If above threshold, attempts a GC nudge (only once per cooldown period)</li>
 *   <li>Re-checks after GC — if still above threshold, trips the breaker</li>
 *   <li>Auto-recovers when heap drops below threshold</li>
 * </ol>
 *
 * <h3>What's protected (user-facing):</h3>
 * Index, Query, Lookup, DeleteByQuery, UpdateSchema — all via {@link RequestHandlerBase}.
 *
 * <h3>What's NOT protected (background):</h3>
 * Periodic commits, TTL sweeps, translog flushes, NRT reopen — these run on their own
 * ScheduledExecutorService and never pass through RequestHandlerBase.
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li>{@code CIRCUIT_BREAKER_HEAP_THRESHOLD} — trip threshold as fraction (default: 0.95, like ES)</li>
 *   <li>{@code CIRCUIT_BREAKER_ENABLED} — set to "false" to disable (default: true)</li>
 * </ul>
 *
 * <h3>Design decisions (from ES):</h3>
 * <ul>
 *   <li>Uses real heap (not estimated) — more accurate under GC pressure</li>
 *   <li>GC nudge before tripping — avoids false positives from unreachable objects</li>
 *   <li>Handles JDK-8207200 race condition in MemoryMXBean gracefully</li>
 *   <li>Trip counter for monitoring/alerting</li>
 *   <li>Log throttling to prevent log flood under sustained pressure</li>
 * </ul>
 */
public class MemoryCircuitBreaker {
    private static final Logger log = LoggerFactory.getLogger(MemoryCircuitBreaker.class);

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private final double threshold;
    private final boolean enabled;

    private final AtomicLong tripCount = new AtomicLong(0);
    private volatile boolean tripped = false;

    // GC nudge cooldown: don't trigger System.gc() more than once per 30s
    private volatile long lastGcNudgeMs = 0;
    private static final long GC_NUDGE_COOLDOWN_MS = 30_000;

    // Log throttling: don't log more than once per 5s when tripped
    private volatile long lastTripLogMs = 0;
    private static final long TRIP_LOG_INTERVAL_MS = 5_000;

    public MemoryCircuitBreaker() {
        this(
            parseDouble(System.getenv().getOrDefault("CIRCUIT_BREAKER_HEAP_THRESHOLD", "0.95")),
            !"false".equalsIgnoreCase(System.getenv().getOrDefault("CIRCUIT_BREAKER_ENABLED", "true"))
        );
    }

    public MemoryCircuitBreaker(double threshold, boolean enabled) {
        this.threshold = (threshold <= 0 || threshold > 1.0) ? 0.95 : threshold;
        this.enabled = enabled;
        if (enabled) {
            long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
            long thresholdMb = (long) (maxHeapMb * this.threshold);
            log.info("Memory circuit breaker enabled: threshold={}% ({}MB of {}MB heap)",
                    (int) (this.threshold * 100), thresholdMb, maxHeapMb);
        } else {
            log.info("Memory circuit breaker disabled");
        }
    }

    /**
     * Returns true if the breaker is tripped and the request should be rejected.
     * Cheap fast-path: if not near threshold, returns false with minimal overhead
     * (~50ns for MemoryMXBean read on modern JVMs).
     */
    public boolean isTripped() {
        if (!enabled) return false;

        long used = safeHeapUsed();
        long max = Runtime.getRuntime().maxMemory();
        if (max <= 0) return false;

        double usage = (double) used / max;

        if (usage < threshold) {
            if (tripped) {
                tripped = false;
                log.info("Circuit breaker RECOVERED: heap {}MB/{}MB ({}%)",
                        used / (1024 * 1024), max / (1024 * 1024), (int) (usage * 100));
            }
            return false;
        }

        // Above threshold — attempt GC nudge before tripping (ES's OverLimitStrategy)
        long now = System.currentTimeMillis();
        if (now - lastGcNudgeMs > GC_NUDGE_COOLDOWN_MS) {
            lastGcNudgeMs = now;
            log.debug("Circuit breaker: heap at {}%, nudging GC before tripping", (int) (usage * 100));
            System.gc();

            // Re-check after GC nudge
            used = safeHeapUsed();
            usage = (double) used / max;
            if (usage < threshold) {
                log.debug("Circuit breaker: GC freed enough memory, heap now at {}%", (int) (usage * 100));
                return false;
            }
        }

        // Still above threshold after GC nudge — trip
        if (!tripped) {
            tripped = true;
            tripCount.incrementAndGet();
        }

        if (now - lastTripLogMs > TRIP_LOG_INTERVAL_MS) {
            lastTripLogMs = now;
            log.warn("Circuit breaker TRIPPED [#{}]: heap {}MB/{}MB ({}%) >= threshold {}%. "
                            + "Rejecting user requests until memory pressure subsides.",
                    tripCount.get(), used / (1024 * 1024), max / (1024 * 1024),
                    (int) (usage * 100), (int) (threshold * 100));
        }
        return true;
    }

    /**
     * Reads current heap usage safely, handling JDK-8207200 race condition
     * where MemoryMXBean can throw IllegalArgumentException during GC.
     * ES handles this same bug in HierarchyCircuitBreakerService.realMemoryUsage().
     */
    private static long safeHeapUsed() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException e) {
            // JDK-8207200: race condition during GC. Return 0 to avoid false trip.
            log.debug("MemoryMXBean race condition (JDK-8207200), returning 0");
            return 0;
        }
    }

    /** Total number of times the breaker has tripped. Useful for monitoring/alerting. */
    public long getTripCount() {
        return tripCount.get();
    }

    public double getThreshold() {
        return threshold;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.95;
        }
    }
}
