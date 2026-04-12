package com.jigyasa.dp.search.metrics;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.services.MemoryCircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Prometheus metrics implementation using Micrometer.
 *
 * <p>Exposes an HTTP /metrics endpoint on a configurable port (default 9090)
 * that Prometheus scrapes. All metrics use the {@code jigyasa_} prefix.
 *
 * <p>Metric categories:
 * <ul>
 *   <li>Request: duration histogram, total counter, active gauge</li>
 *   <li>Facet: duration by type</li>
 *   <li>Index: doc count, write duration</li>
 *   <li>Collection: doc count, segments, store size (lazy gauges)</li>
 *   <li>Circuit breaker: trip count, status</li>
 *   <li>Thread pools: queue size, active, rejected</li>
 *   <li>JVM: heap, GC, threads, CPU (automatic via Micrometer binders)</li>
 * </ul>
 */
public class PrometheusMetricsService implements MetricsService {
    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsService.class);

    private static final Duration[] SLO_BUCKETS;
    static {
        long[] bucketsMs = {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000};
        SLO_BUCKETS = new Duration[bucketsMs.length];
        for (long i = 0; i < bucketsMs.length; i++) {
            SLO_BUCKETS[(int) i] = Duration.ofMillis(bucketsMs[(int) i]);
        }
    }

    private record MeterKey(String prefix, String a, String b, String c) {
        MeterKey(String prefix, String a, String b) { this(prefix, a, b, null); }
        MeterKey(String prefix, String a) { this(prefix, a, null, null); }
    }

    private final PrometheusMeterRegistry registry;
    private final CollectionRegistry collectionRegistry;
    private final long port;
    private HTTPServer httpServer;

    private final JvmGcMetrics jvmGcMetrics;

    private final ConcurrentHashMap<String, AtomicInteger> activeGauges = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<MeterKey, Timer> timerCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MeterKey, Counter> counterCache = new ConcurrentHashMap<>();

    private static final String UNKNOWN_COLLECTION = "_unknown";

    public PrometheusMetricsService(long port, CollectionRegistry collectionRegistry,
                                     MemoryCircuitBreaker circuitBreaker,
                                     ThreadPoolExecutor handlerExecutor) {
        this.port = port;
        this.collectionRegistry = collectionRegistry;
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        // JVM metrics — automatic, ~30 metrics for free
        new JvmMemoryMetrics().bindTo(registry);
        this.jvmGcMetrics = new JvmGcMetrics();
        jvmGcMetrics.bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);

        if (circuitBreaker != null) {
            Gauge.builder("jigyasa_circuit_breaker_status", circuitBreaker,
                    b -> b.isTrippedStatus() ? 1.0 : 0.0).register(registry);
            Gauge.builder("jigyasa_circuit_breaker_trips_total", circuitBreaker,
                    b -> (double) b.getTripCount()).register(registry);
        }

        // Collection gauges (lazy — computed only on scrape)
        if (collectionRegistry != null) {
            Gauge.builder("jigyasa_collections_total", collectionRegistry,
                    r -> r.getHealthForAll().size()).register(registry);
        }

        // Thread pool gauges
        if (handlerExecutor != null) {
            Gauge.builder("jigyasa_threadpool_active", handlerExecutor,
                    ThreadPoolExecutor::getActiveCount).tag("pool", "handler").register(registry);
            Gauge.builder("jigyasa_threadpool_queue_size", handlerExecutor,
                    e -> e.getQueue().size()).tag("pool", "handler").register(registry);
        }

        log.info("Prometheus metrics initialized on port {} ({} meters registered)",
                port, registry.getMeters().size());
    }

    @Override
    public void start() {
        try {
            this.httpServer = HTTPServer.builder()
                    .port((int) port)
                    .registry(registry.getPrometheusRegistry())
                    .buildAndStart();
            log.info("Metrics HTTP server started on port {}", port);
        } catch (IOException e) {
            log.error("Failed to start metrics HTTP server on port {}", port, e);
        }
    }

    @Override
    public void stop() {
        if (httpServer != null) {
            httpServer.close();
            log.info("Metrics HTTP server stopped");
        }
        jvmGcMetrics.close();
        registry.close();
    }

    @Override
    public void recordRequest(String rpc, String collection, String status, long durationMs) {
        String col = sanitizeCollection(collection);
        MeterKey timerKey = new MeterKey("req_dur", rpc, col, status);
        Timer timer = timerCache.computeIfAbsent(timerKey, k ->
                Timer.builder("jigyasa_request_duration_seconds")
                        .tag("rpc", rpc)
                        .tag("collection", col)
                        .tag("status", status)
                        .publishPercentileHistogram()
                        .serviceLevelObjectives(SLO_BUCKETS)
                        .register(registry));
        timer.record(Duration.ofMillis(durationMs));

        MeterKey counterKey = new MeterKey("req_tot", rpc, col, status);
        Counter counter = counterCache.computeIfAbsent(counterKey, k ->
                Counter.builder("jigyasa_requests_total")
                        .tag("rpc", rpc)
                        .tag("collection", col)
                        .tag("status", status)
                        .register(registry));
        counter.increment();
    }

    @Override
    public void incrementActiveRequests(String rpc) {
        activeGauge(rpc).incrementAndGet();
    }

    @Override
    public void decrementActiveRequests(String rpc) {
        activeGauge(rpc).decrementAndGet();
    }

    @Override
    public void recordFacet(String collection, String facetType, long durationMs) {
        String col = sanitizeCollection(collection);
        MeterKey key = new MeterKey("facet", col, facetType);
        Timer timer = timerCache.computeIfAbsent(key, k ->
                Timer.builder("jigyasa_facet_duration_seconds")
                        .tag("collection", col)
                        .tag("type", facetType)
                        .publishPercentileHistogram()
                        .register(registry));
        timer.record(Duration.ofMillis(durationMs));
    }

    @Override
    public void recordIndexDuration(String collection, long durationMs) {
        String col = sanitizeCollection(collection);
        MeterKey key = new MeterKey("idx_dur", col);
        Timer timer = timerCache.computeIfAbsent(key, k ->
                Timer.builder("jigyasa_index_duration_seconds")
                        .tag("collection", col)
                        .publishPercentileHistogram()
                        .register(registry));
        timer.record(Duration.ofMillis(durationMs));
    }

    @Override
    public void recordIndexedDocs(String collection, long count) {
        String col = sanitizeCollection(collection);
        MeterKey key = new MeterKey("idx_docs", col);
        Counter counter = counterCache.computeIfAbsent(key, k ->
                Counter.builder("jigyasa_index_docs_total")
                        .tag("collection", col)
                        .register(registry));
        counter.increment(count);
    }

    // ---- Helpers ----

    /**
     * Gets or creates an AtomicInteger gauge for an RPC name (OCP — no switch).
     * New RPCs auto-register a Micrometer gauge on first use.
     */
    private AtomicInteger activeGauge(String rpc) {
        return activeGauges.computeIfAbsent(rpc, name -> {
            AtomicInteger gauge = new AtomicInteger();
            Gauge.builder("jigyasa_active_requests", gauge, AtomicInteger::get)
                    .tag("rpc", name).register(registry);
            return gauge;
        });
    }

    /**
     * Validates collection name against known collections to prevent cardinality explosion.
     * Unknown collections (e.g., from malformed requests) are bucketed into "_unknown".
     */
    private String sanitizeCollection(String collection) {
        if (collection == null || collection.isEmpty()) return "default";
        if (collectionRegistry != null) {
            if (!collectionRegistry.listCollections().contains(collection)) return UNKNOWN_COLLECTION;
        }
        return collection;
    }
}
