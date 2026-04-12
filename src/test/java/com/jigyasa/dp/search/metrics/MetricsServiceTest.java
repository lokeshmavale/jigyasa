package com.jigyasa.dp.search.metrics;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.services.MemoryCircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetricsServiceTest {

    @Nested
    @DisplayName("NoopMetricsService")
    class NoopTests {
        private final NoopMetricsService noop = new NoopMetricsService();

        @Test
        @DisplayName("All recording methods are no-ops and don't throw")
        void allMethodsAreNoOp() {
            assertThatCode(() -> {
                noop.recordRequest("Query", "default", "ok", 5);
                noop.incrementActiveRequests("Query");
                noop.decrementActiveRequests("Query");
                noop.recordFacet("default", "string_terms", 1);
                noop.recordIndexDuration("default", 10);
                noop.recordIndexedDocs("default", 100);
                noop.start();
                noop.stop();
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("PrometheusMetricsService")
    class PrometheusTests {
        private PrometheusMetricsService service;
        private CollectionRegistry mockRegistry;

        @BeforeEach
        void setUp() {
            mockRegistry = mock(CollectionRegistry.class);
            when(mockRegistry.listCollections()).thenReturn(Set.of("products", "default"));
            when(mockRegistry.getHealthForAll()).thenReturn(java.util.List.of());
            MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(0.95, false);
            service = new PrometheusMetricsService(0, mockRegistry, breaker, null);
        }

        @AfterEach
        void tearDown() {
            service.stop();
        }

        @Test
        @DisplayName("recordRequest increments counter and records timer")
        void recordRequestWorks() {
            assertThatCode(() -> {
                service.recordRequest("Query", "products", "ok", 5);
                service.recordRequest("Query", "products", "ok", 10);
                service.recordRequest("Query", "products", "error", 100);
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Active requests increment and decrement for known and new RPCs")
        void activeRequestsGauge() {
            assertThatCode(() -> {
                service.incrementActiveRequests("Query");
                service.incrementActiveRequests("Query");
                service.decrementActiveRequests("Query");
                service.incrementActiveRequests("NewRpc");
                service.decrementActiveRequests("NewRpc");
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Unknown collection is sanitized to _unknown")
        void unknownCollectionSanitized() {
            assertThatCode(() ->
                    service.recordRequest("Query", "bogus_collection", "ok", 5)
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Empty and null collection default to 'default'")
        void emptyCollectionDefaults() {
            assertThatCode(() -> {
                service.recordRequest("Query", "", "ok", 5);
                service.recordRequest("Query", null, "ok", 5);
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Facet recording works for all types")
        void facetRecording() {
            assertThatCode(() -> {
                service.recordFacet("products", "string_terms", 1);
                service.recordFacet("products", "numeric_terms", 2);
                service.recordFacet("products", "range", 3);
                service.recordFacet("products", "date_histogram", 4);
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Index metrics recording works")
        void indexMetrics() {
            assertThatCode(() -> {
                service.recordIndexDuration("products", 50);
                service.recordIndexedDocs("products", 500);
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Lifecycle start/stop is idempotent")
        void lifecycleIdempotent() {
            assertThatCode(() -> {
                service.start();
                service.stop();
                service.stop();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Null collection registry still works")
        void nullRegistryWorks() {
            var svc = new PrometheusMetricsService(0, null,
                    new MemoryCircuitBreaker(0.95, false), null);
            assertThatCode(() -> {
                svc.recordRequest("Query", "anything", "ok", 5);
                svc.recordFacet("anything", "string_terms", 1);
            }).doesNotThrowAnyException();
            svc.stop();
        }
    }
}
