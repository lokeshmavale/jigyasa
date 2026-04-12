package com.jigyasa.dp.search.metrics;

import com.jigyasa.dp.search.services.MemoryCircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test: starts the metrics HTTP server on an ephemeral port,
 * sends a real HTTP request to /metrics, and verifies the response.
 */
class MetricsEndpointSmokeTest {

    private PrometheusMetricsService service;
    private static final int TEST_PORT = 19090;

    @BeforeEach
    void setUp() {
        MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(0.95, false);
        service = new PrometheusMetricsService(TEST_PORT, null, breaker, null);
        service.start();
    }

    @AfterEach
    void tearDown() {
        service.stop();
    }

    @Test
    @DisplayName("/metrics endpoint responds with 200 and Prometheus format")
    void metricsEndpointResponds() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + TEST_PORT + "/metrics").toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        assertThat(conn.getResponseCode()).isEqualTo(200);
        String body = new String(conn.getInputStream().readAllBytes());
        conn.disconnect();

        assertThat(body).isNotEmpty();
    }

    @Test
    @DisplayName("/metrics contains JVM metrics")
    void containsJvmMetrics() throws IOException {
        String body = fetchMetrics();
        assertThat(body).contains("jvm_memory");
        assertThat(body).contains("jvm_threads");
    }

    @Test
    @DisplayName("/metrics contains circuit breaker gauge")
    void containsCircuitBreakerMetrics() throws IOException {
        String body = fetchMetrics();
        assertThat(body).contains("jigyasa_circuit_breaker_status");
        assertThat(body).contains("jigyasa_circuit_breaker_trips");
    }

    @Test
    @DisplayName("Recording a request makes it appear in /metrics")
    void recordedRequestAppearsInOutput() throws IOException {
        service.recordRequest("Query", "default", "ok", 5);

        String body = fetchMetrics();
        assertThat(body).contains("jigyasa_requests_total");
        assertThat(body).contains("jigyasa_request_duration_seconds");
    }

    @Test
    @DisplayName("Active requests gauge appears after inc/dec")
    void activeRequestsAppear() throws IOException {
        service.incrementActiveRequests("Query");
        service.decrementActiveRequests("Query");

        String body = fetchMetrics();
        assertThat(body).contains("jigyasa_active_requests");
    }

    private String fetchMetrics() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + TEST_PORT + "/metrics").toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        String body = new String(conn.getInputStream().readAllBytes());
        conn.disconnect();
        return body;
    }
}
