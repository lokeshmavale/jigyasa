# Example 05 — Ops & Multi-Tenancy

Demonstrates operational RPCs and multi-tenant data isolation in Jigyasa.

## What This Example Covers

| # | Topic | RPCs Used |
|---|-------|-----------|
| 1 | **Collection lifecycle** | `CreateCollection` → `ListCollections` → `CloseCollection` → `OpenCollection` → `ListCollections` |
| 2 | **Health monitoring** | `Health` (overall status + per-collection metrics) |
| 3 | **Multi-tenancy** | `Index` with `tenant_id`, `Query` / `Count` scoped to a tenant |
| 4 | **Delete by query** | `DeleteByQuery` with tenant-scoped filter |
| 5 | **Force merge** | `ForceMerge` to compact segments |
| 6 | **Prometheus metrics** | Scrape `http://localhost:9090/metrics` for operational metrics |

## Prometheus Metrics

Jigyasa exposes a `/metrics` HTTP endpoint (default port 9090) for Prometheus scraping.

```bash
# Verify metrics endpoint (server must be running)
curl http://localhost:9090/metrics | grep jigyasa_

# Example output after some traffic:
# jigyasa_requests_total{rpc="Query",collection="default",status="ok"} 42.0
# jigyasa_request_duration_seconds_sum{rpc="Query",...} 0.312
# jigyasa_active_requests{rpc="Query"} 0.0
# jigyasa_circuit_breaker_status 0.0
# jigyasa_threadpool_active{pool="handler"} 0.0
```

| Variable | Default | Description |
|---|---|---|
| `METRICS_ENABLED` | `true` | Set to `false` to disable (zero overhead) |
| `METRICS_PORT` | `9090` | HTTP port for `/metrics` |

## Schema

| Field | Type | Role |
|-------|------|------|
| `id` | STRING | key |
| `title` | STRING | searchable (text) |
| `category` | STRING | filterable |

## Tenants

- **acme** — 3 documents (categories: software, hardware)
- **globex** — 2 documents (categories: consulting, software)

Queries demonstrate that each tenant only sees its own documents.

## Running

### Prerequisites

Jigyasa server running on `localhost:50051`.

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:05-ops-and-multitenancy:run

# Python
cd examples/05-ops-and-multitenancy/python
pip install grpcio grpcio-tools
python ops_and_multitenancy.py
```
