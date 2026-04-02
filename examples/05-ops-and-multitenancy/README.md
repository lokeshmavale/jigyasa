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
