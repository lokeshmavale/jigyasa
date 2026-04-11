# Jigyasa Reference

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|---|---|---|
| `GRPC_SERVER_PORT` | `50051` | gRPC server port |
| `INDEX_CACHE_DIR` | `./IndexData/` | Lucene index directory |
| `TRANSLOG_DIRECTORY` | `/TransLog/` | Write-ahead log directory |
| `SERVER_MODE` | `READ_WRITE` | `READ`, `WRITE`, or `READ_WRITE` |
| `TRANSLOG_DURABILITY` | `request` | `request` (fsync per op) or `async` (periodic fsync) |
| `TRANSLOG_FLUSH_INTERVAL_MS` | `200` | Async mode fsync interval (ms) |
| `RAM_BUFFER_SIZE_MB` | `256` | Lucene RAM buffer before flush |
| `USE_COMPOUND_FILE` | `false` | Merge into compound files (slower writes, fewer FDs) |
| `MERGE_MAX_THREADS` | `2` | Concurrent merge threads |
| `MERGE_MAX_MERGE_COUNT` | `4` | Max concurrent merges |
| `INDEX_SCHEMA_PATH` | (not set) | Path to schema JSON file (if not set, uses built-in SampleSchema) |
| `DOCID_OVERLAP_TIMEOUT_MS` | `30000` | Timeout for concurrent updates to same document key |
| `MAX_VECTOR_DIMENSION` | `2048` | Maximum allowed vector dimension |
| `BOOTSTRAP_MEMORY_LOCK` | (not set) | Set to `true` to enable native memory locking (mlockall/VirtualLock). Requires `ulimit -l unlimited` on Linux. |
| `CIRCUIT_BREAKER_HEAP_THRESHOLD` | `0.95` | Heap usage fraction at which user requests are rejected (0.0–1.0). Background tasks unaffected. |
| `CIRCUIT_BREAKER_ENABLED` | `true` | Set to `false` to disable the memory circuit breaker. |

## Data Storage

Jigyasa persists two categories of data to disk. Both paths are configurable via environment variables.

### Directory Layout

```
/data/                          ← root (mount a volume here)
├── index/                      ← INDEX_CACHE_DIR
│   ├── memories/               ← one subdirectory per collection
│   │   ├── segments_N          ← Lucene commit point
│   │   ├── _0.cfs / _0.si     ← segment files (inverted index, vectors, stored fields)
│   │   └── ...
│   └── logs/
│       └── ...
└── translog/                   ← TRANSLOG_DIRECTORY
    ├── memories/               ← one subdirectory per collection
    │   ├── translog.log        ← active write-ahead log
    │   └── ...
    └── ...
```

### What Goes Where

| Data | Location | Purpose |
|---|---|---|
| **Lucene segments** | `INDEX_CACHE_DIR/<collection>/` | Inverted index, HNSW vector graph, stored `_source` fields, doc values for sort/filter. This is your searchable data. |
| **Translog (WAL)** | `TRANSLOG_DIRECTORY/<collection>/` | Write-ahead log. Every index/update/delete is appended here *before* Lucene commit. Used for crash recovery. |
| **Schema** | Lucene commit user data | Schema is stored as metadata in the Lucene commit point — no separate schema file. Survives restarts automatically. |

### Configuring Custom Paths

```bash
# Store index on fast SSD, translog on durable storage
export INDEX_CACHE_DIR=/mnt/ssd/jigyasa/index
export TRANSLOG_DIRECTORY=/mnt/persistent/jigyasa/translog
java -jar Jigyasa-1.0-SNAPSHOT-all.jar
```

Docker — mount a volume:

```bash
docker run -d --name jigyasa \
  -p 50051:50051 \
  -v /host/path/data:/data \
  jigyasa:v0.1
```

Docker Compose (already configured in `docker-compose.yml`):

```yaml
volumes:
  - jigyasa-data:/data    # both index and translog live under /data
```

### Durability Modes

| Mode | Env Value | Behavior | Trade-off |
|---|---|---|---|
| **Request** (default) | `TRANSLOG_DURABILITY=request` | fsync after every operation | Zero data loss, slightly higher write latency |
| **Async** | `TRANSLOG_DURABILITY=async` | fsync every `TRANSLOG_FLUSH_INTERVAL_MS` ms | Higher throughput, up to N ms of data at risk on crash |

### Backup & Restore

Since all data is file-based:

1. **Stop** Jigyasa (or pause writes)
2. **Copy** `INDEX_CACHE_DIR` and `TRANSLOG_DIRECTORY` to your backup location
3. **Restore** by copying them back and starting Jigyasa

For zero-downtime snapshots, use filesystem-level snapshots (ZFS, LVM, EBS snapshots) on the volume.

## Schema Design

```json
{
  "fields": [
    {"name": "id", "type": "STRING", "key": true, "filterable": true},
    {"name": "content", "type": "STRING", "searchable": true},
    {"name": "category", "type": "STRING", "filterable": true, "sortable": true, "facetable": true},
    {"name": "importance", "type": "INT32", "filterable": true, "sortable": true, "facetable": true},
    {"name": "embedding", "type": "VECTOR", "dimensions": 384},
    {"name": "location", "type": "GEO_POINT"}
  ],
  "ttlEnabled": true,
  "hnswConfig": {"maxConn": 16, "beamWidth": 100}
}
```

### Field Types

| Type | Searchable | Filterable | Sortable | Facetable | Notes |
|---|---|---|---|---|---|
| `STRING` | ✅ BM25 | ✅ term/range | ✅ | ✅ terms | Analyzed for search, keyword for filter |
| `INT32` | ❌ | ✅ term/range | ✅ | ✅ terms/range | 32-bit integer |
| `INT64` | ❌ | ✅ term/range | ✅ | ✅ terms/range | 64-bit long |
| `FLOAT` | ❌ | ✅ range | ✅ | ✅ terms/range | 32-bit float |
| `DOUBLE` | ❌ | ✅ range | ✅ | ✅ terms/range | 64-bit double |
| `DATE_TIME_OFFSET` | ❌ | ✅ range | ✅ | ✅ terms/range/date | Epoch-ms; date histogram support |
| `VECTOR` | ✅ KNN | ❌ | ❌ | ❌ | HNSW with optional scalar quantization |
| `GEO_POINT` | ❌ | ✅ distance/bbox | ✅ distance | ❌ | Lat/lon coordinates |

All types except `VECTOR` and `GEO_POINT` support `*_COLLECTION` variants for multi-valued fields.

### Analyzers

Jigyasa supports **42 built-in analyzers** — 4 generic plus 38 language-specific from [Apache Lucene's analysis modules](https://lucene.apache.org/core/10_4_0/analysis/common/index.html). Set per-field via `indexAnalyzer` and `searchAnalyzer` in the schema:

```json
{"name": "title_fr", "type": "STRING", "searchable": true,
 "indexAnalyzer": "lucene.fr", "searchAnalyzer": "lucene.fr"}
```

| Analyzer | Description |
|----------|-------------|
| `standard` (default) | Unicode tokenization + lowercase |
| `simple` | Letter tokenizer + lowercase |
| `keyword` | No tokenization — exact match only |
| `whitespace` | Split on whitespace, no lowercasing |
| `lucene.{lang}` | Language-specific stemming, stop words, normalization |

**Language codes:** `ar` `hy` `eu` `bn` `br` `bg` `ca` `cjk` `cs` `da` `nl` `en` `et` `fi` `fr` `gl` `de` `el` `hi` `hu` `id` `ga` `it` `lv` `lt` `no` `fa` `pl` `pt` `ro` `ru` `sr` `ckb` `es` `sv` `th` `tr` `uk`

For detailed analyzer behavior, see the [Lucene Analysis documentation](https://lucene.apache.org/core/10_4_0/analysis/common/index.html). For a working example, see [Example 07 — Multi-Language Analyzers](../examples/07-multi-language-analyzers/).

### Faceted Navigation

Azure AI Search–style faceted navigation — mark fields as `facetable`, attach `FacetRequest` entries to any query, and get value counts across **all matching documents** in the response.

**2.4x faster than Elasticsearch 8.13** at 1M documents ([benchmarks](#facets--aggregations-1m-docs-e-commerce-dataset)).

#### Schema Setup

```json
{"name": "category", "type": "STRING", "filterable": true, "facetable": true}
{"name": "price", "type": "DOUBLE", "filterable": true, "facetable": true}
{"name": "created", "type": "DATE_TIME_OFFSET", "sortable": true, "facetable": true}
```

#### Facet Types

| Type | When Applied | Example |
|---|---|---|
| **Terms** | STRING, BOOLEAN, or numeric field — no `interval` | Top categories by count |
| **Numeric range** | Numeric field + `interval` | Price buckets: 0–100, 100–200, … |
| **Date histogram** | DATE_TIME_OFFSET field + `date_interval` | Posts per month |
| **Explicit values** | Any facetable field + `values` list | Only ratings 1, 2, 3, 4, 5 |

#### Request Examples (gRPC / Python)

```python
# Terms — top 5 categories
pb.FacetRequest(field="category", count=5)

# Terms — sorted alphabetically
pb.FacetRequest(field="category", count=10, sort=pb.VALUE_ASC)

# Numeric range — price buckets of $100
pb.FacetRequest(field="price", interval=100)

# Date histogram — monthly buckets
pb.FacetRequest(field="created", date_interval=pb.MONTH)

# Explicit values — only these ratings
pb.FacetRequest(field="rating", values=["1", "2", "3", "4", "5"])
```

#### Response Format

Facets are returned in `QueryResponse.facets` — a map of field name to buckets:

```json
{
  "facets": {
    "category": {
      "buckets": [
        {"value": "Electronics", "count": 42},
        {"value": "Books", "count": 31},
        {"value": "Toys", "count": 15}
      ]
    },
    "price": {
      "buckets": [
        {"value": "0-100", "count": 28, "from": "0", "to": "100"},
        {"value": "100-200", "count": 35, "from": "100", "to": "200"}
      ]
    }
  }
}
```

#### Parameters

| Parameter | Default | Description |
|---|---|---|
| `field` | (required) | Must be marked `facetable` in schema |
| `count` | `10` | Max buckets returned. Set `0` for all unique values |
| `sort` | `COUNT_DESC` | `COUNT_DESC`, `COUNT_ASC`, `VALUE_ASC`, `VALUE_DESC` |
| `interval` | — | Numeric range bucket width (mutually exclusive with `values`) |
| `values` | — | Explicit value filter (mutually exclusive with `interval`) |
| `date_interval` | — | `MINUTE`, `HOUR`, `DAY`, `MONTH`, `YEAR` |

#### How It Works

| Concern | Approach |
|---|---|
| **Accuracy** | Counts all matching docs, not just top-K — matches Azure AI Search |
| **MatchAll optimization** | Full DocValues column scan — no BitSet, no FacetsCollector overhead |
| **Filtered queries** | Single-pass via `FacetsCollectorManager` — search + facets in one traversal |
| **Concurrent segments** | Lucene 10 parallelizes collection across segments automatically |
| **Pagination** | `search_after` does not affect facet counts — always reflects full query |
| **Hybrid search** | Facets not supported with hybrid (text + vector) — clear error returned |
| **Ordinal counting** | String facets use `int[]` per segment for cache-friendly counting |
| **Numeric terms** | Primitive `long→long` map — zero autoboxing in hot loop |
| **Date histogram** | Epoch-ms integer arithmetic — no `ZonedDateTime` allocations |

## API Reference

gRPC API defined in [`dpSearch.proto`](../src/main/proto/dpSearch.proto).

| RPC | Description |
|---|---|
| `Index` | Bulk index/update/delete documents |
| `Query` | Search with any combination of query types, filters, sort, facets |
| `Lookup` | Get documents by key (point lookup) |
| `Count` | Count documents matching filters |
| `DeleteByQuery` | Delete documents matching filters |
| `UpdateSchema` | Update collection schema |
| `CreateCollection` | Create a new collection with schema |
| `CloseCollection` | Release collection resources (preserves data) |
| `OpenCollection` | Reopen a closed collection |
| `ListCollections` | List all collections |
| `Health` | Server and per-collection health |
| `ForceMerge` | Compact Lucene segments |

## Full Benchmark Results

All benchmarks run on **Linux containers** with equal resources: **4 CPUs, 12GB memory, 8GB JVM heap, 1 shard, 0 replicas**. Jigyasa uses REQUEST durability (fsync per operation), SIMD vectorization enabled, mlockall active.

### 1M Document Scale (HTTP logs dataset)

| Query Type | Jigyasa p50 | ES 8.13 p50 | Speedup | Jigyasa p99 | ES p99 |
|---|---|---|---|---|---|
| BM25 text search | **2.77ms** | 15.38ms | **5.5x** | 5.55ms | 44.00ms |
| Term filter | **2.30ms** | 16.15ms | **7.0x** | 4.77ms | 33.77ms |
| Range filter | **2.16ms** | 9.47ms | **4.4x** | 4.24ms | 32.92ms |
| Count | **1.84ms** | 8.63ms | **4.7x** | 4.21ms | 33.03ms |
| Sort by field | **9.86ms** | 23.37ms | **2.4x** | 15.13ms | 62.98ms |
| Text + filter | **2.72ms** | 12.71ms | **4.7x** | 6.11ms | 42.90ms |
| **Average** | **3.61ms** | **14.29ms** | **4.0x** | **6.67ms** | **41.60ms** |

### Concurrent Throughput (4 threads, 10s)

| Engine | QPS | Errors |
|---|---|---|
| **Jigyasa** | **1,467** | 0 |
| ES 8.13 | 351 | 0 |
| **Ratio** | **4.2x** | — |

### Bulk Indexing (1M docs, batch=1000)

| Engine | Throughput | Time |
|---|---|---|
| **Jigyasa** | **16,991 docs/s** | 58.9s |
| ES 8.13 | 13,945 docs/s | 71.7s |

### Cold Start (time to first query)

| Engine | Avg |
|---|---|
| **Jigyasa** | **~2s** |
| ES 8.13 | ~16s |

> Reproduce: `python benchmarks/benchmark_1m_sequential.py`

### Facets / Aggregations (1M docs, e-commerce dataset)

| Facet Type | Jigyasa p50 | ES 8.13 p50 | Speedup | Jigyasa p99 | ES p99 |
|---|---|---|---|---|---|
| Terms (1 field, 10 values) | **11.00ms** | 47.89ms | **4.4x** | 20.34ms | 108.79ms |
| Terms (3 fields) | **22.98ms** | 49.78ms | **2.2x** | 34.35ms | 72.46ms |
| Terms + text query | **8.39ms** | 28.85ms | **3.4x** | 34.68ms | 54.91ms |
| Numeric range/histogram | **22.60ms** | 61.70ms | **2.7x** | 43.92ms | 87.90ms |
| Numeric terms | **19.93ms** | 35.60ms | **1.8x** | 37.02ms | 64.31ms |
| Text + filter + 3 facets | **12.34ms** | 13.06ms | **1.1x** | 21.75ms | 41.51ms |
| **Average** | **16.21ms** | **39.48ms** | **2.4x** | **32.01ms** | **71.65ms** |

> Reproduce: `python benchmarks/benchmark_facets.py`

## Production Hardening

Jigyasa is built for production from day one. Three layers of protection prevent a single runaway query from taking down the server.

### Memory Circuit Breaker

Inspired by Elasticsearch's `HierarchyCircuitBreakerService`. Monitors real JVM heap usage and rejects user-facing requests before the JVM hits OOM — while background tasks (commits, TTL sweeps, translog flushes) continue uninterrupted.

| Step | What Happens |
|---|---|
| Every request | Checks `MemoryMXBean.getHeapMemoryUsage()` (~50ns, no allocation) |
| Heap ≥ 95% | Nudges GC once (30s cooldown), re-checks |
| Still ≥ 95% | Returns gRPC `RESOURCE_EXHAUSTED` — client backs off |
| Heap drops below | Auto-recovers, next request succeeds |
| Health API | Always responds — reports `circuit_breaker_tripped` and `trip_count` |

Handles JDK-8207200 race condition in `MemoryMXBean` gracefully (same bug ES handles).

```bash
export CIRCUIT_BREAKER_HEAP_THRESHOLD=0.95   # default: trip at 95% heap
export CIRCUIT_BREAKER_ENABLED=true          # set "false" to disable
```

### Bounded Request Queue

The gRPC handler thread pool uses a bounded queue (capacity: 1000 requests). Under burst load:

- Queue absorbs up to 1000 pending requests (~5 seconds of work at 5ms/request)
- When full, `CallerRunsPolicy` executes on the gRPC I/O thread — this applies natural TCP-level backpressure without dropping any request
- The client experiences higher latency instead of errors

### Concurrent Segment Search

`IndexSearcher` is created with an `Executor` using ES-style thread sizing (`cpus × 1.5 + 1`). Lucene 10.4 automatically parallelizes query execution, scoring, and `CollectorManager` operations (including facets) across segments — no application-level threading code needed.

## Performance Tuning

| Setting | Value | Rationale |
|---|---|---|
| RAM buffer | 256 MB | Large batches stay in memory before flush |
| Compound files | Off | Faster writes, slightly more file descriptors |
| Merge policy | TieredMergePolicy (10 seg/tier) | Balanced read/write |
| Merge scheduler | 2 threads, 4 max merges | Parallel merges without throttling |
| NRT min staleness | 25 ms | Fast refresh for interactive use |
| NRT max staleness | 1 s | Upper bound when no waiters |
| Translog durability | REQUEST (default) | fsync per op, zero data loss |
| Translog async interval | 200 ms | Periodic fsync when `TRANSLOG_DURABILITY=async` |
| QueryParser cache | ThreadLocal per field | Avoid per-query allocation |
| HNSW | maxConn=16, beamWidth=100 | Good recall/speed tradeoff |

### Production JVM Flags

| Flag | Purpose |
|------|---------|
| `--add-modules jdk.incubator.vector` | **Required for SIMD.** Enables Lucene's `PanamaVectorizationProvider` for 512-bit SIMD vector distance computations (dot product, cosine, euclidean). Without this, HNSW falls back to scalar math. |
| `-Dlucene.useScalarFMA=true` | Enable CPU FMA (Fused Multiply-Add) for scalar distance operations |
| `-Dlucene.useVectorFMA=true` | Enable FMA for SIMD distance operations. Both default to `auto`; set `true` to force-enable on known-good hardware. |
| `-XX:+AlwaysPreTouch` | Pre-faults all heap pages at startup — ensures heap is resident in physical RAM |
| `-Xms` / `-Xmx` | Set to N/2 of available memory (leave room for Lucene off-heap mmaps) |

> **Launcher scripts:** Use `jigyasa.sh` (Linux/macOS) or `jigyasa.bat` (Windows) to launch with all production flags pre-configured. These set SIMD, FMA, AlwaysPreTouch, and heap sizing automatically.

### gRPC Server Tuning

| Setting | Default | Description |
|---------|---------|-------------|
| I/O Event Loops | 2 threads | Netty NIO — handles TCP accept/read/write |
| Handler Executor | CPU count | Fixed thread pool — executes Lucene operations |
| Max inbound message | 64 MB | Max size of a single gRPC request (e.g., large batch index) |
| Keep-alive time | 5 minutes | Server-side connection keep-alive interval |
| Keep-alive timeout | 20 seconds | Timeout for keep-alive ping response |
