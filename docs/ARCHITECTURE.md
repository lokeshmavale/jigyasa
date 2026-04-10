# Jigyasa Architecture

```
gRPC Server (50051)
 └─ 12 RPCs → Handler layer
     └─ CollectionRegistry
         └─ CollectionContext (per collection)
             ├─ IndexWriterManager    (write path)
             ├─ IndexSearcherManager  (NRT search)
             ├─ SchemaManager         (typed field mapping)
             ├─ TranslogAppender      (WAL)
             └─ TtlSweeper           (background expiry)
         └─ Query Pipeline
             ├─ BM25 / KNN / Hybrid RRF
             ├─ Filter builders (term/range/geo/bool)
             ├─ Sort + recency decay
             └─ Apache Lucene 10.4
```

## Startup & Bootstrap

On startup, `Main.java` runs `BootstrapChecks` before initializing the server:

1. **Heap size check** — warns if < 512MB (recommend ≥ 512MB for production)
2. **Memory lock** — if `BOOTSTRAP_MEMORY_LOCK=true`, calls `mlockall()` on Linux/macOS or `VirtualLock` on Windows via JNA. Pins JVM heap to physical RAM, preventing swap-induced GC spikes.
3. **AlwaysPreTouch check** — warns if `-XX:+AlwaysPreTouch` JVM flag is missing
4. **SIMD vectorization check** — verifies `jdk.incubator.vector` module is active. Lucene uses this for 512-bit SIMD distance computations (dot product, cosine similarity) during HNSW graph traversal. Without it, vector search falls back to scalar math (4-8x slower at high dimensions). Also logs FMA (Fused Multiply-Add) settings (`lucene.useScalarFMA`, `lucene.useVectorFMA`).

### gRPC Thread Model

The gRPC server uses a split I/O + handler architecture:

- **NIO Event Loop** (2 threads) — handles TCP accept/read/write only
- **Handler Executor** (fixed thread pool, CPU count) — executes Lucene search/index operations
- This separation prevents CPU-bound Lucene operations from blocking I/O threads

## Component Overview

### gRPC Layer
- `GrpcServerWrapper` — server lifecycle, JVM shutdown hook, graceful shutdown ordering
- `AnweshanDataPlaneImpl` — routes 12 RPCs to handlers

### Handlers
- `IndexRequestHandler` — bulk index/update/delete with translog write-ahead
- `QueryRequestHandler` — delegates to query pipeline
- `LookupRequestHandler` — point lookups by document key
- `RecoveryCommitServiceISCH` — translog replay on startup

### Collection Management
- `CollectionRegistry` — create/open/close/list collections
- `CollectionContext` — per-collection bundle of writer, searcher, schema, translog, TTL sweeper

### Query Pipeline
- `BaseQueryBuilder` → `QueryPipeline` → `Executor`
- `RecencyDecayModifier` — time-based score boosting
- `FilterQueryBuilder` — term, range, geo-distance, geo-bbox, boolean composition
- `SortBuilder` — field sort, geo-distance sort, relevance score
- `HybridRrfExecutor` — BM25 + KNN fusion via Reciprocal Rank Fusion
- `FacetExecutor` — terms, numeric range, date histogram faceting via direct DocValues iteration

### Storage
- `IndexWriterManager` — Lucene IndexWriter with RAM buffer, merge policy/scheduler
- `IndexSearcherManager` — NRT SearcherManager with 25ms refresh
- `TranslogAppender` / `FileAppender` — write-ahead log for crash recovery
- `TtlSweeper` — background thread expiring documents past their memory tier TTL

### DI & Config
- Guice `ServiceModules` — wires all components
- `ConfigSupplierModule` — environment variable configuration
