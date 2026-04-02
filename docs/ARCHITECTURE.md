# Jigyasa Architecture

```
gRPC Server (50051)
 └─ 13 RPCs → Handler layer
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

## Component Overview

### gRPC Layer
- `GrpcServerWrapper` — server lifecycle, JVM shutdown hook, graceful shutdown ordering
- `AnweshanDataPlaneImpl` — routes 13 RPCs to handlers

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

### Storage
- `IndexWriterManager` — Lucene IndexWriter with RAM buffer, merge policy/scheduler
- `IndexSearcherManager` — NRT SearcherManager with 25ms refresh
- `TranslogAppender` / `FileAppender` — write-ahead log for crash recovery
- `TtlSweeper` — background thread expiring documents past their memory tier TTL

### DI & Config
- Guice `ServiceModules` — wires all components
- `ConfigSupplierModule` — environment variable configuration
