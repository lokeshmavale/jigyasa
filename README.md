<div align="center">

# Jigyasa

**A high-performance, embeddable search engine built on Apache Lucene 10.4**

*Drop-in replacement for SQLite + Pinecone + Redis — one engine for full-text, vector, and hybrid search*

[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![Lucene 10.4](https://img.shields.io/badge/Lucene-10.4-orange)](https://lucene.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-1.80-green)](https://grpc.io/)
[![License](https://img.shields.io/badge/License-Apache%202.0-yellowgreen)](LICENSE)

</div>

---

## Why Jigyasa?

LLM agents today cobble together **3–4 systems** for memory: SQLite for state, a vector DB for embeddings, maybe Elasticsearch for search, plus custom glue code for TTL and ranking. Each adds latency, complexity, and failure modes.

**Jigyasa replaces that entire stack with a single embedded engine:**

| Capability | Typical Agent Stack | **Jigyasa** |
|---|---|---|
| State checkpointing | SQLite (opaque BLOBs) | ✅ Searchable documents |
| Full-text search | ❌ Add Elasticsearch | ✅ BM25 built-in |
| Vector search | ❌ Add Chroma/Weaviate | ✅ HNSW built-in |
| Hybrid text + vector | ❌ Manual glue code | ✅ RRF fusion |
| Structured filters | ❌ Scattered across DBs | ✅ Term, range, geo, boolean |
| Memory tiers + TTL | ❌ Custom expiry logic | ✅ Native (working/episodic/semantic) |
| Relevance ranking | ❌ No scoring | ✅ BM25 + recency decay |
| Embeddable / on-premise | Varies | ✅ Single JAR, no cloud dependency |

## Features

### 12 Query Types
- **Text search** — BM25 scoring with configurable analyzers
- **Phrase queries** — exact phrase matching with slop tolerance
- **Fuzzy queries** — typo-tolerant search (Levenshtein distance)
- **Prefix queries** — autocomplete-style prefix matching
- **Query string** — full Lucene syntax (`status:[400 TO 499] AND (ext:php OR ext:html)`)
- **Vector KNN** — HNSW approximate nearest neighbor search
- **Hybrid RRF** — BM25 + KNN fusion via Reciprocal Rank Fusion
- **Match-all** — retrieve all documents (with optional filters)
- **Term filters** — exact value matching on keyword fields
- **Range filters** — numeric/string range queries
- **Geo queries** — distance radius and bounding box filters
- **Boolean filters** — compound must/should/must_not composition

### Engine Capabilities
- **Multi-collection** — create, close, open, list independent collections
- **Schema-driven** — typed fields (STRING, INT32, INT64, DOUBLE, FLOAT, GEO_POINT, VECTOR)
- **Field projection** — return only requested fields from `_source`
- **Sort** — by any sortable field, geo-distance, or relevance score
- **Pagination** — offset-based and cursor-based (`search_after`) deep pagination
- **Count API** — optimized document counting with filter support
- **Min score threshold** — filter low-relevance results
- **NRT search** — near-real-time with 25ms min staleness
- **Memory tiers** — WORKING (1h TTL), EPISODIC (24h), SEMANTIC (permanent)
- **TTL sweeper** — automatic background expiry of aged documents
- **Tenant isolation** — multi-tenant scoping on all operations
- **Recency decay** — time-based score boosting for recent documents
- **Schema persistence** — schemas survive restarts via Lucene commit data
- **Force merge** — on-demand segment compaction
- **Translog** — write-ahead log for crash recovery

## Quick Start

### Docker (recommended)

```bash
docker run -d --name jigyasa \
  -p 50051:50051 \
  -v jigyasa-data:/data \
  jigyasa:v0.1
```

### Docker Compose

```bash
git clone https://github.com/lokeshmavale/jigyasa.git
cd jigyasa
./gradlew shadowJar
docker compose up -d
```

### Fat JAR

```bash
./gradlew shadowJar
java -jar build/libs/Jigyasa-1.0-SNAPSHOT-all.jar
```

Server starts on **port 50051** (gRPC).

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|---|---|---|
| `GRPC_SERVER_PORT` | `50051` | gRPC server port |
| `INDEX_CACHE_DIR` | `/data/index` | Lucene index directory |
| `TRANSLOG_DIRECTORY` | `/data/translog` | Write-ahead log directory |
| `SERVER_MODE` | `READ_WRITE` | `READ`, `WRITE`, or `READ_WRITE` |
| `TRANSLOG_DURABILITY` | `request` | `request` (fsync per op) or `async` (periodic fsync) |
| `TRANSLOG_FLUSH_INTERVAL_MS` | `200` | Async mode fsync interval (ms) |
| `RAM_BUFFER_SIZE_MB` | `256` | Lucene RAM buffer before flush |
| `USE_COMPOUND_FILE` | `false` | Merge into compound files (slower writes, fewer FDs) |
| `MERGE_MAX_THREADS` | `2` | Concurrent merge threads |
| `MERGE_MAX_MERGE_COUNT` | `4` | Max concurrent merges |

## API Reference

Jigyasa exposes a gRPC API defined in [`dpSearch.proto`](src/main/proto/dpSearch.proto).

### 13 RPCs

| RPC | Description |
|---|---|
| `Index` | Bulk index/update/delete documents |
| `Query` | Search with any combination of query types, filters, sort |
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

### Example: Index Documents

```python
import grpc
from generated import dpSearch_pb2 as pb, dpSearch_pb2_grpc as pb_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

# Create collection
stub.CreateCollection(pb.CreateCollectionRequest(
    collection="memories",
    indexSchema='{"fields": [{"name": "id", "type": "STRING", "key": true}, {"name": "content", "type": "STRING", "searchable": true}]}'
))

# Index
stub.Index(pb.IndexRequest(
    collection="memories",
    item=[pb.IndexItem(document='{"id": "m1", "content": "User prefers dark mode"}')]
))
```

### Example: Search

```python
# Full-text search
resp = stub.Query(pb.QueryRequest(
    collection="memories",
    text_query="dark mode preferences",
    include_source=True,
    top_k=10
))

# With filters
resp = stub.Query(pb.QueryRequest(
    collection="memories",
    text_query="user preferences",
    filters=[pb.FilterClause(
        field="category",
        term_filter=pb.TermFilter(value="settings")
    )],
    include_source=True,
    top_k=5
))

# Lucene query string syntax
resp = stub.Query(pb.QueryRequest(
    collection="memories",
    query_string='content:"dark mode" AND category:settings',
    query_string_default_field="content",
    include_source=True
))

# Count
count = stub.Count(pb.CountRequest(collection="memories"))
```

## Schema Design

```json
{
  "fields": [
    {"name": "id", "type": "STRING", "key": true, "filterable": true},
    {"name": "content", "type": "STRING", "searchable": true},
    {"name": "category", "type": "STRING", "filterable": true, "sortable": true},
    {"name": "importance", "type": "INT32", "filterable": true, "sortable": true},
    {"name": "embedding", "type": "VECTOR", "dimensions": 384},
    {"name": "location", "type": "GEO_POINT"}
  ],
  "ttlEnabled": true,
  "hnswConfig": {"maxConn": 16, "beamWidth": 100}
}
```

### Field Types

| Type | Searchable | Filterable | Sortable | Notes |
|---|---|---|---|---|
| `STRING` | ✅ BM25 | ✅ term/range | ✅ | Analyzed for search, keyword for filter |
| `INT32` | ❌ | ✅ term/range | ✅ | 32-bit integer |
| `INT64` | ❌ | ✅ term/range | ✅ | 64-bit long |
| `FLOAT` | ❌ | ✅ range | ✅ | 32-bit float |
| `DOUBLE` | ❌ | ✅ range | ✅ | 64-bit double |
| `VECTOR` | ✅ KNN | ❌ | ❌ | HNSW with optional scalar quantization |
| `GEO_POINT` | ❌ | ✅ distance/bbox | ✅ distance | Lat/lon coordinates |

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  gRPC Server (50051)             │
├─────────────────────────────────────────────────┤
│  AnweshanDataPlaneImpl → 13 RPCs                │
├──────────┬──────────┬───────────┬───────────────┤
│  Index   │  Query   │  Lookup   │  Collection   │
│  Handler │  Handler │  Handler  │  Manager      │
├──────────┴──────────┴───────────┴───────────────┤
│              CollectionRegistry                  │
│  ┌─────────────────────────────────────────┐    │
│  │  CollectionContext (per collection)      │    │
│  │  ├─ IndexWriterManager (write path)     │    │
│  │  ├─ IndexSearcherManager (NRT search)   │    │
│  │  ├─ SchemaManager (typed field mapping) │    │
│  │  ├─ TranslogAppender (WAL)              │    │
│  │  └─ TtlSweeper (background expiry)     │    │
│  └─────────────────────────────────────────┘    │
├─────────────────────────────────────────────────┤
│  Query Pipeline                                  │
│  BaseQueryBuilder → QueryPipeline → Executor    │
│  ├─ RecencyDecayModifier                        │
│  ├─ FilterQueryBuilder (term/range/geo/bool)    │
│  ├─ SortBuilder (field/geo-distance)            │
│  └─ HybridRrfExecutor (BM25 + KNN fusion)      │
├─────────────────────────────────────────────────┤
│              Apache Lucene 10.4                  │
│  Segments │ HNSW │ BM25 │ LatLonPoint │ NRT    │
└─────────────────────────────────────────────────┘
```

## Performance Benchmarks

All benchmarks run head-to-head against **Elasticsearch 8.13.0** on the same machine, same data, same shard count. Jigyasa uses REQUEST durability (fsync per operation) — the safe default.

### 1M Document Scale

| Query Type | Jigyasa p50 | ES 8.13 p50 | Speedup |
|---|---|---|---|
| BM25 text search | **4.32ms** | 13.33ms | **3.1x** |
| Term filter | **1.07ms** | 15.06ms | **14.1x** |
| Range filter | **1.37ms** | 8.30ms | **6.1x** |
| Boolean compound | **1.34ms** | 14.78ms | **11.0x** |
| Text + filter | **5.20ms** | 13.02ms | **2.5x** |
| Sort by field | **10.63ms** | 26.08ms | **2.5x** |
| Count | **0.64ms** | 7.16ms | **11.2x** |
| Count + filter | **0.87ms** | 6.76ms | **7.8x** |

**Average: 3.18ms vs 13.06ms — Jigyasa 4.1x faster. Score: Jigyasa 8 – ES 0.**

### Bulk Indexing (1M docs, batch=1000)

| Engine | Throughput | Time |
|---|---|---|
| **Jigyasa** | **21,507 docs/s** | 46.5s |
| ES 8.13 | 14,206 docs/s | 70.4s |

### Vector KNN Search (50K docs, 128-dim HNSW)

| Query Type | Jigyasa p50 | ES 8.13 p50 | Speedup |
|---|---|---|---|
| KNN top-10 | **2.07ms** | 11.35ms | **5.5x** |
| KNN top-50 | **6.44ms** | 16.22ms | **2.5x** |
| KNN + filter | **2.86ms** | 16.03ms | **5.6x** |
| Hybrid BM25+KNN | **2.23ms** | 12.49ms | **5.6x** |

### Concurrent Throughput & Operational

| Metric | Jigyasa | ES 8.13 | Advantage |
|---|---|---|---|
| Peak QPS (4 threads) | **3,154** | 786 (8 threads) | **4.0x** |
| Cold start | **1.84s** | 19.51s | **11x** |
| Artifact size | **29 MB** JAR | 587 MB Docker | **21x smaller** |
| Memory baseline | 512 MB | 1.4 GB | **3x less** |

### JMH Microbenchmarks (pure Lucene, no gRPC overhead)

Raw Lucene performance measured with JMH (in-memory `ByteBuffersDirectory`, JDK 21, Vector API enabled):

| Operation | 10K docs | 100K docs | 1M docs | Unit |
|---|---|---|---|---|
| BM25 text search | 10,648 | 4,040 | 530 | ops/ms |
| Term filter | 42,113 | 41,339 | 37,647 | ops/ms |
| Range filter | 35,088 | 7,407 | 911 | ops/ms |
| Boolean compound | 11,825 | 1,302 | 128 | ops/ms |
| Text + filter | 9,487 | 2,169 | 251 | ops/ms |
| Sort by field | 9,551 | 1,052 | 103 | ops/ms |
| Count + filter | 83,333 | 58,824 | 71,429 | ops/ms |

*Term filter at 37M ops/sec on 1M docs — Lucene is the speed floor. gRPC adds ~1ms overhead, which is the gap between JMH and end-to-end numbers.*

*Why faster than ES? Embedded Lucene (no HTTP/JSON parsing, no shard routing, gRPC binary protocol). Same Lucene engine underneath — Jigyasa just removes the distributed systems tax.*

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

## Building from Source

```bash
# Prerequisites: Java 21+
./gradlew build          # Compile + test
./gradlew shadowJar      # Build fat JAR (28 MB)
./gradlew test           # Run 191 tests
```

## Testing

```bash
# Unit + integration tests (191 tests)
./gradlew test

# Smoke test against running server
pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos
python smoke_test.py
```

## Project Structure

```
jigyasa/
├── src/main/proto/dpSearch.proto     # gRPC API definition
├── src/main/java/com/jigyasa/dp/search/
│   ├── entrypoint/                   # Main, GrpcServerWrapper, IndexManager
│   ├── services/                     # Guice modules, gRPC impl
│   ├── handlers/                     # Index/Query/Lookup handlers, NRT, WAL
│   ├── query/                        # Query pipeline, builders, executors
│   ├── collections/                  # Multi-collection lifecycle
│   ├── models/                       # Schema, field mappers, configs
│   ├── utils/                        # Filters, system fields, schema utils
│   └── configs/                      # Environment variables
├── src/test/java/                    # 191 tests
├── docs/AGENT_INTEGRATION_GUIDE.md   # 30KB agent-facing documentation
├── Dockerfile                        # Production container
├── docker-compose.yml                # One-command deployment
├── smoke_test.py                     # End-to-end Python test
└── build.gradle                      # Gradle + shadow + protobuf
```

## Roadmap

- [ ] **Engram Python SDK** — `EngramCheckpointer` drop-in for LangGraph `SqliteSaver`
- [ ] **Learning-to-Rank** — agent task outcomes feed back into retrieval scoring
- [ ] **More-Like-This** — find similar documents by example
- [ ] **Aggregations** — faceted counts and statistics
- [ ] **Auth** — API key and mTLS authentication
- [ ] **Observability** — Prometheus metrics endpoint
- [ ] **Helm chart** — Kubernetes deployment
- [ ] **Multi-node replication** — horizontal scalability

## License

Apache License 2.0

---

<div align="center">
Built with ❤️ by <a href="https://github.com/lokeshmavale">Lokesh Mawale</a>
</div>