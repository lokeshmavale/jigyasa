# Jigyasa Reference

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

## API Reference

gRPC API defined in [`dpSearch.proto`](../src/main/proto/dpSearch.proto).

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

## Full Benchmark Results

All benchmarks run against **Elasticsearch 8.13.0** on the same machine, same data, same shard count. Jigyasa uses REQUEST durability (fsync per operation).

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

### JMH Microbenchmarks (pure Lucene, no gRPC)

| Operation | 10K docs | 100K docs | 1M docs | Unit |
|---|---|---|---|---|
| BM25 text search | 10,648 | 4,040 | 530 | ops/ms |
| Term filter | 42,113 | 41,339 | 37,647 | ops/ms |
| Range filter | 35,088 | 7,407 | 911 | ops/ms |
| Boolean compound | 11,825 | 1,302 | 128 | ops/ms |
| Text + filter | 9,487 | 2,169 | 251 | ops/ms |
| Sort by field | 9,551 | 1,052 | 103 | ops/ms |
| Count + filter | 83,333 | 58,824 | 71,429 | ops/ms |

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
