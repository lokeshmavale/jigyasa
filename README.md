<div align="center">

# Jigyasa

**Lightweight Elasticsearch alternative — full-text, vector, and hybrid search in a single JAR**

[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![Lucene 10.4](https://img.shields.io/badge/Lucene-10.4-orange)](https://lucene.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-1.80-green)](https://grpc.io/)
[![License](https://img.shields.io/badge/License-Apache%202.0-yellowgreen)](LICENSE)

</div>

Elasticsearch is powerful but heavy — 587 MB, 20s cold start, GBs of RAM for basic search. Jigyasa runs the **same Lucene engine** without the distributed-systems tax: BM25, HNSW KNN, hybrid RRF, geo filters, and translog in a single embeddable JAR.

**Most search workloads don't need a cluster.** They need fast, correct search on a single node — and they need it without a week of infra setup. If your data fits on one machine, Jigyasa gives you Elasticsearch-grade search at a fraction of the cost, complexity, and latency. No shards to tune. No coordinating nodes. No YAML maze. Just `java -jar` and you're searching.

Deploy it in edge services, embedded search, microservices, CI pipelines, or as the search backbone for LLM agent memory.

## Highlights

- **12 query types** — BM25, phrase, fuzzy, prefix, query-string, KNN, hybrid RRF, term/range/geo/boolean filters
- **12 gRPC RPCs** — index, query, lookup, count, delete-by-query, schema, collections, health, force-merge
- **Schema-driven** — STRING, INT32/64, FLOAT, DOUBLE, VECTOR, GEO_POINT with per-field search/filter/sort
- **NRT search** — 25ms refresh, recency decay scoring, multi-tenant isolation
- **Agent-ready** — built-in memory tiers (WORKING/EPISODIC/SEMANTIC) with TTL sweeper for LLM agent workflows

## vs Elasticsearch 8.13 (1M docs, 4 CPUs, 8GB heap, 1 shard, Linux containers)

| Metric | Jigyasa | ES 8.13 | Speedup |
|--------|---------|---------|---------|
| BM25 text search (p50) | **2.77ms** | 15.38ms | **5.5x** |
| Term filter (p50) | **2.30ms** | 16.15ms | **7.0x** |
| Range filter (p50) | **2.16ms** | 9.47ms | **4.4x** |
| Count (p50) | **1.84ms** | 8.63ms | **4.7x** |
| Text + filter (p50) | **2.72ms** | 12.71ms | **4.7x** |
| Avg query p50 | **3.61ms** | 14.29ms | **4.0x** |
| Concurrent throughput | **1,467 qps** | 351 qps | **4.2x** |
| Bulk indexing | **17K docs/s** | 14K docs/s | **1.2x** |
| Cold start | **~2s** | ~16s | **8x** |
| Artifact size | **Single JAR** | 587 MB Docker | — |

> Reproduce: `python benchmarks/benchmark_1m_sequential.py` · Full tables → [docs/REFERENCE.md](docs/REFERENCE.md#full-benchmark-results)

## Quick Start

```bash
# Start the server (with SIMD + memory optimization)
./gradlew run                                          # → localhost:50051

# Or build a fat JAR and use the launcher script
./gradlew shadowJar
./jigyasa.sh                                           # Linux/macOS (all prod flags)
jigyasa.bat                                            # Windows (all prod flags)

# Or launch manually with full optimization
java --add-modules jdk.incubator.vector \
     -Dlucene.useScalarFMA=true -Dlucene.useVectorFMA=true \
     -XX:+AlwaysPreTouch -Xms1g -Xmx1g \
     -jar build/libs/Jigyasa-1.0-SNAPSHOT-all.jar

# Docker
docker compose up -d
```

## Usage

```python
import grpc
from generated import dpSearch_pb2 as pb, dpSearch_pb2_grpc as pb_grpc

stub = pb_grpc.JigyasaDataPlaneServiceStub(grpc.insecure_channel("localhost:50051"))

# Create collection + index
stub.CreateCollection(pb.CreateCollectionRequest(
    collection="memories",
    indexSchema='{"fields": [{"name": "id", "type": "STRING", "key": true}, {"name": "content", "type": "STRING", "searchable": true}]}'
))
stub.Index(pb.IndexRequest(collection="memories",
    item=[pb.IndexItem(document='{"id": "m1", "content": "User prefers dark mode"}')]))

# Search
resp = stub.Query(pb.QueryRequest(collection="memories", text_query="dark mode", include_source=True, top_k=10))
```

## Build, Test & Run Examples

```bash
./gradlew build                                        # compile + test
./gradlew test                                         # 214 tests
./gradlew spotbugsMain                                 # Static analysis (0 bugs)
python smoke_test.py                                   # e2e against running server

# Run any Java example (server must be running)
./gradlew :examples:00-quickstart:run
./gradlew :examples:07-multi-language-analyzers:run
# See examples/README.md for the full list
```

## Docs

| | |
|---|---|
| [Examples](examples/) | 8 hands-on examples from quickstart to multi-language analyzers (Java + Python) |
| [Architecture](docs/ARCHITECTURE.md) | Component overview, query pipeline, storage layer |
| [Reference](docs/REFERENCE.md) | Configuration, schema design, field types, full benchmarks, tuning |
| [Agent Integration Guide](docs/AGENT_INTEGRATION_GUIDE.md) | How to wire Jigyasa into LLM agent frameworks |
| [Proto definition](src/main/proto/dpSearch.proto) | Full gRPC API spec |

## Roadmap

- [ ] Engram Python SDK — drop-in `EngramCheckpointer` for LangGraph
- [ ] Learning-to-Rank — agent task outcomes feed retrieval scoring
- [ ] Aggregations, More-Like-This, Auth (API key + mTLS)
- [ ] Prometheus metrics, Helm chart, multi-node replication

## License

Apache 2.0

<div align="center">
Built by <a href="https://github.com/lokeshmavale">Lokesh Mawale</a>
</div>