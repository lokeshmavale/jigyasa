# Jigyasa Learning Guide

> **From zero to production** — a structured path to mastering Jigyasa, the lightweight Elasticsearch alternative.

---

## Who This Guide Is For

| You are... | Start at... |
|-----------|------------|
| Evaluating Jigyasa for a project | [Module 1](#module-1-what-is-jigyasa) → [Module 2](#module-2-quickstart-5-min) |
| A developer integrating Jigyasa | [Module 2](#module-2-quickstart-5-min) → [Module 5](#module-5-query-mastery) |
| Building an AI agent with memory | [Module 8](#module-8-ai-agent-integration) |
| Operating Jigyasa in production | [Module 7](#module-7-operations--multi-tenancy) → [Module 9](#module-9-production-deployment) |

---

## Module 1: What Is Jigyasa?

Jigyasa is a **search engine in a JAR**. It replaces Elasticsearch for use cases where you need full-text, vector, and hybrid search without the operational overhead of a distributed cluster.

### The 30-Second Pitch

```
Elasticsearch (cluster)          Jigyasa (single JAR)
─────────────────────           ─────────────────────
JVM heap tuning                  java -jar jigyasa.jar
Cluster coordination             ← not needed
Shard allocation                 ← not needed
Index lifecycle management       ← automatic
REST + JSON over HTTP            gRPC (binary, typed)
~20s cold start                  ~2s cold start
```

### When to Use Jigyasa

✅ **Good fit:**
- Embedded search in microservices
- AI agent memory (replacing SQLite)
- Single-node deployments (up to ~10M docs)
- Low-latency search (sub-5ms p50)
- Edge/IoT devices with limited resources

❌ **Not a fit:**
- Multi-node distributed search (use Elasticsearch)
- Petabyte-scale analytics (use Elasticsearch/OpenSearch)
- SQL queries (use PostgreSQL + pg_trgm)

### Architecture at a Glance

```
Client (gRPC) ──→ GrpcServer:50051
                   └─ 13 RPCs → Handler Layer
                      └─ CollectionRegistry
                         └─ CollectionContext (per collection)
                            ├─ IndexWriterManager  (write path)
                            ├─ IndexSearcherManager (NRT read path)
                            ├─ SchemaManager        (typed fields)
                            ├─ TranslogAppender     (WAL durability)
                            └─ TtlSweeper           (background expiry)
                         └─ Query Pipeline
                            ├─ BM25 / KNN / Hybrid RRF
                            ├─ Filters (term, range, geo, boolean)
                            ├─ Sort + pagination
                            └─ Apache Lucene 10.4
```

**Key design decisions:**
- **Index-then-translog WAL**: Lucene write first, then persist to translog. If crash between the two, uncommitted buffer is lost (same as ES default durability).
- **NRT (Near-Real-Time)**: Documents are searchable within 25ms of indexing (configurable).
- **Schema-driven**: Every field has a type, and search/filter/sort capabilities are declared upfront.

📖 Deep dive: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## Module 2: Quickstart (5 min)

### Prerequisites

- Java 21+
- Gradle 8+ (bundled via `gradlew`)
- Python 3.8+ (for Python examples and benchmarks)

### Start the Server

```bash
# Clone and run
git clone <repo-url> && cd jigyasa
./gradlew run                    # Starts gRPC server on localhost:50051
```

### Run Your First Example

```bash
# Java
./gradlew :examples:00-quickstart:run

# Python
cd examples/00-quickstart/python
pip install grpcio grpcio-tools
python quickstart.py
```

**What happens:**
1. Health check → server is `SERVING`
2. Create collection `quickstart-java`
3. Index 5 documents
4. Search for `"search engine library"` → 2 hits
5. Count total documents → 5

### What You Just Did

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  Health()    │───→│  Server OK   │    │             │
│  Create()    │───→│  Collection  │    │             │
│  Index(5)    │───→│  Lucene+WAL  │───→│  Searchable │
│  Query()     │───→│  BM25 score  │───→│  2 hits     │
│  Count()     │───→│  Total docs  │───→│  5          │
└─────────────┘    └──────────────┘    └─────────────┘
```

📂 Source: [`examples/00-quickstart/`](examples/00-quickstart/)

---

## Module 3: Schema Design (15 min)

### Core Concept

Every collection needs a **schema** — a JSON definition of fields, their types, and capabilities.

### Field Types

| Type | Searchable | Filterable | Sortable | Use Case |
|------|:----------:|:----------:|:--------:|----------|
| `STRING` | ✅ BM25 | ✅ term/range | ✅ | Text content, names, categories |
| `STRING_COLLECTION` | ✅ | ✅ | — | Tags, keywords |
| `INT32` | — | ✅ term/range | ✅ | Counts, flags |
| `INT64` | — | ✅ term/range | ✅ | Timestamps, IDs |
| `DOUBLE` | — | ✅ range | ✅ | Prices, scores, ratings |
| `BOOLEAN` | — | ✅ term | ✅ | Flags |
| `GEO_POINT` | — | ✅ distance/bbox | ✅ distance | Locations |
| `VECTOR` | ✅ KNN | — | — | Embeddings |
| `DATE_TIME_OFFSET` | — | ✅ range | ✅ | Dates (stored as INT64) |

> ⚠️ **Key field rule**: Always declare exactly one field with `"key": true, "filterable": true`. Never add `"searchable": true` to the key field.

### Example Schema

```json
{
  "fields": [
    { "name": "id",          "type": "STRING",  "key": true,  "filterable": true },
    { "name": "title",       "type": "STRING",  "searchable": true, "filterable": true, "sortable": true },
    { "name": "description", "type": "STRING",  "searchable": true },
    { "name": "category",    "type": "STRING",  "filterable": true },
    { "name": "price",       "type": "DOUBLE",  "filterable": true, "sortable": true },
    { "name": "tags",        "type": "STRING_COLLECTION", "filterable": true },
    { "name": "location",    "type": "GEO_POINT", "filterable": true, "sortable": true },
    { "name": "embedding",   "type": "VECTOR",  "searchable": true,
      "vectorDimension": 128, "vectorSimilarity": "COSINE" }
  ],
  "hnswConfig": { "maxConn": 16, "beamWidth": 100 },
  "ttlEnabled": false
}
```

### Analyzers

Jigyasa supports **42 built-in analyzers** powered by Apache Lucene. You can set `indexAnalyzer` and `searchAnalyzer` per field.

**Generic:** `standard` (default), `simple`, `keyword`, `whitespace`

**Language-specific (38):** `lucene.en`, `lucene.fr`, `lucene.de`, `lucene.es`, `lucene.it`, `lucene.pt`, `lucene.nl`, `lucene.ru`, `lucene.ar`, `lucene.hi`, `lucene.cjk`, `lucene.pl`, `lucene.tr`, `lucene.th`, and 24 more.

```json
{ "name": "title_en", "type": "STRING", "searchable": true,
  "indexAnalyzer": "lucene.en", "searchAnalyzer": "lucene.en" }
```

> 📖 Full analyzer list and behavior: [Apache Lucene Analysis documentation](https://lucene.apache.org/core/10_0_0/analysis/common/index.html)

📂 Source: [`examples/01-schema-and-indexing/`](examples/01-schema-and-indexing/)  
📂 Source: [`examples/07-multi-language-analyzers/`](examples/07-multi-language-analyzers/)

---

## Module 4: Indexing Data (15 min)

### Index Operations

Jigyasa supports two actions per item in a batch:
- **`UPDATE`** — Upsert (insert or replace by key)
- **`DELETE`** — Remove by key

```java
// Index a batch of documents
IndexRequest.newBuilder()
    .setCollection("products")
    .addItem(IndexItem.newBuilder()
        .setAction(IndexAction.UPDATE)
        .setDocument("{\"id\":\"p1\",\"title\":\"Laptop\",\"price\":999.99}"))
    .addItem(IndexItem.newBuilder()
        .setAction(IndexAction.DELETE)
        .setDocument("{\"id\":\"p2\"}"))
    .setRefresh(RefreshPolicy.WAIT_FOR)   // Block until searchable
    .build();
```

### Refresh Policies

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `WAIT_FOR` | Block until NRT refresh makes docs searchable | Default — correctness |
| `NONE` | Fire-and-forget, docs visible within ~25ms | Bulk ingestion throughput |
| `IMMEDIATE` | Force flush immediately | Testing, single-doc inserts |

### Memory Tiers (for AI agents)

```java
IndexItem.newBuilder()
    .setAction(IndexAction.UPDATE)
    .setDocument(json)
    .setMemoryTier(MemoryTier.EPISODIC)  // Auto-expires via TTL
    .setTtlSeconds(86400)                // 24 hours
```

| Tier | TTL | Use Case |
|------|-----|----------|
| `SEMANTIC` | Permanent | Knowledge base, facts |
| `EPISODIC` | Configurable | Conversation history |
| `WORKING` | Short (minutes) | Scratch pad, temp context |

📂 Source: [`examples/01-schema-and-indexing/`](examples/01-schema-and-indexing/)  
📂 Source: [`examples/04-configuration/`](examples/04-configuration/)

---

## Module 5: Query Mastery (30 min)

This is the core module. Jigyasa supports **12 query types**.

### Text Queries (scored by BM25)

```java
// 1. Text search — BM25 scoring across searchable fields
QueryRequest.newBuilder()
    .setTextQuery("wireless headphones")
    .setTextField("title")           // Optional: empty = all searchable fields
    .setTopK(10)

// 2. Phrase query — exact word sequence with slop tolerance
    .setPhraseQuery("cast iron skillet")
    .setPhraseSlop(2)                // Allow 2 words between terms

// 3. Fuzzy query — typo tolerance
    .setFuzzyQuery("headphoness")    // Typo!
    .setMaxEdits(2)                  // Levenshtein distance

// 4. Prefix query — autocomplete
    .setPrefixQuery("wire")

// 5. Query string — full Lucene syntax
    .setQueryString("title:laptop AND price:[500 TO 1000]")
```

### Filters (not scored, just match/no-match)

```java
// 6. Term filter — exact match
FilterClause.newBuilder()
    .setField("category")
    .setTermFilter(TermFilter.newBuilder().setValue("electronics"))

// 7. Range filter — numeric/string ranges
    .setRangeFilter(RangeFilter.newBuilder()
        .setGte("20.0").setLte("100.0"))

// 8. Geo distance — within radius of a point
    .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
        .setLat(40.7128).setLon(-74.0060)
        .setDistanceKm(50.0))

// 9. Boolean compound — AND/OR/NOT
    .setCompoundFilter(CompoundFilter.newBuilder()
        .setOperator(CompoundFilter.Operator.AND)
        .addClauses(termFilter("category", "electronics"))
        .addNotClauses(termFilter("brand", "FlexZone")))

// 10. Exists filter — field presence
    .setExistsFilter(ExistsFilter.newBuilder().setMustExist(true))
```

### Vector & Hybrid Search

```java
// 11. KNN vector search
QueryRequest.newBuilder()
    .setVectorQuery(VectorQuery.newBuilder()
        .setField("embedding")
        .addAllVector(queryEmbedding)   // float[]
        .setK(10))

// 12. Hybrid search — text + vector with RRF fusion
QueryRequest.newBuilder()
    .setTextQuery("machine learning")
    .setVectorQuery(VectorQuery.newBuilder()
        .setField("embedding")
        .addAllVector(queryEmbedding)
        .setK(10))
    .setTextWeight(0.7)                 // 70% text, 30% vector
```

### Sorting & Pagination

```java
// Sort by price descending
QueryRequest.newBuilder()
    .addSort(SortClause.newBuilder()
        .setField("price").setDescending(true))
    .setOffset(20).setTopK(10)          // Page 3

// Cursor-based pagination (for deep paging)
    .setSearchAfter(previousResponse.getNextSearchAfter())
```

📂 Source: [`examples/02-query-cookbook/`](examples/02-query-cookbook/)  
📂 Source: [`examples/03-vector-and-hybrid/`](examples/03-vector-and-hybrid/)

---

## Module 6: Building a Real App (1 hr)

The e-commerce example ties everything together into a working product search application.

```bash
./gradlew :examples:06-e-commerce-app:run
```

**What it builds:**
1. Creates a `products` collection with a rich schema
2. Indexes 20 products from `data/products.jsonl`
3. Demonstrates 6 real-world search scenarios:

| Scenario | Queries Used |
|----------|-------------|
| Product text search | BM25 + field projection |
| Browse by category | Term filter + sort |
| Price range filter | Range filter + sort |
| Store locator | Geo distance filter |
| Advanced search | Text + filter + sort combined |
| Product count | Count API with filters |

📂 Source: [`examples/06-e-commerce-app/`](examples/06-e-commerce-app/)

---

## Module 7: Operations & Multi-tenancy (15 min)

### Collection Lifecycle

```java
// Create
stub.createCollection(CreateCollectionRequest.newBuilder()
    .setCollection("logs")
    .setIndexSchema(schemaJson).build());

// List
stub.listCollections(ListCollectionsRequest.getDefaultInstance());

// Close (release resources, keep data)
stub.closeCollection(CloseCollectionRequest.newBuilder()
    .setCollection("logs").build());

// Reopen
stub.openCollection(OpenCollectionRequest.newBuilder()
    .setCollection("logs")
    .setIndexSchema(schemaJson).build());
```

### Multi-tenant Isolation

Tenant isolation is **built-in** — no application-level filtering needed.

```java
// Index with tenant
IndexItem.newBuilder()
    .setTenantId("acme")
    .setDocument(json)

// Query scoped to tenant
QueryRequest.newBuilder()
    .setTenantId("acme")         // Only sees acme's docs
    .setTextQuery("report")

// Delete tenant's data
DeleteByQueryRequest.newBuilder()
    .setTenantId("acme")
    .addFilters(filter)
```

### Health Monitoring

```java
HealthResponse health = stub.health(HealthRequest.getDefaultInstance());
// health.getStatus()           → SERVING / NOT_SERVING
// health.getCollections(0)     → name, doc_count, segment_count, writer_open
```

### Maintenance

```java
// Compact segments (reduces disk, improves query speed)
stub.forceMerge(ForceMergeRequest.newBuilder()
    .setCollection("logs")
    .setMaxSegments(1).build());
```

📂 Source: [`examples/05-ops-and-multitenancy/`](examples/05-ops-and-multitenancy/)

---

## Module 8: AI Agent Integration

Jigyasa was designed as a **drop-in replacement for SQLite** in AI agent frameworks, with proper search capabilities.

### Why Replace SQLite?

| Capability | SQLite | Jigyasa |
|-----------|--------|---------|
| Text search | `LIKE '%word%'` (slow, no ranking) | BM25 + phrase + fuzzy |
| Vector search | ❌ | HNSW KNN |
| Hybrid | ❌ | BM25 + KNN with RRF |
| Filtering | SQL WHERE | Term/range/geo/boolean |
| Scoring | ❌ | BM25 (+ optional recency decay) |
| Memory tiers | Manual expiry | SEMANTIC/EPISODIC/WORKING auto-TTL |
| Multi-tenancy | Manual | Native tenant_id |

### Three Schema Templates

**1. Checkpoint Storage** (replacing LangGraph SqliteSaver):
```json
{
  "fields": [
    { "name": "checkpoint_id", "type": "STRING", "key": true, "filterable": true },
    { "name": "thread_id",     "type": "STRING", "filterable": true },
    { "name": "channel_values","type": "STRING" },
    { "name": "metadata",      "type": "STRING", "searchable": true },
    { "name": "step",          "type": "INT32",  "filterable": true, "sortable": true },
    { "name": "created_at",    "type": "INT64",  "filterable": true, "sortable": true }
  ],
  "ttlEnabled": false
}
```

**2. Conversation Memory** (episodic with embeddings):
```json
{
  "fields": [
    { "name": "message_id",  "type": "STRING", "key": true, "filterable": true },
    { "name": "session_id",  "type": "STRING", "filterable": true },
    { "name": "role",        "type": "STRING", "filterable": true },
    { "name": "content",     "type": "STRING", "searchable": true },
    { "name": "embedding",   "type": "VECTOR", "searchable": true,
      "vectorDimension": 1536, "vectorSimilarity": "COSINE" },
    { "name": "timestamp",   "type": "INT64", "filterable": true, "sortable": true }
  ],
  "ttlEnabled": true,
  "hnswConfig": { "maxConn": 16, "beamWidth": 100 }
}
```

**3. Knowledge Base** (semantic, permanent):
```json
{
  "fields": [
    { "name": "fact_id",    "type": "STRING",  "key": true, "filterable": true },
    { "name": "category",   "type": "STRING",  "filterable": true },
    { "name": "title",      "type": "STRING",  "searchable": true },
    { "name": "body",       "type": "STRING",  "searchable": true },
    { "name": "embedding",  "type": "VECTOR",  "searchable": true,
      "vectorDimension": 1536, "vectorSimilarity": "COSINE" },
    { "name": "tags",       "type": "STRING_COLLECTION", "filterable": true },
    { "name": "confidence", "type": "DOUBLE",  "filterable": true, "sortable": true }
  ],
  "ttlEnabled": false,
  "hnswConfig": { "maxConn": 16, "beamWidth": 100 }
}
```

📖 Deep dive: [docs/AGENT_INTEGRATION_GUIDE.md](docs/AGENT_INTEGRATION_GUIDE.md)

---

## Module 9: Production Deployment

### Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_SERVER_PORT` | `50051` | gRPC listen port |
| `INDEX_CACHE_DIR` | `/data/index` | Lucene index directory |
| `TRANSLOG_DIRECTORY` | `/data/translog` | WAL directory |
| `SERVER_MODE` | `READ_WRITE` | `READ`, `WRITE`, or `READ_WRITE` |
| `TRANSLOG_DURABILITY` | `request` | `request` (fsync per op) or `async` |
| `RAM_BUFFER_SIZE_MB` | `256` | Lucene in-memory buffer before flush |
| `NRT_REFRESH_INTERVAL_MS` | `25` | Near-real-time refresh interval |
| `TTL_SWEEP_INTERVAL_MS` | `30000` | TTL expiry sweep interval |

### Docker Deployment

```bash
# Build fat JAR
./gradlew shadowJar

# Docker
docker compose up -d
```

### Durability Modes

| Mode | Behavior | Data Loss Window | Use Case |
|------|----------|-----------------|----------|
| `request` | fsync after every write | Zero | Financial, critical data |
| `async` | fsync every N ms | Up to N ms | High-throughput ingestion |

### Performance Tuning

```bash
# High-throughput ingestion
RAM_BUFFER_SIZE_MB=512          # Larger buffer = fewer flushes
TRANSLOG_DURABILITY=async       # Batch fsync
TRANSLOG_FLUSH_INTERVAL_MS=1000 # 1s fsync interval

# Low-latency search
NRT_REFRESH_INTERVAL_MS=10      # 10ms refresh
RAM_BUFFER_SIZE_MB=128          # Smaller buffer = faster NRT

# Add JVM vector optimization
java --add-modules jdk.incubator.vector -jar jigyasa.jar
```

### Monitoring

Use the `Health` RPC for liveness/readiness probes:
```bash
grpcurl -plaintext localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Health
```

📖 Full reference: [docs/REFERENCE.md](docs/REFERENCE.md)

---

## Module 10: Benchmarking

### Run Benchmarks

```bash
cd benchmarks/
pip install grpcio grpcio-tools

# Core benchmark (10K docs, all query types)
python benchmark.py

# Advanced benchmark (concurrent load, Jigyasa vs ES comparison)
python benchmark_advanced.py

# Scale benchmark (variable dataset sizes)
python benchmark_scale.py
```

### Reference Numbers (10K docs, single JVM)

| Query Type | Jigyasa p50 | ES Reference p50 |
|-----------|------------|------------------|
| BM25 text search | 2.15ms | ~5ms |
| Term filter | 1.58ms | ~3ms |
| Range filter | 1.31ms | ~4ms |
| Boolean compound | 1.39ms | ~5ms |
| Query string | 1.42ms | ~6ms |
| Match-all + sort | 1.55ms | ~2ms |
| Count API | 0.83ms | ~2ms |

---

## Learning Path Summary

```
Module 1: What Is Jigyasa?         ──→  Understand the "why"
Module 2: Quickstart                ──→  Running in 5 minutes
Module 3: Schema Design             ──→  Model your data
Module 4: Indexing Data             ──→  Write path mastery
Module 5: Query Mastery             ──→  All 12 query types
Module 6: Building a Real App       ──→  Tie it all together
Module 7: Operations                ──→  Collections, tenants, health
Module 8: AI Agent Integration      ──→  Replace SQLite in agents
Module 9: Production Deployment     ──→  Config, durability, tuning
Module 10: Benchmarking             ──→  Measure and validate
```

---

## Quick Reference

### Running Examples

```bash
./gradlew :examples:00-quickstart:run
./gradlew :examples:01-schema-and-indexing:run
./gradlew :examples:02-query-cookbook:run
./gradlew :examples:03-vector-and-hybrid:run
./gradlew :examples:04-configuration:run
./gradlew :examples:05-ops-and-multitenancy:run
./gradlew :examples:06-e-commerce-app:run
./gradlew :examples:07-multi-language-analyzers:run
```

### Proto Package

```protobuf
package jigyasa_dp_search;
option java_package = "com.jigyasa.dp.search.protocol";
service JigyasaDataPlaneService { ... }
```

### Further Reading

| Document | What's In It |
|----------|-------------|
| [README.md](README.md) | Project overview, performance numbers |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Component deep dive |
| [docs/REFERENCE.md](docs/REFERENCE.md) | Full config & API reference |
| [docs/AGENT_INTEGRATION_GUIDE.md](docs/AGENT_INTEGRATION_GUIDE.md) | AI agent patterns & schemas |
| [Apache Lucene Analysis](https://lucene.apache.org/core/10_0_0/analysis/common/index.html) | 42 analyzer behaviors |
