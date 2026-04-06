# Jigyasa: Agent Memory Engine — Integration Guide

> **Audience:** LLM agent developers replacing SQLite/blob storage with a searchable, vector-capable memory engine.
> **Protocol:** gRPC (protobuf) — package `jigyasa_dp_search`, service `JigyasaDataPlaneService`
> **Default port:** `50051` (override via `GRPC_SERVER_PORT` env var)

---

## Why Replace SQLite?

| Capability | SQLite | Jigyasa |
|-----------|--------|---------|
| Storage | Opaque BLOBs | Searchable JSON documents |
| Text search | `LIKE` only | BM25 full-text, phrase, fuzzy, prefix |
| Vector search | None | Native HNSW KNN (cosine, dot, L2) |
| Hybrid search | None | BM25 + KNN with configurable text/vector weight |
| Filtering | SQL `WHERE` | Term, range, geo-distance, geo-bbox, boolean compound, exists |
| Scoring | None | BM25, recency decay (exponential half-life), `min_score` threshold |
| Memory tiers | Manual | Built-in SEMANTIC / EPISODIC / WORKING with auto-TTL |
| Multi-tenancy | Manual | Native per-document tenant isolation |
| Pagination | `LIMIT/OFFSET` | Offset-based + cursor-based (`search_after`) for deep paging |

---

## Quick Start

### 1. Building and Starting the Server

**Requirements:** Java 21+, Gradle

```bash
# Build
./gradlew build

# Run (default port 50051)
java -cp build/libs/jigyasa-1.0-SNAPSHOT.jar com.jigyasa.dp.search.entrypoint.Main

# Override port
GRPC_SERVER_PORT=9090 java -cp build/libs/jigyasa-1.0-SNAPSHOT.jar com.jigyasa.dp.search.entrypoint.Main
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_SERVER_PORT` | `50051` | gRPC listen port |
| `INDEX_CACHE_DIR` | `./IndexData/` | Lucene index storage directory |
| `SERVER_MODE` | `READ_WRITE` | `WRITE`, `READ`, or `READ_WRITE` |
| `MAX_VECTOR_DIMENSION` | `2048` | Maximum vector dimension allowed |

### 2. Creating a Collection (= SQLite Table)

Every logical data store is a **collection**. Collections are created with a JSON schema that defines fields, types, and indexing behavior.

```protobuf
// gRPC call
rpc CreateCollection (CreateCollectionRequest) returns (CreateCollectionResponse);
```

```json
// CreateCollectionRequest
{
  "collection": "agent_checkpoints",
  "indexSchema": "{\"fields\":[{\"name\":\"checkpoint_id\",\"type\":\"STRING\",\"key\":true,\"searchable\":false,\"filterable\":true,\"sortable\":false},{\"name\":\"thread_id\",\"type\":\"STRING\",\"searchable\":false,\"filterable\":true,\"sortable\":false},{\"name\":\"channel_values\",\"type\":\"STRING\",\"searchable\":false,\"filterable\":false,\"sortable\":false},{\"name\":\"metadata\",\"type\":\"STRING\",\"searchable\":true,\"filterable\":false,\"sortable\":false},{\"name\":\"created_at\",\"type\":\"INT64\",\"searchable\":false,\"filterable\":true,\"sortable\":true}],\"ttlEnabled\":false}"
}
```

> **Note:** The `indexSchema` value is a JSON *string* (escaped). The schema persists across restarts.

### 3. Schema Design for Agent Memory

#### Checkpoint Storage (replacing SqliteSaver)

```json
{
  "fields": [
    { "name": "checkpoint_id", "type": "STRING", "key": true, "searchable": false, "filterable": true, "sortable": false },
    { "name": "thread_id", "type": "STRING", "searchable": false, "filterable": true, "sortable": false },
    { "name": "parent_checkpoint_id", "type": "STRING", "searchable": false, "filterable": true, "sortable": false },
    { "name": "channel_values", "type": "STRING", "searchable": false, "filterable": false, "sortable": false },
    { "name": "metadata", "type": "STRING", "searchable": true, "filterable": false, "sortable": false },
    { "name": "step", "type": "INT32", "searchable": false, "filterable": true, "sortable": true },
    { "name": "created_at", "type": "INT64", "searchable": false, "filterable": true, "sortable": true }
  ],
  "ttlEnabled": false
}
```

#### Conversation Memory (Episodic)

```json
{
  "fields": [
    { "name": "message_id", "type": "STRING", "key": true, "searchable": false, "filterable": true, "sortable": false },
    { "name": "session_id", "type": "STRING", "searchable": false, "filterable": true, "sortable": false },
    { "name": "role", "type": "STRING", "searchable": false, "filterable": true, "sortable": false },
    { "name": "content", "type": "STRING", "searchable": true, "filterable": false, "sortable": false },
    { "name": "embedding", "type": "VECTOR", "dimension": 1536, "similarityFunction": "COSINE" },
    { "name": "timestamp", "type": "INT64", "searchable": false, "filterable": true, "sortable": true }
  ],
  "ttlEnabled": true
}
```

#### Knowledge Base (Semantic + Vector)

```json
{
  "fields": [
    { "name": "fact_id", "type": "STRING", "key": true, "searchable": false, "filterable": true, "sortable": false },
    { "name": "category", "type": "STRING", "searchable": false, "filterable": true, "sortable": true },
    { "name": "title", "type": "STRING", "searchable": true, "filterable": false, "sortable": false },
    { "name": "body", "type": "STRING", "searchable": true, "filterable": false, "sortable": false },
    { "name": "embedding", "type": "VECTOR", "dimension": 1536, "similarityFunction": "COSINE" },
    { "name": "source", "type": "STRING", "searchable": false, "filterable": true, "sortable": false },
    { "name": "tags", "type": "STRING_COLLECTION", "searchable": true, "filterable": true, "sortable": false },
    { "name": "confidence", "type": "DOUBLE", "searchable": false, "filterable": true, "sortable": true }
  ],
  "ttlEnabled": false,
  "hnswConfig": { "maxConn": 16, "beamWidth": 100 }
}
```

---

## API Reference

### Document Operations

#### Index (Create / Update / Delete) — Bulk Supported

```protobuf
rpc Index (IndexRequest) returns (IndexResponse);
```

**IndexRequest:**

| Field | Type | Description |
|-------|------|-------------|
| `collection` | string | Collection name (empty = `"default"`) |
| `item` | repeated IndexItem | Batch of documents to index |

**IndexItem:**

| Field | Type | Description |
|-------|------|-------------|
| `action` | IndexAction | `UPDATE` (upsert) or `DELETE` |
| `document` | string | JSON document string (must include the `key` field) |
| `memory_tier` | MemoryTier | `SEMANTIC` (permanent), `EPISODIC` (24h TTL), `WORKING` (5min TTL) |
| `ttl_seconds` | int32 | Custom TTL override; `0` = use tier default |
| `tenant_id` | string | Tenant ID for multi-tenant isolation (optional) |

**Example — index a checkpoint:**
```json
{
  "collection": "agent_checkpoints",
  "item": [
    {
      "action": "UPDATE",
      "document": "{\"checkpoint_id\":\"cp-456\",\"thread_id\":\"thread-123\",\"parent_checkpoint_id\":\"cp-455\",\"channel_values\":\"{\\\"messages\\\":[...]}\",\"metadata\":\"{\\\"step\\\":5,\\\"source\\\":\\\"loop\\\"}\",\"step\":5,\"created_at\":1711929600000}",
      "memory_tier": "SEMANTIC"
    }
  ]
}
```

**Example — index episodic conversation message:**
```json
{
  "collection": "conversations",
  "item": [
    {
      "action": "UPDATE",
      "document": "{\"message_id\":\"msg-001\",\"session_id\":\"sess-789\",\"role\":\"assistant\",\"content\":\"The user asked about deployment strategies\",\"embedding\":[0.1,0.2,0.05,-0.3],\"timestamp\":1711929600000}",
      "memory_tier": "EPISODIC",
      "tenant_id": "user-42"
    }
  ]
}
```

**Example — delete a document:**
```json
{
  "collection": "agent_checkpoints",
  "item": [
    {
      "action": "DELETE",
      "document": "{\"checkpoint_id\":\"cp-456\"}"
    }
  ]
}
```

**IndexResponse:** Returns a `google.rpc.Status` per item (OK on success, error details on failure).

---

#### Lookup (by Key)

```protobuf
rpc Lookup (LookupRequest) returns (LookupResponse);
```

| Field | Type | Description |
|-------|------|-------------|
| `docKeys` | repeated string | Key field values to retrieve |
| `collection` | string | Collection name |

**Example:**
```json
{
  "collection": "agent_checkpoints",
  "docKeys": ["cp-456", "cp-455"]
}
```

**LookupResponse:** `documents` — array of JSON strings (one per found key, in order).

---

#### DeleteByQuery (Filtered Delete)

```protobuf
rpc DeleteByQuery (DeleteByQueryRequest) returns (DeleteByQueryResponse);
```

| Field | Type | Description |
|-------|------|-------------|
| `filters` | repeated FilterClause | AND-combined filters defining docs to delete |
| `tenant_id` | string | Scope deletion to a tenant (optional) |
| `collection` | string | Collection name |

**Example — delete all checkpoints for a thread:**
```json
{
  "collection": "agent_checkpoints",
  "filters": [
    { "field": "thread_id", "term_filter": { "value": "thread-123" } }
  ]
}
```

**Response:** `deleted_count` — may be `-1` (Lucene does not provide synchronous delete count). Query before/after if exact count is needed.

---

### Query Operations

All query types use a single unified RPC:

```protobuf
rpc Query (QueryRequest) returns (QueryResponse);
```

#### Text Search (BM25)

```json
{
  "collection": "conversations",
  "text_query": "deployment strategies",
  "text_field": "content",
  "top_k": 10,
  "include_source": true
}
```

- `text_field` empty → searches default search field (first searchable field).
- Scored by BM25 (k1=1.2, b=0.75 defaults, configurable via schema `bm25Config`).

#### Phrase Search

```json
{
  "collection": "knowledge",
  "phrase_query": "machine learning",
  "phrase_field": "body",
  "phrase_slop": 0,
  "top_k": 5,
  "include_source": true
}
```

- `phrase_slop`: `0` = exact phrase, `1` = one word gap allowed, etc.

#### Fuzzy Search (Typo-Tolerant)

```json
{
  "collection": "knowledge",
  "fuzzy_query": "kubernetse",
  "fuzzy_field": "title",
  "max_edits": 2,
  "prefix_length": 3,
  "top_k": 10,
  "include_source": true
}
```

- `max_edits`: 0–2 (Lucene max is 2).
- `prefix_length`: number of leading characters that must match exactly (improves performance).

#### Prefix Search

```json
{
  "collection": "knowledge",
  "prefix_query": "deploy",
  "prefix_field": "title",
  "top_k": 10,
  "include_source": true
}
```

#### Query String (Full Lucene Syntax)

```json
{
  "collection": "knowledge",
  "query_string": "title:kubernetes AND body:\"service mesh\" AND NOT deprecated",
  "query_string_default_field": "body",
  "top_k": 20,
  "include_source": true
}
```

- **Takes precedence** over `text_query`, `phrase_query`, `fuzzy_query`, `prefix_query` when set.
- See [Query String Syntax Reference](#query-string-syntax-reference) below.

#### Vector Search (KNN)

```json
{
  "collection": "conversations",
  "vector_query": {
    "field": "embedding",
    "vector": [0.1, 0.2, 0.05, -0.3],
    "k": 10
  },
  "include_source": true
}
```

#### Hybrid Search (BM25 + KNN)

Set both `text_query` and `vector_query` on the same request:

```json
{
  "collection": "conversations",
  "text_query": "deployment strategies",
  "text_field": "content",
  "vector_query": {
    "field": "embedding",
    "vector": [0.1, 0.2, 0.05, -0.3],
    "k": 20
  },
  "text_weight": 0.5,
  "top_k": 10,
  "include_source": true
}
```

- `text_weight`: `0.0` = vector only, `1.0` = text only, `0.5` = equal blend.

#### Count (Efficient Doc Count)

```protobuf
rpc Count (CountRequest) returns (CountResponse);
```

```json
{
  "collection": "agent_checkpoints",
  "filters": [
    { "field": "thread_id", "term_filter": { "value": "thread-123" } }
  ]
}
```

**Response:** `{ "count": 42 }` — Uses `IndexSearcher.count()`, no scoring, no doc retrieval.

---

### Filtering

Filters are **non-scoring constraints** added to any query. Multiple top-level filters are AND-combined.

#### Term Filter (Exact Match)

Works on STRING, BOOLEAN, INT32, INT64, DOUBLE fields.

```json
{ "field": "thread_id", "term_filter": { "value": "thread-123" } }
```

```json
{ "field": "role", "term_filter": { "value": "assistant" } }
```

#### Range Filter

Works on INT32, INT64, DOUBLE, DATE_TIME_OFFSET fields.

```json
{
  "field": "created_at",
  "range_filter": {
    "min": "1711929600000",
    "max": "1711939600000",
    "min_exclusive": false,
    "max_exclusive": true
  }
}
```

- `min`/`max` are strings (serialized values). Empty = unbounded on that side.
- `min_exclusive`/`max_exclusive` default to `false` (inclusive).

#### Geo Distance Filter

```json
{
  "field": "location",
  "geo_distance_filter": {
    "lat": 37.7749,
    "lon": -122.4194,
    "distance_meters": 5000.0
  }
}
```

#### Geo Bounding Box Filter

```json
{
  "field": "location",
  "geo_bounding_box_filter": {
    "top_lat": 37.80,
    "left_lon": -122.50,
    "bottom_lat": 37.70,
    "right_lon": -122.40
  }
}
```

#### Exists Filter

```json
{ "field": "embedding", "exists_filter": { "must_exist": true } }
```

```json
{ "field": "deprecated_field", "exists_filter": { "must_exist": false } }
```

#### Compound Filter (Boolean Logic)

Supports nesting for complex queries.

```json
{
  "field": "",
  "compound_filter": {
    "operator": "AND",
    "must": [
      { "field": "role", "term_filter": { "value": "assistant" } }
    ],
    "should": [
      { "field": "category", "term_filter": { "value": "facts" } },
      { "field": "category", "term_filter": { "value": "preferences" } }
    ],
    "must_not": [
      { "field": "archived", "term_filter": { "value": "true" } }
    ]
  }
}
```

- `must`: All must match (AND).
- `should`: At least one must match (OR).
- `must_not`: None must match (exclusion/NOT).
- **Nestable**: Any clause inside `must`/`should`/`must_not` can itself be a `compound_filter`.
- **Must not be empty** — at least one of `must`/`should`/`must_not` must contain clauses.

---

### Sorting

```json
{
  "sort": [
    { "field": "created_at", "descending": true },
    { "field": "step", "descending": false }
  ]
}
```

- Sortable fields: any field with `"sortable": true` in schema.
- Supported types for sorting: INT32, INT64, DOUBLE, STRING, DATE_TIME_OFFSET, BOOLEAN, GEO_POINT.
- Empty sort array → sort by relevance score (descending).

#### Geo Distance Sort

```json
{
  "sort": [
    {
      "field": "location",
      "descending": false,
      "geo_origin": { "lat": 37.7749, "lon": -122.4194 }
    }
  ]
}
```

---

### Pagination

#### Offset-Based

```json
{ "top_k": 10, "offset": 20 }
```

Returns results 21–30. Simple but inefficient for deep pages.

#### Cursor-Based (Recommended for Deep Pagination)

**First page:**
```json
{ "top_k": 10 }
```

**Response includes:**
```json
{
  "total_hits": 1500,
  "total_hits_exact": false,
  "next_search_after": {
    "score": 3.45,
    "doc_id": 1022,
    "sort_field_values": ["1711929600000", "5"]
  },
  "hits": [...]
}
```

**Next page — pass the token back:**
```json
{
  "top_k": 10,
  "search_after": {
    "score": 3.45,
    "doc_id": 1022,
    "sort_field_values": ["1711929600000", "5"]
  }
}
```

---

### Response Control

| Field | Type | Description |
|-------|------|-------------|
| `include_source` | bool | Return full JSON document in `hit.source` |
| `source_fields` | repeated string | Return only these fields (implies `include_source`) |
| `min_score` | float | Exclude hits below this score threshold |

**Example — projected fields with minimum score:**
```json
{
  "collection": "knowledge",
  "text_query": "kubernetes deployment",
  "text_field": "body",
  "top_k": 5,
  "source_fields": ["title", "category", "confidence"],
  "min_score": 1.5
}
```

**QueryResponse:**
```json
{
  "total_hits": 42,
  "total_hits_exact": true,
  "hits": [
    {
      "score": 4.23,
      "doc_id": "fact-101",
      "source": "{\"title\":\"K8s Blue-Green Deploy\",\"category\":\"devops\",\"confidence\":0.95}"
    }
  ],
  "next_search_after": { "score": 2.1, "doc_id": 507, "sort_field_values": [] }
}
```

---

### Memory Features

#### Memory Tiers

| Tier | Enum Value | Default TTL | Use Case |
|------|-----------|-------------|----------|
| `SEMANTIC` | 0 | Never expires | Facts, preferences, knowledge base |
| `EPISODIC` | 1 | 24 hours | Conversation memory, session context |
| `WORKING` | 2 | 5 minutes | Scratchpad, intermediate computation |

Set on each `IndexItem.memory_tier`. Override TTL with `IndexItem.ttl_seconds`.

#### TTL (Time-To-Live)

- **Requires** `"ttlEnabled": true` in the collection schema.
- Background sweeper runs every **30 seconds**, deleting expired documents.
- System field `_ttl_expires_at` (epoch ms) is set automatically. `0` = never expires.
- Custom TTL: set `ttl_seconds` on `IndexItem` to override the tier default.

#### Recency Decay

Exponentially boost recent documents during retrieval:

```json
{
  "collection": "conversations",
  "text_query": "error handling",
  "recency_decay": { "half_life_seconds": 3600 },
  "top_k": 10,
  "include_source": true
}
```

- `half_life_seconds`: At 1 half-life ago, boost = 0.5×. At 2 half-lives, boost = 0.25×. Default: 3600 (1 hour). Set `0` to disable.
- Only works when `ttlEnabled=true` (requires `_indexed_at` system field).

#### Tenant Isolation

Isolate data per agent, user, or organization:

```json
// Index with tenant
{
  "item": [{ "document": "{...}", "tenant_id": "agent-007" }],
  "collection": "memory"
}

// Query within tenant
{
  "collection": "memory",
  "text_query": "previous approach",
  "tenant_id": "agent-007",
  "top_k": 10
}
```

System field `_tenant_id` is automatically indexed and filtered.

---

### Collection Management

| RPC | Description |
|-----|-------------|
| `CreateCollection` | Create a new collection with schema |
| `CloseCollection` | Release resources (writer/searcher) but preserve data on disk |
| `OpenCollection` | Reopen a closed collection (schema optional — reads persisted schema) |
| `ListCollections` | List all open collection names |
| `Health` | Server health + per-collection stats (doc count, segment count, writer/searcher status) |
| `ForceMerge` | Optimize segments (set `max_segments`, default 1). Returns segments before/after |
| `UpdateSchema` | Update schema for an existing collection |

**Health response example:**
```json
{
  "status": "SERVING",
  "collections": [
    {
      "name": "agent_checkpoints",
      "writer_open": true,
      "searcher_available": true,
      "doc_count": 15230,
      "segment_count": 3
    }
  ]
}
```

---

## Common Patterns for LLM Agents

### Pattern 1: Replacing SqliteSaver (LangGraph Checkpointing)

**Schema:** (see [Checkpoint Storage](#checkpoint-storage-replacing-sqlitesaver) above)

**Write a checkpoint:**
```json
// IndexRequest
{
  "collection": "agent_checkpoints",
  "item": [{
    "action": "UPDATE",
    "document": "{\"checkpoint_id\":\"cp-456\",\"thread_id\":\"thread-123\",\"parent_checkpoint_id\":\"cp-455\",\"channel_values\":\"{\\\"messages\\\":[{\\\"role\\\":\\\"user\\\",\\\"content\\\":\\\"deploy to staging\\\"}],\\\"next_step\\\":\\\"execute\\\"}\",\"metadata\":\"{\\\"step\\\":5,\\\"source\\\":\\\"loop\\\",\\\"writes\\\":{\\\"agent\\\":{\\\"messages\\\":[...]}}}\",\"step\":5,\"created_at\":1711929600000}",
    "memory_tier": "SEMANTIC"
  }]
}
```

**Get latest checkpoint for a thread:**
```json
// QueryRequest
{
  "collection": "agent_checkpoints",
  "filters": [
    { "field": "thread_id", "term_filter": { "value": "thread-123" } }
  ],
  "sort": [{ "field": "created_at", "descending": true }],
  "top_k": 1,
  "include_source": true
}
```

**Get specific checkpoint by key:**
```json
// LookupRequest
{
  "collection": "agent_checkpoints",
  "docKeys": ["cp-456"]
}
```

**List checkpoint history (parent chain):**
```json
// QueryRequest — get all checkpoints for a thread, ordered
{
  "collection": "agent_checkpoints",
  "filters": [
    { "field": "thread_id", "term_filter": { "value": "thread-123" } }
  ],
  "sort": [{ "field": "step", "descending": false }],
  "top_k": 100,
  "include_source": true
}
```

**Delete all checkpoints for a thread:**
```json
// DeleteByQueryRequest
{
  "collection": "agent_checkpoints",
  "filters": [
    { "field": "thread_id", "term_filter": { "value": "thread-123" } }
  ]
}
```

---

### Pattern 2: Conversation Memory (Episodic)

**Index a message with auto-expiry:**
```json
{
  "collection": "conversations",
  "item": [{
    "action": "UPDATE",
    "document": "{\"message_id\":\"msg-042\",\"session_id\":\"sess-789\",\"role\":\"assistant\",\"content\":\"The user prefers blue-green deployments over canary\",\"embedding\":[0.1,0.2,0.05,-0.3],\"timestamp\":1711929600000}",
    "memory_tier": "EPISODIC",
    "tenant_id": "user-42"
  }]
}
```

**Hybrid retrieval — find relevant past messages:**
```json
{
  "collection": "conversations",
  "text_query": "deployment strategy",
  "text_field": "content",
  "vector_query": {
    "field": "embedding",
    "vector": [0.12, 0.19, 0.04, -0.28],
    "k": 20
  },
  "text_weight": 0.4,
  "filters": [
    { "field": "session_id", "term_filter": { "value": "sess-789" } }
  ],
  "recency_decay": { "half_life_seconds": 7200 },
  "tenant_id": "user-42",
  "top_k": 5,
  "include_source": true
}
```

---

### Pattern 3: Knowledge Base (Semantic Memory)

**Index a permanent fact:**
```json
{
  "collection": "knowledge",
  "item": [{
    "action": "UPDATE",
    "document": "{\"fact_id\":\"fact-301\",\"category\":\"user_prefs\",\"title\":\"Deployment preference\",\"body\":\"User strongly prefers blue-green deployments for production. Canary is acceptable for staging only.\",\"embedding\":[0.1,0.2,...],\"source\":\"conversation-2024-03-25\",\"tags\":[\"deployment\",\"preferences\",\"production\"],\"confidence\":0.92}",
    "memory_tier": "SEMANTIC"
  }]
}
```

**Semantic search with keyword filter:**
```json
{
  "collection": "knowledge",
  "vector_query": {
    "field": "embedding",
    "vector": [0.11, 0.21, ...],
    "k": 10
  },
  "filters": [
    { "field": "category", "term_filter": { "value": "user_prefs" } },
    { "field": "confidence", "range_filter": { "min": "0.7" } }
  ],
  "top_k": 5,
  "include_source": true
}
```

---

### Pattern 4: Agent Task Memory with Feedback

**Schema:**
```json
{
  "fields": [
    { "name": "task_id", "type": "STRING", "key": true, "searchable": false, "filterable": true },
    { "name": "task_type", "type": "STRING", "searchable": false, "filterable": true, "sortable": true },
    { "name": "description", "type": "STRING", "searchable": true, "filterable": false },
    { "name": "approach", "type": "STRING", "searchable": true, "filterable": false },
    { "name": "embedding", "type": "VECTOR", "dimension": 1536, "similarityFunction": "COSINE" },
    { "name": "success", "type": "BOOLEAN", "searchable": false, "filterable": true },
    { "name": "error_message", "type": "STRING", "searchable": true, "filterable": false },
    { "name": "duration_ms", "type": "INT64", "searchable": false, "filterable": true, "sortable": true },
    { "name": "attempt_count", "type": "INT32", "searchable": false, "filterable": true, "sortable": true },
    { "name": "created_at", "type": "INT64", "searchable": false, "filterable": true, "sortable": true }
  ],
  "ttlEnabled": true
}
```

**Query past successful approaches for similar tasks:**
```json
{
  "collection": "task_memory",
  "vector_query": {
    "field": "embedding",
    "vector": [0.1, 0.2, ...],
    "k": 10
  },
  "filters": [
    { "field": "success", "term_filter": { "value": "true" } },
    { "field": "task_type", "term_filter": { "value": "code_generation" } }
  ],
  "sort": [{ "field": "created_at", "descending": true }],
  "top_k": 3,
  "source_fields": ["description", "approach", "duration_ms"],
  "min_score": 0.5
}
```

**Query failed attempts to avoid repeating mistakes:**
```json
{
  "collection": "task_memory",
  "text_query": "deploy kubernetes",
  "text_field": "description",
  "filters": [
    { "field": "success", "term_filter": { "value": "false" } }
  ],
  "top_k": 5,
  "source_fields": ["description", "approach", "error_message"]
}
```

---

## Field Type Reference

| Type | Searchable | Filterable | Sortable | Notes |
|------|-----------|------------|----------|-------|
| `STRING` | ✅ BM25 full-text | ✅ Term | ✅ Alphabetical | Analyzed for search, keyword for filter |
| `BOOLEAN` | ❌ | ✅ Term | ✅ | Values: `"true"` / `"false"` as strings |
| `INT32` | ❌ | ✅ Term + Range | ✅ | 32-bit integer |
| `INT64` | ❌ | ✅ Term + Range | ✅ | 64-bit long (use for timestamps) |
| `DOUBLE` | ❌ | ✅ Term + Range | ✅ | 64-bit floating point |
| `DATE_TIME_OFFSET` | ❌ | ✅ Term + Range | ✅ | Epoch milliseconds |
| `GEO_POINT` | ❌ | ✅ Distance + BBox | ✅ Distance sort | JSON: `{"lat": 37.77, "lon": -122.41}` |
| `VECTOR` | ❌ | ❌ | ❌ | HNSW KNN; requires `dimension` and `similarityFunction` |
| `STRING_COLLECTION` | ✅ BM25 | ✅ Term | ❌ | Multi-value string array |
| `BOOLEAN_COLLECTION` | ❌ | ✅ Term | ❌ | Multi-value boolean |
| `INT32_COLLECTION` | ❌ | ✅ Term + Range | ❌ | Multi-value int32 |
| `INT64_COLLECTION` | ❌ | ✅ Term + Range | ❌ | Multi-value int64 |
| `DOUBLE_COLLECTION` | ❌ | ✅ Term + Range | ❌ | Multi-value double |
| `DATE_TIME_OFFSET_COLLECTION` | ❌ | ✅ Term + Range | ❌ | Multi-value datetime |
| `GEO_POINT_COLLECTION` | ❌ | ✅ Distance + BBox | ❌ | Multi-value geo points |

### Vector Similarity Functions

| Function | Description |
|----------|-------------|
| `COSINE` | Cosine similarity (default). Best for normalized embeddings. |
| `DOT_PRODUCT` | Dot product. Use with pre-normalized vectors for speed. |
| `EUCLIDEAN` | L2 distance. Lower = more similar. |
| `MAXIMUM_INNER_PRODUCT` | Maximum inner product. Supports non-normalized vectors. |

### HNSW Configuration

Set via `hnswConfig` in schema JSON:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxConn` | 16 | Max connections per HNSW graph node. Higher = more accurate, more memory. |
| `beamWidth` | 100 | Beam width during index build. Higher = slower indexing, better recall. |
| `scalarQuantization` | false | Enable int8 scalar quantization (~4× memory reduction). |

### BM25 Configuration

Set via `bm25Config` in schema JSON:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `k1` | 1.2 | Term frequency saturation. Higher = more weight on term frequency. |
| `b` | 0.75 | Length normalization. `0` = no normalization, `1` = full normalization. |

---

## Query String Syntax Reference

Full Lucene QueryParser syntax available via `query_string` field:

| Syntax | Example | Description |
|--------|---------|-------------|
| Field-scoped | `title:kubernetes` | Search specific field |
| Phrase | `"machine learning"` | Exact phrase match |
| Fuzzy | `kubernetes~2` | Edit distance (0–2) |
| Prefix wildcard | `deploy*` | Prefix match |
| Range (inclusive) | `status:[400 TO 499]` | Inclusive range |
| Range (exclusive) | `price:{10 TO 100}` | Exclusive range |
| Boolean | `(A OR B) AND NOT C` | Boolean operators |
| Grouping | `(error OR warning) AND critical` | Parenthetical grouping |
| Field boost | `title:ml^2.0` | Boost field relevance |
| Wildcard | `depl?y` | Single-character wildcard |
| Required | `+kubernetes -deprecated` | Must include / must exclude |

**Note:** `query_string` takes precedence over `text_query`, `phrase_query`, `fuzzy_query`, and `prefix_query`.

---

## System Fields

When `ttlEnabled=true`, Jigyasa automatically injects and manages these fields:

| Field | Type | Description |
|-------|------|-------------|
| `_memory_tier` | STRING (keyword) | `SEMANTIC`, `EPISODIC`, or `WORKING`. Filterable. |
| `_ttl_expires_at` | INT64 | Epoch ms when doc expires. `0` = never. Range-filterable. |
| `_indexed_at` | INT64 | Epoch ms when doc was indexed. Used by recency decay. |
| `_tenant_id` | STRING (keyword) | Tenant ID. Auto-filtered when `tenant_id` is set on query. |

These fields are queryable/filterable like any other field.

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| NRT refresh latency | 25ms min stale, 1s max stale |
| Write buffer | 64 MB RAM before flush |
| Merge policy | TieredMergePolicy, 10 segments/tier |
| Count API | O(segment_count), no scoring overhead |
| Vector search | HNSW with configurable maxConn (16) / beamWidth (100) |
| TTL sweeper | Runs every 30 seconds |
| Compound files | Enabled (fewer file handles) |
| Lucene version | 10.4.0 |
| gRPC version | 1.80.0 |

---

## Error Handling

All errors are returned as gRPC status codes:

| Condition | gRPC Status | Details |
|-----------|------------|---------|
| Bad query/filter syntax | `INVALID_ARGUMENT` | Error message with parse details |
| Filter on non-existent field | `INVALID_ARGUMENT` | Field not found in schema |
| Sort on non-sortable field | `INVALID_ARGUMENT` | Field must have `sortable: true` |
| Empty CompoundFilter | `INVALID_ARGUMENT` | At least one clause required |
| Invalid `query_string` syntax | `INVALID_ARGUMENT` | Lucene parse error details |
| Collection not found | `INVALID_ARGUMENT` | Collection must be created first |
| Server error | `INTERNAL` | Unexpected server-side failure |

---

## gRPC Service Summary

```protobuf
service JigyasaDataPlaneService {
  // Document operations
  rpc Index (IndexRequest) returns (IndexResponse);              // Create/Update/Delete docs (bulk)
  rpc Lookup (LookupRequest) returns (LookupResponse);           // Get docs by key
  rpc Query (QueryRequest) returns (QueryResponse);              // Search (text/vector/hybrid/filters)
  rpc DeleteByQuery (DeleteByQueryRequest) returns (DeleteByQueryResponse); // Filtered bulk delete
  rpc Count (CountRequest) returns (CountResponse);              // Fast filtered count

  // Schema
  rpc UpdateSchema (UpdateSchemaRequest) returns (UpdateSchemaResponse);

  // Collection lifecycle
  rpc CreateCollection (CreateCollectionRequest) returns (CreateCollectionResponse);
  rpc CloseCollection (CloseCollectionRequest) returns (CloseCollectionResponse);
  rpc OpenCollection (OpenCollectionRequest) returns (OpenCollectionResponse);
  rpc ListCollections (ListCollectionsRequest) returns (ListCollectionsResponse);

  // Operations
  rpc Health (HealthRequest) returns (HealthResponse);
  rpc ForceMerge (ForceMergeRequest) returns (ForceMergeResponse);
}
```
