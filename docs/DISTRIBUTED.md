# Distributed Jigyasa — Architecture Document

**Author:** Lokesh Mawale
**Status:** Design — Reviewed by Claude Sonnet 4.5 + GPT-5.2-Codex
**Core principle:** Jigyasa is untouched. Distribution is a layer above it.

---

## 1. Problem Statement

Jigyasa is 2-5x faster than Elasticsearch on a single node. But single-node search has hard ceilings: one machine's storage, one machine's CPU for throughput, zero fault tolerance. The industry answer (ES/OS) is a 600MB distributed system with master elections, split-brain risk, fixed shard counts, wrong BM25 scores, approximate aggregations, and 20-minute shard recoveries.

We can do better. No leader elections. No split-brain. No approximate scoring. No fixed shard counts.

---

## 2. Design Constraints

| Constraint | Implication |
|---|---|
| Jigyasa core is frozen | All distribution logic lives in a separate layer |
| No leader election. No Raft. No ZooKeeper. | Every node is equal. State in Redis (cache) + S3 (source of truth). Concurrency via Lua scripts + epoch fencing. |
| Per-index independent scaling | Scaling "products" does not touch "logs" |
| Correct BM25 scoring | Global IDF via DFS phase — mathematically correct, not approximate |
| Exact facet counts | Coordinator sums all shards — not top-N approximation |
| Grow without reindexing | Shard splitting via Lucene soft deletes — near-zero data movement |
| Every operation is idempotent | Multiple nodes can attempt the same action safely |
| Metadata is durable | Every Redis state mutation also writes to S3 (versioned objects) |
| Observability from day one | Metrics, tracing, structured logs before production |

---

## 3. Data Partitioning

### 3.1 Three Strategies — User Chooses Per Index

```json
{
  "index": "products",
  "strategy": "document",
  "shards": 4,
  "replicas": 2
}
```

#### Strategy A: SINGLE (Default)

No sharding. One Jigyasa instance holds the entire index. Scale reads with replicas only.

- **When:** Index < 10M docs, or where single-node latency meets SLA
- **BM25:** Perfect — global stats by definition
- **Facets:** Exact — no merging needed
- **Max practical size:** ~1.2B docs (Lucene int32 doc ID limit)

Most users never leave this mode.

#### Strategy B: DOCUMENT (Consistent Hash Ring)

Partition documents across N shards using consistent hashing with 256 virtual nodes per physical shard.

```
doc routing: slot = murmur3(doc.key_field) % 1024
             shard = ring.lookup(slot)
```

- **When:** Index exceeds single-node storage or throughput
- **BM25:** Requires DFS phase for correctness (see §4)
- **Facets:** Exact — coordinator sums bucket counts
- **Growth:** Shard splitting without reindex (see §3.2)

#### Strategy C: TIME (Rollover)

Each time window is a separate index. Managed by lifecycle checks.

```
logs-2026-04-11T00  → active (R/W), 2 shards
logs-2026-04-10T18  → warm (READ), force-merged
logs-2026-04-10T12  → frozen (S3 on-demand)
logs-2026-04-10T06  → deleted (retention)

Alias "logs-write" → logs-2026-04-11T00
Alias "logs-read"  → union of all non-deleted windows
```

Rollover triggers (first hit wins): `max_docs: 50M`, `max_size_gb: 25`, `max_age: 6h`.

### 3.2 Shard Splitting

**Honest cost: near-zero reindex, not zero data movement.** Force-merge is mandatory and I/O-intensive. Requires 3x storage during the split window.

```
Before: shard-0 owns hash range [0, 511]

Split (background, queries served throughout):
  1. Pause writes to shard-0 (buffer in coordinator, ~seconds)
  2. Copy shard-0's segment files (S3 copy or hard-link on local FS)
  3. shard-0a [0, 255]: Lucene soft-delete docs with hash ∈ [256, 511]
     shard-0b [256, 511]: Lucene soft-delete docs with hash ∈ [0, 255]
  4. Force-merge each to reclaim space (background, can take minutes)
     Queries on shard-0a/0b are valid during merge (live docs only)
  5. Atomic cutover: CAS update shard-map in Redis + S3
     Old shard-0 still serves until cutover completes
  6. Resume writes to shard-0a and shard-0b
  7. Retire shard-0 after all in-flight queries drain

Storage during split: 3x (original + 2 copies + merge temp). Document as hard requirement.
```

---

## 4. Scoring Correctness

### Two-Phase DFS (Distributed Frequency Statistics)

```
Phase 1 — Stats Collection (~1-2ms):
  Node → all shards: "For terms [kubernetes, deployment], give me DF, N, sumTTF"

Phase 2 — Compute global IDF:
  global_df = sum(per_shard_df)
  global_n  = sum(per_shard_n)

Phase 3 — Search with global stats:
  Node → all shards: QueryRequest + global_stats
  Each shard scores using global IDF instead of local
```

**When DFS runs:**

| Scenario | DFS? | Why |
|---|---|---|
| SINGLE strategy | No | One shard = global stats by definition |
| DOCUMENT + `dfs: true` | Yes | User opts in for correct scoring |
| TIME strategy | No | Per-window scoring acceptable |
| Filter/Count/Facet-only | No | No scoring involved |

### Stats Freshness

DFS stats cache has **no TTL**. Instead, invalidated incrementally:

```
On document insert/delete (batched every 5s):
  affected_terms = extract_terms(doc)
  DEL dj:index:{name}:global-stats:{term} for each affected term

On next DFS query for a deleted term:
  cache miss → recompute from shards → cache result
```

This eliminates stale-score windows and score discontinuities at cache expiry.

**DFS short-circuit:** If all query terms have `df > N/100` (common terms), per-shard IDF is close enough to global — skip DFS. Saves overhead for 80%+ of queries.

---

## 5. Facet Correctness

Each shard returns exact counts for ALL matching docs (not top-N approximation):

```
Shard 0: {Electronics: 42, Books: 31}
Shard 1: {Electronics: 38, Toys: 25}
Merged:  {Electronics: 80, Books: 31, Toys: 25}  ← EXACT
```

**Precision guarantees:**
- Integer fields + term facets: **Exact**
- Double/float fields: **±1 ULP** (Lucene stores doubles with IEEE 754 precision in DocValues)
- Date facets: **Exact** (all shards must use UTC — coordinator validates timezone consistency)
- Range facets with identical `interval`: **Exact** (identical bucket boundaries across shards)

For high-cardinality fields (>10K unique terms per shard), coordinator requests `count: requested_count × 2` from each shard as a safety margin, then merges and trims.

---

## 6. Leaderless Architecture

### Every Node is Equal

No coordinator vs orchestrator distinction. No leader election. Every node routes queries, runs lifecycle, handles failover.

```
┌─────────────────┐
│  Load Balancer   │
└────────┬────────┘
         │
  ┌──────┼───────────────────────┐
  │      │                       │
  │  ┌───▼───┐  ┌───────┐  ┌────▼──┐
  │  │Node 1 │  │Node 2 │  │Node 3 │
  │  │ EQUAL │  │ EQUAL │  │ EQUAL │
  │  └───┬───┘  └───┬───┘  └───┬───┘
  │      └───────────┼──────────┘
  │                  │
  │       ┌──────────▼──────────┐
  │       │   Redis (cache)     │
  │       │   + Pub/Sub         │
  │       └──────────┬──────────┘
  │                  │
  │       ┌──────────▼──────────┐
  │       │   S3 (source of     │
  │       │   truth for         │
  │       │   metadata +        │
  │       │   segments)         │
  │       └─────────────────────┘
  │                  │
  │   ┌──────────────┼──────────────┐
  │   │              │              │
  │ ┌─▼──────────┐ ┌▼──────────┐ ┌─▼─────────┐
  │ │ products    │ │ logs       │ │ memories   │
  │ │ writer ×1  │ │ writer ×1  │ │ writer ×1  │
  │ │ reader ×5  │ │ reader ×2  │ │ reader ×1  │
  │ │ HPA: p99   │ │ HPA: CPU   │ │ fixed      │
  │ └────────────┘ └────────────┘ └────────────┘
  │       Per-index independent scaling
  └──────────────────────────────────────────
```

### Epoch Fencing — Prevents Split-Brain

Every shard has a monotonically increasing epoch number. CAS promotions increment the epoch. Writers include the epoch in every write. Stale writers are rejected immediately.

```
Redis: dj:index:products:shard-0:epoch = 10

Writer-0 sends write with epoch=10 → accepted
Node-A detects Writer-0 slow, promotes Replica-1:
  epoch → 11 (atomic via Lua script)
Writer-0 wakes up, sends write with epoch=10 → REJECTED (stale epoch)
Replica-1 (now primary) sends write with epoch=11 → accepted
```

**No split-brain possible.** Old primary is fenced out the instant a new epoch is published.

### Atomic Operations via Redis Lua Scripts

All state mutations use Lua scripts — atomic, no retry storm, no WATCH/MULTI races.

**Failover (atomic health check + promote):**
```lua
-- EVALSHA: atomically check health + promote if healthy
local shard_key = KEYS[1]   -- dj:index:X:shard-map:0
local health_key = KEYS[2]  -- dj:index:X:health:0
local epoch_key = KEYS[3]   -- dj:index:X:shard-0:epoch
local new_primary = ARGV[1]
local expected_epoch = ARGV[2]

local current_epoch = redis.call('GET', epoch_key)
if current_epoch ~= expected_epoch then return 0 end  -- stale

local health = redis.call('HGET', health_key, new_primary)
if health ~= 'healthy' then return 0 end  -- candidate unhealthy

redis.call('SET', epoch_key, current_epoch + 1)
redis.call('HSET', shard_key, 'primary', new_primary)
return 1  -- success
```

**Shard-map update (per-shard key, not per-index):**
```
dj:index:{name}:shard-map:{shard_id}   -- NOT one key for whole index
```
Splits on shard-0 don't block operations on shard-1.

### Event Propagation — Redis Pub/Sub + Debounce

```
Any node mutates state:
  1. Lua script updates Redis
  2. PUBLISH dj:events:{index_name} "shard-0:failover:epoch=11"

All other nodes (subscribed):
  → Debounce: refresh local cache at most once per 1 second
  → Prevents thundering herd on burst events
```

If a node misses Pub/Sub (network blip), it self-heals on next Redis poll (every 1s).

### Lifecycle Checks — Distributed Cron with Jitter

Every node runs lifecycle checks independently:

```
Every 30s (± random 0-15s jitter):
  For each index assigned to this node (round-robin by node_id % num_indices):
    stats = GET dj:index:{name}:lifecycle
    if should_rollover(stats):
      attempt_rollover(name)   // idempotent Lua script
    if should_delete(stats):
      attempt_delete(name)     // idempotent
```

Jitter + round-robin assignment prevents thundering herd. If two nodes race, Lua script ensures exactly one succeeds.

### Failover — No Leader Needed

```
Any node detects writer-0 unhealthy (Health RPC timeout):
  1. Run Lua script: check health + check epoch + promote replica
  2. If script returns 1 → publish event
  3. If script returns 0 → another node already handled it → do nothing
```

---

## 7. Segment Replication — Three Tiers

Per-index configurable. Tier-aware routing prevents consistency surprises.

| Tier | Mechanism | Latency | Cost |
|---|---|---|---|
| **HOT** | Lucene NRT Replication (primary → replica TCP) | <1 second | Free |
| **WARM** | S3 manifest-based push/pull + Redis Pub/Sub | 5-30 seconds | S3 only |
| **FROZEN** | S3 on-demand + local LRU SSD cache | 200-500ms cold, <10ms cached | S3 + SSD |

### Manifest-Based S3 Writes (Atomic)

Lucene segments are multi-file. S3 PUTs are not atomic across keys. Solution:

```
Writer after NRT refresh:
  1. Upload new segment files to s3://jigyasa/{shard}/pending/
  2. Verify all uploads complete (checksum)
  3. Write manifest: s3://jigyasa/{shard}/manifest-{gen}.json
     → lists all segment files + checksums for this generation
  4. PUBLISH generation to Redis

Reader (WARM):
  1. Receive generation notification
  2. Read manifest-{gen}.json from S3
  3. Pull only files listed in manifest (skip already-cached files)
  4. Verify checksums
  5. Reopen IndexReader
```

Partial upload = no manifest = readers never see incomplete state.

### Tier-Aware Routing

Queries include optional consistency hint:

```protobuf
message QueryRequest {
  // ... existing fields ...
  ReplicaTier min_tier = 29;  // HOT (default), WARM, or ANY
}

enum ReplicaTier {
  HOT = 0;     // Only route to hot replicas (<1s lag)
  WARM = 1;    // Allow warm replicas (5-30s lag)
  ANY = 2;     // Allow frozen replicas (minutes-old, on-demand)
}
```

Coordinator checks replica generation against primary generation:
- `lag = primary_gen - replica_gen`
- HOT: lag ≤ 2 generations
- WARM: lag ≤ 100 generations
- Skip replicas that don't meet the requested tier

### Cold Start (Empty Disk)

```
1. New replica starts
2. Read Redis: generation = 504, s3_path = s3://jigyasa/shard-0/
3. Read latest manifest from S3
4. Download all segment files (parallel, ~10s for 10GB)
5. Open IndexReader
6. Connect to replication stream
7. Catch up remaining generations (milliseconds)
8. Ready to serve
```

---

## 8. State Management — Dual Write (Redis + S3)

### Redis = Hot Cache, S3 = Source of Truth

Every state mutation writes to **both** Redis and S3. Redis serves reads (~0.1ms). S3 provides durability.

### Redis Keyspace (Sharded Per Shard, Not Per Index)

```
dj:index:{name}:config                      → strategy, shard_count, replica config
dj:index:{name}:shard-map:{shard_id}        → {primary, hot_replicas, warm_replicas}
dj:index:{name}:shard-{id}:epoch            → monotonic fencing counter
dj:index:{name}:shard-{id}:gen              → latest NRT generation
dj:index:{name}:shard-{id}:stats            → {doc_count, size_bytes, segments}
dj:index:{name}:aliases                     → {write: "current", read: [...]}
dj:index:{name}:lifecycle                   → {state, created_at, rollover_config}
dj:index:{name}:global-stats:{term}         → {df, n, sumTTF} (no TTL, invalidated on write)

Pub/Sub:
  dj:events:{index_name}                    → lifecycle events, generation updates
```

### S3 Metadata (Source of Truth)

```
s3://jigyasa/metadata/
  index/{name}/config.json                  → same as Redis config
  index/{name}/shard-map/{shard_id}.json    → same as Redis shard-map
  index/{name}/aliases.json                 → same as Redis aliases
  index/{name}/lifecycle.json               → same as Redis lifecycle
```

Every Lua script that mutates Redis also triggers an async S3 write (fire-and-forget, retried on failure). On Redis cold start, state is rebuilt from S3.

### Failure Modes

| Scenario | Behavior |
|---|---|
| **Redis available** | Nodes read from Redis, mutate via Lua scripts, propagate via Pub/Sub |
| **Redis temporarily down** | Reads from local cache (stale but functional). Writes buffered locally, replayed on recovery. |
| **Redis data loss** | Rebuild from S3 metadata. All state reconstructed. ~10 seconds for 1000 indices. |
| **S3 down** | Redis serves reads/writes. S3 writes queued. Segment replication paused. |
| **Both down** | Read-only from local cache. Writers continue to local disk. Manual recovery. |

### Graceful Degradation During Redis Outage

- **Reads:** Served from locally-cached shard-map (refreshed every 1s normally)
- **Writes:** Continue to known primaries. Writers accept writes without epoch validation (best-effort). Writes include local-epoch in WAL for reconciliation.
- **Failover:** Paused (no CAS possible). Writers with health-check failure are noted locally, retried when Redis returns.
- **Lifecycle:** Paused. Rollover/deletion deferred.

---

## 9. Scatter-Gather Search

```
QueryRequest arrives at any node:
  │
  ├── Read shard-map from local cache
  │
  ├── If dfs=true and strategy=DOCUMENT:
  │   ├── Check per-term stats cache (Redis)
  │   ├── Cache miss → collect DF/N from shards (parallel)
  │   └── Compute global IDF, attach to query
  │
  ├── Pick one reader per shard:
  │   ├── Filter by min_tier (HOT/WARM/ANY)
  │   └── Rendezvous hashing for cache affinity
  │
  ├── Fan out QueryRequest to N readers (parallel gRPC, timeout)
  │
  ├── Merge:
  │   ├── TopDocs: priority queue by score/sort, take top K
  │   ├── Facets: sum bucket counts per field, re-sort, trim
  │   ├── total_hits: sum across shards
  │   └── SearchAfter: per-shard cursor with epoch + generation
  │
  └── Return merged QueryResponse
```

### SearchAfter Across Shards

Cursor encodes per-shard position + epoch for consistency:

```json
{
  "shards": [
    {"id": 0, "epoch": 11, "gen": 504, "doc_id": 1234, "score": 0.85, "sort": ["2026-04-11"]},
    {"id": 1, "epoch": 8, "gen": 401, "doc_id": 5678, "score": 0.82, "sort": ["2026-04-10"]}
  ]
}
```

If a shard's epoch changed since the cursor was created (failover occurred), the coordinator re-executes that shard from scratch rather than risking inconsistent pagination.

---

## 10. Implementation — Baby Steps

```
Step 0:  Observability foundation            Prometheus client in Jigyasa (3 metrics:
                                              query_duration, index_generation, heap_usage)
                                              Structured JSON logging. Health endpoint.

Step 1:  READ mode periodic refresh          5 lines Java. Enables shared FS.
         + NFS coherency validation          Bash test: writer syncs, reader sees immediately.

Step 2:  Docker Compose proof                Split into:
         2a: 1W + 1R shared volume           Proves R/W separation
         2b: 2W + 2R, 2 shards              Proves multi-shard
         2c: Manual failover test            Kill writer, promote by hand

Step 3:  Redis state library (Go)            Lua scripts for atomic CAS + epoch fencing.
                                              Dual-write to S3 metadata. ~300 lines.

Step 4:  Node — scatter-gather reads         Fan out + merge TopDocs + facets. ~500 lines Go.

Step 5:  Node — write routing                Hash routing + epoch validation. ~200 lines Go.

Step 6:  S3 manifest-based push              Writer sidecar: upload segments → write manifest.
                                              ~100 lines bash + Go.

Step 7:  S3 pull (reader)                    Subscribe Redis Pub/Sub → pull manifest → sync.
                                              Enables warm tier + cold-start bootstrap.

Step 8:  DFS phase                           Global stats with write-invalidation cache.
                                              ~30 lines Java + ~100 lines Go.

Step 9:  Lifecycle                           Rollover, retention, shard splitting (Lua scripts).
                                              ~500 lines Go.

Step 10: Lucene NRT Replication              PrimaryNode/ReplicaNode for hot tier. ~200 lines Java.

Step 11: Helm chart + HPA + chaos test       Production K8s. 24h chaos-mesh before GA.
         + auth/TLS + tracing

Step 12: Frozen tier                         On-demand S3 loading + LRU SSD cache. ~300 lines Go.
```

### Dependency Graph

```
Step 0 (observability) ──────────────────────── always first
  │
Step 1 (READ refresh + NFS test) ─── Step 2a/2b/2c (Docker proof)
  │
Step 3 (Redis + Lua + S3 metadata) ─── Step 4 (scatter-gather)
                                              │
                                         Step 5 (write routing)
                                              │
                                         Step 8 (DFS)
  │
Step 6 (S3 push) ─── Step 7 (S3 pull) ─── removes shared FS dependency
                              │
                         Step 9 (lifecycle)
                              │
                         Step 10 (NRT replication)
                              │
                         Step 11 (Helm + chaos + auth)
                              │
                         Step 12 (frozen tier)
```

---

## 11. Competitive Position

| | Elasticsearch | OpenSearch | Quickwit | **Distributed Jigyasa** |
|---|---|---|---|---|
| Per-shard search p50 | 14ms | ~14ms | ~50ms | **3.6ms** |
| Per-shard facets p50 | 39ms | ~39ms | N/A | **16ms** |
| BM25 correctness | Approximate | Approximate | N/A | **Exact (DFS)** |
| Facet correctness | Approximate | Approximate | Exact | **Exact** |
| Leader election | Raft master | Raft master | Chitchat | **None (epoch fencing)** |
| Split-brain risk | Yes (mitigated) | Yes | Minimal | **Impossible (fenced)** |
| Replica cost | 1x (re-index) | 0.3x (segment) | 0x | **0x** |
| Scale-out time | Minutes | Minutes | Seconds | **Seconds** |
| Shard splitting | Downtime | Downtime | N/A | **Background, near-zero** |
| Storage tiering | Enterprise $$$ | Preview | Native | **Hot/Warm/Frozen** |
| Per-index scaling | No | No | Yes | **Yes** |
| Node roles | Master, Data, Coord | Same | Searcher, Indexer | **All equal** |
| Metadata durability | Cluster state (Raft) | Same | PostgreSQL | **Redis + S3 dual-write** |
| Consistency control | None (eventual) | None | None | **Tier-aware (per-query)** |
| Artifact per node | 587MB | 600MB+ | ~50MB | **40MB** |
| License | SSPL | Apache 2.0 | AGPL | **Apache 2.0** |
