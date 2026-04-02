# 04 — Configuration: Memory Tiers, TTL & Recency Decay

**Time:** ~15 minutes | **Prerequisites:** [00-quickstart](../00-quickstart/)

This example demonstrates how Jigyasa's configuration knobs control document
lifecycle, search freshness, and indexing durability.

---

## Concepts

### Memory Tiers

Every indexed document belongs to a **memory tier** that determines its default
time-to-live:

| Tier | Enum Value | Default TTL | Use Case |
|------|-----------|-------------|----------|
| **SEMANTIC** | `0` | ∞ (permanent) | Facts, user preferences, knowledge base |
| **EPISODIC** | `1` | 24 hours | Conversation history, session context |
| **WORKING** | `2` | 5 minutes | Scratchpad, transient computation state |

Set the tier on each `IndexItem.memory_tier`. The TTL sweeper automatically
removes expired documents in the background.

### Custom TTL Override

Set `IndexItem.ttl_seconds` to a non-zero value to override the tier default.
For example, an EPISODIC document with `ttl_seconds=3600` expires after 1 hour
instead of 24 hours.

When `ttl_seconds=0` (the default), the tier's built-in TTL applies.

### Enabling the TTL Sweeper

The collection schema must include `"ttlEnabled": true` for TTL expiration to
be active:

```json
{
  "ttlEnabled": true,
  "fields": [ ... ]
}
```

Without this flag, documents in EPISODIC and WORKING tiers are stored but never
automatically swept.

### Recency Decay

`QueryRequest.recency_decay` applies an exponential time-based boost so that
newer documents score higher. Configure the decay curve with `half_life_seconds`:

| `half_life_seconds` | Behaviour |
|---------------------|-----------|
| `3600` (default) | A document indexed 1 hour ago gets 0.5× boost |
| `300` | Aggressive — 5-minute half-life |
| `86400` | Gentle — 1-day half-life |
| `0` | Disabled (no recency boost) |

### Refresh Policies

Control when newly indexed documents become searchable:

| Policy | Enum | Behaviour |
|--------|------|-----------|
| **WAIT_FOR** | `0` | Block until docs are searchable (default, consistent reads) |
| **NONE** | `1` | Fire-and-forget — best throughput for bulk loads |
| **IMMEDIATE** | `2` | Force segment flush — highest consistency, lowest throughput |

---

## Running the Examples

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:04-configuration:run

# Python
cd examples/04-configuration/python
pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos
python configuration_demo.py
```

---

## Server-Side Environment Variables

These are set on the **Jigyasa server process**, not via gRPC. Documented here
for reference when tuning a deployment.

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_SERVER_PORT` | `50051` | Port the gRPC server listens on |
| `INDEX_CACHE_DIR` | `./data/index` | Root directory for index segments |
| `TRANSLOG_DIRECTORY` | `./data/translog` | Write-ahead log directory |
| `SERVER_MODE` | `standalone` | Deployment mode (`standalone`, `cluster`) |
| `TRANSLOG_DURABILITY` | `REQUEST` | Translog fsync policy — `REQUEST` (fsync per request, safest) or `ASYNC` (fsync on interval, faster) |
| `TRANSLOG_SYNC_INTERVAL_MS` | `5000` | Interval for async translog fsync (only when `ASYNC`) |
| `NRT_REFRESH_INTERVAL_MS` | `1000` | Near-real-time refresh interval in milliseconds |
| `TTL_SWEEP_INTERVAL_MS` | `60000` | How often the TTL sweeper checks for expired docs |
| `MERGE_POLICY_MAX_SEGMENTS` | `10` | Maximum segments before a merge is triggered |

### Durability vs. Throughput Trade-offs

- **Safest:** `TRANSLOG_DURABILITY=REQUEST` — every index call is fsync'd
  before acknowledgement. Zero data loss on crash.
- **Fastest:** `TRANSLOG_DURABILITY=ASYNC` with a higher
  `TRANSLOG_SYNC_INTERVAL_MS` — risk losing up to one interval of data on
  crash, but much higher write throughput.
- **Refresh:** Lower `NRT_REFRESH_INTERVAL_MS` makes documents searchable
  faster but increases I/O. The `IMMEDIATE` refresh policy bypasses this
  interval entirely.

---

## What You'll Learn

1. Creating a TTL-enabled collection
2. Indexing documents across SEMANTIC, EPISODIC, and WORKING tiers
3. Overriding default TTL with `ttl_seconds`
4. Using all three refresh policies
5. Querying with recency decay to boost recent documents
6. Understanding server-side tuning knobs
