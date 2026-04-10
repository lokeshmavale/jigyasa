# Jigyasa Examples

Learn Jigyasa from zero to production. Each example builds on the previous one.
Every example ships in **both Java and Python** so you can follow along in the language you prefer.

## Prerequisites

- **Java 21+** (for Java examples and running Jigyasa)
- **Python 3.10+** (for Python examples)
- **Docker** (optional, for containerized setup)
- **Python packages** (for Python examples): `pip install grpcio grpcio-tools googleapis-common-protos`

## Examples

| # | Example | What You'll Learn | Time |
|---|---------|-------------------|------|
| [00](00-quickstart/) | **Quickstart** | Start server, health check, index & search | 5 min |
| [01](01-schema-and-indexing/) | **Schema & Indexing** | Typed schemas, bulk indexing, lookup, count | 15 min |
| [02](02-query-cookbook/) | **Query Cookbook** | All 12 query types with working examples | 30 min |
| [03](03-vector-and-hybrid/) | **Vector & Hybrid Search** | KNN similarity, hybrid RRF fusion | 20 min |
| [04](04-configuration/) | **Configuration** | Memory tiers, TTL, durability modes | 15 min |
| [05](05-ops-and-multitenancy/) | **Ops & Multi-tenancy** | Collection lifecycle, tenant isolation, admin | 15 min |
| [06](06-e-commerce-app/) | **E-commerce App** | Full search application with UI | 1 hr |
| [07](07-multi-language-analyzers/) | **Multi-Language Analyzers** | Per-field analyzers, 42 languages, stemming comparison | 20 min |

---

### 00 — Quickstart

Your first five minutes with Jigyasa. Build the JAR, start the server, and run a health check,
create a collection, index 5 documents, and perform a text search — all in a single script.

**Key concepts:** `HealthCheck`, `CreateCollection`, `Index`, `Query`, `Count`

```bash
cd 00-quickstart/python && python quickstart.py
```

---

### 01 — Schema & Indexing

Defines a rich schema with typed fields (STRING, INT32, DOUBLE, GEO_POINT) and field properties
(key, searchable, filterable, sortable). Bulk-indexes 20 e-commerce products from JSONL, then
demonstrates document lookup by key, total/filtered count, and collection health inspection.

**Key concepts:** Schema JSON, field types, `IndexAction.UPDATE`, `Lookup`, `Count` with `FilterClause`

```bash
cd 01-schema-and-indexing/python && python schema_and_indexing.py
```

---

### 02 — Query Cookbook

A complete tour of all 12 query types Jigyasa supports, each with a working example against real data:

1. **BM25 text search** — ranked full-text retrieval
2. **Phrase query** — exact phrase with configurable slop
3. **Fuzzy query** — typo-tolerant search (Levenshtein distance)
4. **Prefix query** — autocomplete-style prefix matching
5. **Query string** — Lucene query syntax with field targeting
6. **Match-all** — retrieve all documents
7. **Term filter** — exact value filtering on keyword fields
8. **Range filter** — numeric/date range filtering
9. **Geo distance filter** — location-based radius search
10. **Boolean compound** — combine filters with AND/OR/NOT
11. **Sort** — order results by any sortable field
12. **Combined** — text + filter + sort in one query

```bash
cd 02-query-cookbook/python && python query_cookbook.py
```

---

### 03 — Vector & Hybrid Search

Sets up an HNSW vector index with 16-dimensional embeddings, then demonstrates:
- **Pure KNN search** — cosine similarity on precomputed embeddings
- **Pure BM25 search** — keyword relevance ranking
- **Hybrid RRF fusion** — combines KNN + BM25 with tunable `text_weight`
- **Filtered vector search** — KNN constrained by metadata filters

**Key concepts:** `VECTOR` field type, `hnswConfig`, `knn_query`, `text_weight`, reciprocal rank fusion

```bash
cd 03-vector-and-hybrid/python && python vector_and_hybrid.py
```

---

### 04 — Configuration

Explores Jigyasa's runtime configuration options:
- **Memory tiers** — SEMANTIC (permanent), EPISODIC (24h TTL), WORKING (5min TTL)
- **Custom TTL overrides** — per-document expiration
- **Refresh policies** — NONE (fire-and-forget), IMMEDIATE (forced flush), WAIT_FOR (default)
- **Recency decay scoring** — boost recent documents with configurable half-life

**Key concepts:** `memory_tier`, `ttl_seconds`, `RefreshPolicy`, `recency_decay_half_life_seconds`

```bash
cd 04-configuration/python && python configuration_demo.py
```

---

### 05 — Ops & Multi-tenancy

Operational workflows for production deployments:
- **Collection lifecycle** — create, close, reopen, delete
- **Health monitoring** — server status, per-collection metrics (doc count, segments, writer state)
- **Multi-tenant isolation** — `tenant_id` on index and query for data isolation
- **Delete by query** — bulk removal with filters
- **Force merge** — segment compaction for read performance

```bash
cd 05-ops-and-multitenancy/python && python ops_and_multitenancy.py
```

---

### 06 — E-commerce App

A complete interactive e-commerce search application with a CLI menu:
1. **Search products** — free-text product search
2. **Browse by category** — filtered listing by product category
3. **Filter by price range** — numeric range queries
4. **Find nearby stores** — geo-distance search within a radius
5. **Advanced search** — text + category filter + price sort combined
6. **Product count** — total and per-category counts
7. **Faceted search** — browse with category and price range facets

Indexes 20 real-looking products with text, numeric, boolean, and geo-point fields.

```bash
cd 06-e-commerce-app/python && python ecommerce_app.py
```

---

### 07 — Multi-Language Analyzers

Demonstrates Jigyasa's **42 built-in analyzers** for multilingual search.
Shows how language-specific stemming dramatically improves recall compared to the default `standard` analyzer:

- **French** — `maison` (singular) matches `maisons` (plural) with `lucene.fr`
- **German** — `Haus` matches `Häuser` (umlaut + plural) with `lucene.de`
- **Hindi** — `वास्तुकला` search with `lucene.hi`
- **CJK** — `建築` bigram tokenization for Japanese/Chinese/Korean with `lucene.cjk`
- **English** — `architectural` matches `architecture`/`architects` with `lucene.en`
- **Keyword** — exact-match only, no tokenization

**Schema usage:**
```json
{
  "name": "title_fr",
  "type": "STRING",
  "searchable": true,
  "indexAnalyzer": "lucene.fr",
  "searchAnalyzer": "lucene.fr"
}
```

**All 42 analyzers:** `standard`, `simple`, `keyword`, `whitespace`, plus 38 language analyzers —
`lucene.en`, `lucene.fr`, `lucene.de`, `lucene.es`, `lucene.it`, `lucene.pt`, `lucene.nl`,
`lucene.ru`, `lucene.ar`, `lucene.hi`, `lucene.cjk`, `lucene.pl`, `lucene.tr`, `lucene.th`,
`lucene.uk`, `lucene.sv`, `lucene.no`, `lucene.da`, `lucene.fi`, `lucene.hu`, `lucene.ro`,
`lucene.bg`, `lucene.cs`, `lucene.el`, `lucene.id`, `lucene.lv`, `lucene.lt`, `lucene.fa`,
`lucene.sr`, `lucene.ca`, `lucene.gl`, `lucene.eu`, `lucene.hy`, `lucene.ga`, `lucene.bn`,
`lucene.br`, `lucene.et`, `lucene.ckb`

```bash
cd 07-multi-language-analyzers/python && python multi_language_analyzers.py
```

---

## Quick Setup

```bash
# 1. Start Jigyasa (from repo root)
./gradlew run                        # starts server on localhost:50051

# 2. Run a Java example (from repo root, in a separate terminal)
./gradlew :examples:00-quickstart:run
./gradlew :examples:01-schema-and-indexing:run
./gradlew :examples:02-query-cookbook:run
./gradlew :examples:03-vector-and-hybrid:run
./gradlew :examples:04-configuration:run
./gradlew :examples:05-ops-and-multitenancy:run
./gradlew :examples:06-e-commerce-app:run
./gradlew :examples:07-multi-language-analyzers:run

# 3. Or run a Python example
pip install grpcio grpcio-tools googleapis-common-protos
cd examples/00-quickstart/python
python quickstart.py
```

> **Tip:** All Java examples are registered as Gradle subprojects. Run `./gradlew tasks --all | grep ":examples"` to see all available tasks.

## Shared Datasets

All examples use datasets from [`data/`](data/):

| File | Records | Description |
|------|---------|-------------|
| `products.jsonl` | 20 | E-commerce products with text, numeric, and geo fields |
| `articles-with-embeddings.jsonl` | 10 | Articles with 16-dim precomputed embeddings |
