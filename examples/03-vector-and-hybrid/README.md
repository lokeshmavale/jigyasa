# 03 — Vector & Hybrid Search

Demonstrates KNN vector search, BM25 text search, and hybrid (RRF fusion)
search on the same collection using 16-dimensional embeddings.

## What This Example Shows

| Step | Description |
|------|-------------|
| 1 | Create a collection with a `VECTOR` field and HNSW config |
| 2 | Index 10 articles with pre-computed 16-dim embeddings |
| 3 | **Pure KNN** vector search — find articles similar to a query embedding |
| 4 | **Pure BM25** text search on the same data for comparison |
| 5 | **Hybrid search** (`text_weight=0.5`) — RRF fusion of text + vector |
| 6 | Hybrid with different weights (`0.3` vs `0.7`) to show the effect |
| 7 | Vector search **with a filter** (`category="ai"`) |

## Data

Uses `../data/articles-with-embeddings.jsonl` — 10 articles with fields:
`id`, `title`, `content`, `author`, `category`, `embedding` (16-dim float vector).

## Prerequisites

Start Jigyasa on **localhost:50051** (see `00-quickstart`).

## Running

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:03-vector-and-hybrid:run

# Python
cd examples/03-vector-and-hybrid/python
pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos
python vector_and_hybrid.py
```
