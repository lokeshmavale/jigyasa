# 02 – Query Cookbook

Demonstrates every query type supported by Jigyasa using a 20-product
e-commerce catalog (`../data/products.jsonl`).

Both the Python and Java examples create a **cookbook** collection, index all
products, then run the queries listed below.

## Queries demonstrated

| # | Type | What it shows |
|---|------|---------------|
| 1 | **BM25 text search** | `"wireless headphones"` — ranked full-text retrieval |
| 2 | **Phrase query** | `"cast iron skillet"` with `slop=0` — exact phrase match |
| 3 | **Fuzzy query** | `"headphoness"` (typo) with `max_edits=2` — typo tolerance |
| 4 | **Prefix query** | `"wire"` — autocomplete-style matching |
| 5 | **Query string** | `description:organic AND category:food` — Lucene syntax |
| 6 | **Match-all** | No query, just `top_k` — returns all documents |
| 7 | **Term filter** | `category = "electronics"` — exact value constraint |
| 8 | **Range filter** | `price` between 20 and 100 |
| 9 | **Geo distance filter** | Within 1 000 km of New York City |
| 10 | **Boolean compound filter** | `(category=electronics OR category=sports) AND NOT brand=FlexZone` |
| 11 | **Sort** | By price descending |
| 12 | **Combined** | Text search + filter + sort in one request |
| 13 | **Query timeout** | `timeout_ms=5000` — partial results with deadline, `timed_out` flag |

## Running

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:02-query-cookbook:run

# Python
cd examples/02-query-cookbook/python
pip install grpcio grpcio-tools
python query_cookbook.py
```
