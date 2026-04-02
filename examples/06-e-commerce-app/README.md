# Example 06: E-Commerce Product Search

A full working e-commerce product search application powered by Jigyasa. Demonstrates
text search, category browsing, price filtering, geo-based store lookup, and advanced
multi-criteria queries against a catalog of 20 products.

## Prerequisites

- Jigyasa gRPC server running on `localhost:50051`
- Product data in `../data/products.jsonl`

## Running

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:06-e-commerce-app:run

# Python
cd examples/06-e-commerce-app/python
pip install grpcio grpcio-tools
python ecommerce_app.py
```

## Python (Interactive CLI)

### Menu

```
===== E-Commerce Product Search =====
[1] Search products
[2] Browse by category
[3] Filter by price range
[4] Find nearby stores
[5] Advanced search
[6] Product count
[0] Exit
======================================
```

### Example session

```
Choice: 1
Search query: wireless headphones

  #1  Wireless Noise-Cancelling Headphones  ($149.99)  ★ 4.7
      Premium over-ear headphones with active noise cancellation...

  #2  Bluetooth Sport Earbuds  ($79.99)  ★ 4.3
      Sweat-resistant wireless earbuds for workouts...

Choice: 3
Min price: 50
Max price: 100

  Found 6 products between $50.00 and $100.00
  ...

Choice: 4
Latitude: 37.7749
Longitude: -122.4194
Radius (meters) [5000]: 10000

  Stores within 10.0 km of (37.7749, -122.4194):
  ...
```

## Java (Non-Interactive Demo)

Runs all search scenarios sequentially and prints results:

```
=== E-Commerce Search Demo (Java) ===

--- Indexing 20 products ---
Done.

--- Scenario 1: Text Search for "laptop" ---
  ...
--- Scenario 2: Browse by Category "Electronics" ---
  ...
--- Scenario 3: Price Range $25 - $75 ---
  ...
--- Scenario 4: Nearby Stores (geo search) ---
  ...
--- Scenario 5: Advanced Search ---
  ...
--- Scenario 6: Product Count ---
  Total products: 20
```

## What This Example Demonstrates

| Feature | API Used |
|---|---|
| Collection creation | `CreateCollection` with typed schema |
| Bulk indexing | `Index` with batch of `IndexItem` |
| Full-text search | `Query` with `text_query` |
| Term filtering | `FilterClause` with `TermFilter` |
| Range filtering | `FilterClause` with `RangeFilter` |
| Geo-distance search | `FilterClause` with `GeoDistanceFilter` |
| Compound filters | `CompoundFilter` with AND/OR operators |
| Sorting | `SortClause` on price, rating |
| Pagination | `top_k` and `offset` |
| Document count | `Count` with optional filters |
