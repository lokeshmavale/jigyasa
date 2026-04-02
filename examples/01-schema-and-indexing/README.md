# Example 01 – Schema Definition & Bulk Indexing

This example demonstrates the foundational operations of the Jigyasa search engine:

1. **Create a collection** with a rich schema covering multiple field types
   (STRING, INT32, DOUBLE, GEO_POINT) and field properties (key, searchable,
   filterable, sortable).
2. **Bulk-index documents** – load 20 products from a shared JSONL dataset in a
   single `Index` RPC call.
3. **Lookup documents** by their primary key using the `Lookup` RPC.
4. **Count documents** – total count and filtered count via the `Count` RPC.
5. **Check collection health** – verify doc count, segment count, and writer /
   searcher status via the `Health` RPC.

## Dataset

The products dataset lives at `../data/products.jsonl` (20 products with fields
such as `id`, `title`, `description`, `category`, `brand`, `price`, `rating`,
`in_stock`, `tags`, and `location`).

## Prerequisites

* A running Jigyasa gRPC server on `localhost:50051`.

## Running the Examples

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:01-schema-and-indexing:run

# Python
cd examples/01-schema-and-indexing/python
pip install grpcio grpcio-tools
python schema_and_indexing.py
```

## Schema overview

| Field       | Type      | Key | Searchable | Filterable | Sortable |
|-------------|-----------|-----|------------|------------|----------|
| id          | STRING    | ✔   | ✔          | ✔          |          |
| title       | STRING    |     | ✔          |            |          |
| description | STRING    |     | ✔          |            |          |
| category    | STRING    |     |            | ✔          |          |
| brand       | STRING    |     |            | ✔          |          |
| price       | DOUBLE    |     |            | ✔          | ✔        |
| rating      | DOUBLE    |     |            | ✔          | ✔        |
| in_stock    | INT32     |     |            | ✔          |          |
| location    | GEO_POINT |     |            |            |          |
