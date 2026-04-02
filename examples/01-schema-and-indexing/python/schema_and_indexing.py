#!/usr/bin/env python3
"""
Example 01 – Schema Definition & Bulk Indexing (Python)

Demonstrates:
  1. Creating a collection with a rich schema
  2. Bulk-indexing 20 products from a JSONL file
  3. Looking up documents by key
  4. Counting documents (total and filtered)
  5. Checking collection health
"""

import json
import os
import subprocess
import sys
import site
import time

# ---------------------------------------------------------------------------
# 1. Generate gRPC stubs from the proto definition
# ---------------------------------------------------------------------------
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)

import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")

site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]

subprocess.run(
    [
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--proto_path={grpc_include}",
        f"--proto_path={site_pkgs}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        "dpSearch.proto",
    ],
    capture_output=True,
    text=True,
    check=True,
)

sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

import grpc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COLLECTION = "products_01"
SERVER = "localhost:50051"
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "products.jsonl")

# ---------------------------------------------------------------------------
# Schema – defines every field's type and properties
# ---------------------------------------------------------------------------
SCHEMA = {
    "fields": [
        {"name": "id",          "type": "STRING",    "key": True, "filterable": True},
        {"name": "title",       "type": "STRING",    "searchable": True},
        {"name": "description", "type": "STRING",    "searchable": True},
        {"name": "category",    "type": "STRING",    "filterable": True},
        {"name": "brand",       "type": "STRING",    "filterable": True},
        {"name": "price",       "type": "DOUBLE",    "filterable": True, "sortable": True},
        {"name": "rating",      "type": "DOUBLE",    "filterable": True, "sortable": True},
        {"name": "in_stock",    "type": "INT32",     "filterable": True},
        {"name": "location",    "type": "GEO_POINT", "filterable": True},
    ]
}


def load_products(path: str) -> list[dict]:
    """Read every line of the JSONL file into a list of dicts."""
    products = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                products.append(json.loads(line))
    return products


def make_index_item(product: dict) -> pb.IndexItem:
    """Convert a product dict into an IndexItem for the Index RPC."""
    # Convert boolean in_stock to int (1/0) for the INT32 field
    doc = dict(product)
    doc["in_stock"] = 1 if doc.get("in_stock") else 0
    return pb.IndexItem(
        document=json.dumps(doc),
        action=pb.IndexAction.UPDATE,
    )


def main():
    channel = grpc.insecure_channel(SERVER)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # ------------------------------------------------------------------
    # Step 1 – Create collection with schema
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Step 1: Create collection with schema")
    print("=" * 60)

    schema_json = json.dumps(SCHEMA)
    create_req = pb.CreateCollectionRequest(
        collection=COLLECTION,
        indexSchema=schema_json,
    )
    try:
        create_resp = stub.CreateCollection(create_req)
        print(f"  Collection '{COLLECTION}' created successfully.")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            print(f"  Collection '{COLLECTION}' already exists – continuing.")
        else:
            raise
    print()

    # ------------------------------------------------------------------
    # Step 2 – Bulk-index all products
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Step 2: Bulk-index products from JSONL")
    print("=" * 60)

    products = load_products(DATA_FILE)
    print(f"  Loaded {len(products)} products from {os.path.basename(DATA_FILE)}")

    items = [make_index_item(p) for p in products]
    index_req = pb.IndexRequest(
        item=items,
        collection=COLLECTION,
        refresh=pb.IMMEDIATE,
    )
    index_resp = stub.Index(index_req)
    print(f"  Indexed {len(items)} documents (refresh=IMMEDIATE).")
    print()

    # Brief pause to let the index settle
    time.sleep(1)

    # ------------------------------------------------------------------
    # Step 3 – Lookup documents by key
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Step 3: Lookup documents by key")
    print("=" * 60)

    lookup_keys = ["p1", "p10", "p20"]
    lookup_req = pb.LookupRequest(
        docKeys=lookup_keys,
        collection=COLLECTION,
    )
    lookup_resp = stub.Lookup(lookup_req)
    for doc_str in lookup_resp.documents:
        doc = json.loads(doc_str)
        print(f"  [{doc['id']}] {doc['title']}  –  ${doc['price']}")
    print()

    # ------------------------------------------------------------------
    # Step 4 – Count documents (total and filtered)
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Step 4: Count documents")
    print("=" * 60)

    # Total count
    count_req = pb.CountRequest(collection=COLLECTION)
    count_resp = stub.Count(count_req)
    print(f"  Total documents: {count_resp.count}")

    # Filtered count – only electronics
    filter_req = pb.CountRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="category",
            term_filter=pb.TermFilter(value="electronics"),
        )],
    )
    filter_resp = stub.Count(filter_req)
    print(f"  Electronics:     {filter_resp.count}")
    print()

    # ------------------------------------------------------------------
    # Step 5 – Collection health check
    # ------------------------------------------------------------------
    print("=" * 60)
    print("Step 5: Collection health")
    print("=" * 60)

    health_req = pb.HealthRequest()
    health_resp = stub.Health(health_req)
    for coll in health_resp.collections:
        if coll.name == COLLECTION:
            print(f"  Collection:         {coll.name}")
            print(f"  Writer open:        {coll.writer_open}")
            print(f"  Searcher available: {coll.searcher_available}")
            print(f"  Doc count:          {coll.doc_count}")
            print(f"  Segment count:      {coll.segment_count}")
            break
    else:
        print(f"  Collection '{COLLECTION}' not found in health response.")
    print()

    print("Done ✓")
    channel.close()


if __name__ == "__main__":
    main()
