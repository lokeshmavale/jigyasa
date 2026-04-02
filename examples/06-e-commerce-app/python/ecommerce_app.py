#!/usr/bin/env python3
"""Interactive e-commerce product search CLI powered by Jigyasa."""

import json
import os
import site
import subprocess
import sys

# ---------------------------------------------------------------------------
# gRPC stub generation
# ---------------------------------------------------------------------------
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)

import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]

subprocess.run([
    sys.executable, "-m", "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}", f"--proto_path={grpc_include}", f"--proto_path={site_pkgs}",
    f"--python_out={GEN_DIR}", f"--grpc_python_out={GEN_DIR}", "dpSearch.proto"
], capture_output=True, text=True, check=True)

sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

import grpc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COLLECTION = "shop"
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "products.jsonl")
SERVER = "localhost:50051"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_products():
    products = []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                products.append(json.loads(line))
    return products


def build_schema():
    schema = {
        "fields": [
            {"name": "id", "type": "STRING", "key": True, "filterable": True},
            {"name": "title", "type": "STRING", "searchable": True, "sortable": True},
            {"name": "description", "type": "STRING", "searchable": True},
            {"name": "category", "type": "STRING", "filterable": True, "sortable": True},
            {"name": "brand", "type": "STRING", "filterable": True},
            {"name": "price", "type": "DOUBLE", "filterable": True, "sortable": True},
            {"name": "rating", "type": "DOUBLE", "filterable": True, "sortable": True},
            {"name": "in_stock", "type": "INT32", "filterable": True},
            {"name": "location", "type": "GEO_POINT", "filterable": True},
        ]
    }
    return json.dumps(schema)


def format_product(hit, index):
    src = json.loads(hit.source) if hit.source else {}
    title = src.get("title", "N/A")
    price = src.get("price", 0)
    rating = src.get("rating", 0)
    desc = src.get("description", "")
    category = src.get("category", "")
    brand = src.get("brand", "")
    in_stock = src.get("in_stock", False)
    stock_label = "In Stock" if in_stock else "Out of Stock"

    print(f"  #{index:<3} {title}  (${price:.2f})  \u2605 {rating}")
    if brand:
        print(f"       Brand: {brand}  |  Category: {category}  |  {stock_label}")
    if desc:
        print(f"       {desc[:100]}{'...' if len(desc) > 100 else ''}")
    print()


def display_results(response):
    if not response.hits:
        print("\n  No results found.\n")
        return
    print(f"\n  Found {len(response.hits)} result(s):\n")
    for i, hit in enumerate(response.hits, 1):
        format_product(hit, i)


# ---------------------------------------------------------------------------
# Setup: create collection and index products
# ---------------------------------------------------------------------------
def setup(stub):
    print("Creating collection and indexing products...")

    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION,
            indexSchema=build_schema(),
        ))
        print(f"  Collection '{COLLECTION}' created.")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()).lower():
            print(f"  Collection '{COLLECTION}' already exists, reusing.")
        else:
            raise

    products = load_products()
    for p in products:
        p["in_stock"] = 1 if p.get("in_stock") else 0
    items = [pb.IndexItem(document=json.dumps(p)) for p in products]
    stub.Index(pb.IndexRequest(item=items, collection=COLLECTION))
    print(f"  Indexed {len(products)} products.\n")


# ---------------------------------------------------------------------------
# Menu actions
# ---------------------------------------------------------------------------
def search_products(stub):
    query = input("  Search query: ").strip()
    if not query:
        return
    resp = stub.Query(pb.QueryRequest(
        text_query=query,
        collection=COLLECTION,
        top_k=10,
        include_source=True,
    ))
    display_results(resp)


def browse_by_category(stub):
    # Retrieve all products to extract distinct categories
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        top_k=100,
        include_source=True,
        source_fields=["category"],
    ))
    categories = sorted({json.loads(h.source).get("category", "") for h in resp.hits if h.source})
    if not categories:
        print("\n  No categories found.\n")
        return

    print("\n  Categories:")
    for i, cat in enumerate(categories, 1):
        print(f"    [{i}] {cat}")
    choice = input("  Select category number: ").strip()
    try:
        selected = categories[int(choice) - 1]
    except (ValueError, IndexError):
        print("  Invalid selection.")
        return

    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="category",
            term_filter=pb.TermFilter(value=selected),
        )],
        sort=[pb.SortClause(field="rating", descending=True)],
        top_k=20,
        include_source=True,
    ))
    print(f"\n  Products in '{selected}' (sorted by rating):")
    display_results(resp)


def filter_by_price(stub):
    min_price = input("  Min price: ").strip()
    max_price = input("  Max price: ").strip()
    if not min_price or not max_price:
        print("  Both min and max price are required.")
        return

    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="price",
            range_filter=pb.RangeFilter(min=min_price, max=max_price),
        )],
        sort=[pb.SortClause(field="price", descending=False)],
        top_k=20,
        include_source=True,
    ))
    print(f"\n  Products between ${float(min_price):.2f} and ${float(max_price):.2f}:")
    display_results(resp)


def find_nearby(stub):
    lat = input("  Latitude: ").strip()
    lon = input("  Longitude: ").strip()
    radius = input("  Radius in meters [5000]: ").strip() or "5000"
    if not lat or not lon:
        print("  Latitude and longitude are required.")
        return

    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="location",
            geo_distance_filter=pb.GeoDistanceFilter(
                lat=float(lat),
                lon=float(lon),
                distance_meters=float(radius),
            ),
        )],
        top_k=20,
        include_source=True,
    ))
    print(f"\n  Stores within {float(radius)/1000:.1f} km of ({lat}, {lon}):")
    display_results(resp)


def advanced_search(stub):
    query = input("  Search text (optional): ").strip()
    category = input("  Category filter (optional): ").strip()
    min_price = input("  Min price (optional): ").strip()
    max_price = input("  Max price (optional): ").strip()
    sort_field = input("  Sort by [price/rating]: ").strip() or "price"
    sort_desc = input("  Descending? [y/N]: ").strip().lower() == "y"

    filters = []
    if category:
        filters.append(pb.FilterClause(
            field="category",
            term_filter=pb.TermFilter(value=category),
        ))
    if min_price or max_price:
        rf = pb.RangeFilter()
        if min_price:
            rf.min = min_price
        if max_price:
            rf.max = max_price
        filters.append(pb.FilterClause(field="price", range_filter=rf))

    # Combine with AND if multiple filters
    final_filters = filters
    if len(filters) > 1:
        final_filters = [pb.FilterClause(
            compound_filter=pb.CompoundFilter(
                operator=pb.CompoundFilter.AND,
                must=filters,
            ),
        )]

    resp = stub.Query(pb.QueryRequest(
        text_query=query if query else "",
        collection=COLLECTION,
        filters=final_filters,
        sort=[pb.SortClause(field=sort_field, descending=sort_desc)],
        top_k=20,
        include_source=True,
    ))
    display_results(resp)


def product_count(stub):
    # Total count
    total = stub.Count(pb.CountRequest(collection=COLLECTION))
    print(f"\n  Total products: {total.count}")

    # Count by category — ask user if they want filtered count
    category = input("  Count by category (optional, press Enter to skip): ").strip()
    if category:
        filtered = stub.Count(pb.CountRequest(
            collection=COLLECTION,
            filters=[pb.FilterClause(
                field="category",
                term_filter=pb.TermFilter(value=category),
            )],
        ))
        print(f"  Products in '{category}': {filtered.count}")
    print()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
MENU = """
===== E-Commerce Product Search =====
[1] Search products
[2] Browse by category
[3] Filter by price range
[4] Find nearby stores
[5] Advanced search
[6] Product count
[0] Exit
======================================"""


def main():
    channel = grpc.insecure_channel(SERVER)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    setup(stub)

    actions = {
        "1": search_products,
        "2": browse_by_category,
        "3": filter_by_price,
        "4": find_nearby,
        "5": advanced_search,
        "6": product_count,
    }

    while True:
        print(MENU)
        choice = input("Choice: ").strip()
        if choice == "0":
            print("Goodbye!")
            break
        action = actions.get(choice)
        if action:
            try:
                action(stub)
            except grpc.RpcError as e:
                print(f"  gRPC error: {e.details()}")
            except Exception as e:
                print(f"  Error: {e}")
        else:
            print("  Invalid choice.")

    channel.close()


if __name__ == "__main__":
    main()
