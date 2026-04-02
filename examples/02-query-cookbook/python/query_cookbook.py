#!/usr/bin/env python3
"""Query Cookbook — demonstrates every Jigyasa query type."""

import json, os, sys, subprocess, time

# ── Stub generation ──────────────────────────────────────────────────────────
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)

import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
import site
site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]

subprocess.run([sys.executable, "-m", "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}", f"--proto_path={grpc_include}", f"--proto_path={site_pkgs}",
    f"--python_out={GEN_DIR}", f"--grpc_python_out={GEN_DIR}", "dpSearch.proto"
], capture_output=True, text=True, check=True)

sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

import grpc

ADDR = "localhost:50051"
COLLECTION = "cookbook"
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "products.jsonl")

# ── Helpers ──────────────────────────────────────────────────────────────────

def banner(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_hits(resp):
    print(f"  Total hits: {resp.total_hits} (exact={resp.total_hits_exact})")
    for i, hit in enumerate(resp.hits, 1):
        if hit.source:
            doc = json.loads(hit.source)
            print(f"  {i}. [{hit.score:.4f}] {doc.get('title', hit.doc_id)}  "
                  f"(${doc.get('price', '?')}  ★{doc.get('rating', '?')})")
        else:
            print(f"  {i}. [{hit.score:.4f}] doc_id={hit.doc_id}")

# ── Setup: create collection & index data ────────────────────────────────────

def setup(stub):
    banner("Setup — create collection & index products")

    schema = json.dumps({"fields": [
        {"name": "id",          "type": "STRING",    "key": True, "filterable": True},
        {"name": "title",       "type": "STRING",    "searchable": True, "sortable": True},
        {"name": "description", "type": "STRING",    "searchable": True},
        {"name": "category",    "type": "STRING",    "filterable": True, "sortable": True},
        {"name": "brand",       "type": "STRING",    "filterable": True},
        {"name": "price",       "type": "DOUBLE",    "filterable": True, "sortable": True},
        {"name": "rating",      "type": "DOUBLE",    "filterable": True, "sortable": True},
        {"name": "in_stock",    "type": "INT32",     "filterable": True},
        {"name": "location",    "type": "GEO_POINT", "filterable": True},
    ]})

    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=schema))
        print(f"  Created collection '{COLLECTION}'")
    except grpc.RpcError as e:
        if "already exists" in str(e):
            print(f"  Collection '{COLLECTION}' already exists — reusing")
        else:
            raise

    # Load products and convert in_stock bool → int
    with open(DATA_FILE) as f:
        docs = [json.loads(line) for line in f if line.strip()]
    for d in docs:
        d["in_stock"] = 1 if d.get("in_stock") else 0

    items = [pb.IndexItem(document=json.dumps(d)) for d in docs]
    resp = stub.Index(pb.IndexRequest(item=items, collection=COLLECTION))
    ok = sum(1 for r in resp.itemResponse if r.code == 0)
    print(f"  Indexed {len(items)} products  (succeeded={ok})")
    time.sleep(1)  # wait for NRT refresh

# ── 1. BM25 text search ─────────────────────────────────────────────────────

def query_bm25(stub):
    banner("1. BM25 Text Search — \"wireless headphones\"")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="wireless headphones",
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── 2. Phrase query ──────────────────────────────────────────────────────────

def query_phrase(stub):
    banner("2. Phrase Query — \"cast iron skillet\" (slop=0)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        phrase_query="cast iron skillet",
        phrase_field="title",
        phrase_slop=0,
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── 3. Fuzzy query ───────────────────────────────────────────────────────────

def query_fuzzy(stub):
    banner("3. Fuzzy Query — \"headphoness\" (typo, max_edits=2)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        fuzzy_query="headphoness",
        fuzzy_field="title",
        max_edits=2,
        prefix_length=0,
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── 4. Prefix query ─────────────────────────────────────────────────────────

def query_prefix(stub):
    banner("4. Prefix Query — \"wire\" (autocomplete)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        prefix_query="wire",
        prefix_field="title",
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── 5. Query string ─────────────────────────────────────────────────────────

def query_string(stub):
    banner("5. Query String — \"description:organic AND description:natural\"")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        query_string="description:organic AND description:natural",
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── 6. Match-all ─────────────────────────────────────────────────────────────

def query_match_all(stub):
    banner("6. Match-All (no query, top_k=20)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        top_k=20,
        include_source=True,
    ))
    print_hits(resp)

# ── 7. Term filter ───────────────────────────────────────────────────────────

def query_term_filter(stub):
    banner("7. Term Filter — category=\"electronics\"")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="category",
            term_filter=pb.TermFilter(value="electronics"),
        )],
        top_k=10,
        include_source=True,
    ))
    print_hits(resp)

# ── 8. Range filter ──────────────────────────────────────────────────────────

def query_range_filter(stub):
    banner("8. Range Filter — price between $20 and $100")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="price",
            range_filter=pb.RangeFilter(min="20", max="100"),
        )],
        top_k=10,
        include_source=True,
    ))
    print_hits(resp)

# ── 9. Geo distance filter ───────────────────────────────────────────────────

def query_geo_distance(stub):
    banner("9. Geo Distance Filter — within 1000km of NYC (40.7128, -74.0060)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            field="location",
            geo_distance_filter=pb.GeoDistanceFilter(
                lat=40.7128, lon=-74.0060, distance_meters=1_000_000,
            ),
        )],
        top_k=10,
        include_source=True,
    ))
    print_hits(resp)

# ── 10. Boolean compound filter ──────────────────────────────────────────────

def query_compound_filter(stub):
    banner("10. Boolean Compound — (electronics OR sports) AND NOT brand=FlexZone")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(
            compound_filter=pb.CompoundFilter(
                operator=pb.CompoundFilter.AND,
                should=[
                    pb.FilterClause(field="category",
                                    term_filter=pb.TermFilter(value="electronics")),
                    pb.FilterClause(field="category",
                                    term_filter=pb.TermFilter(value="sports")),
                ],
                must_not=[
                    pb.FilterClause(field="brand",
                                    term_filter=pb.TermFilter(value="FlexZone")),
                ],
            ),
        )],
        top_k=10,
        include_source=True,
    ))
    print_hits(resp)

# ── 11. Sort ──────────────────────────────────────────────────────────────────

def query_sort(stub):
    banner("11. Sort — by price descending")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        sort=[pb.SortClause(field="price", descending=True)],
        top_k=10,
        include_source=True,
    ))
    print_hits(resp)

# ── 12. Combined ──────────────────────────────────────────────────────────────

def query_combined(stub):
    banner("12. Combined — text + filter + sort")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="premium quality",
        filters=[pb.FilterClause(
            field="price",
            range_filter=pb.RangeFilter(min="50", max="300"),
        )],
        sort=[pb.SortClause(field="rating", descending=True)],
        top_k=5,
        include_source=True,
    ))
    print_hits(resp)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    setup(stub)

    query_bm25(stub)
    query_phrase(stub)
    query_fuzzy(stub)
    query_prefix(stub)
    query_string(stub)
    query_match_all(stub)
    query_term_filter(stub)
    query_range_filter(stub)
    query_geo_distance(stub)
    query_compound_filter(stub)
    query_sort(stub)
    query_combined(stub)

    print(f"\n{'='*60}")
    print("  All 12 queries completed!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
