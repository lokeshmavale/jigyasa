"""
Jigyasa Example 03 — Vector & Hybrid Search.

Demonstrates pure KNN, pure BM25, and hybrid (RRF fusion) search
on a collection with 16-dimensional embeddings.

Prerequisites:
    pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos

Usage:
    python vector_and_hybrid.py
"""
import grpc
import json
import subprocess
import sys
import os
import time

ADDR = "localhost:50051"
COLLECTION = "vector-hybrid-demo"
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "articles-with-embeddings.jsonl")

# Query vector: slightly perturbed version of article a4 (LLM agents memory)
QUERY_VECTOR = [
    0.44, -0.66, 0.90, 0.22, -0.35, 0.55, -0.77, 0.13,
    0.92, -0.44, 0.66, -0.88, 0.24, 0.35, -0.55, 0.77,
]


def generate_stubs():
    os.makedirs(GEN_DIR, exist_ok=True)
    import grpc_tools
    grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
    import site
    site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]
    result = subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--proto_path={grpc_include}",
        f"--proto_path={site_pkgs}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        "dpSearch.proto"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Proto compilation failed: {result.stderr}")
        sys.exit(1)
    sys.path.insert(0, GEN_DIR)


def print_hits(resp, label=""):
    if label:
        print(f"   --- {label} ---")
    print(f"   Total hits: {resp.total_hits}")
    for i, hit in enumerate(resp.hits):
        doc = json.loads(hit.source)
        print(f"   {i+1}. [{hit.score:.4f}] {doc['id']:>3s} | {doc['title']}")
    print()


def main():
    generate_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # ── 1. Create collection with vector field ───────────────────────
    print("1. Create collection with VECTOR field + HNSW config")
    schema = json.dumps({
        "fields": [
            {"name": "id",        "type": "STRING", "key": True, "filterable": True},
            {"name": "title",     "type": "STRING", "searchable": True},
            {"name": "content",   "type": "STRING", "searchable": True},
            {"name": "author",    "type": "STRING", "filterable": True},
            {"name": "category",  "type": "STRING", "filterable": True},
            {"name": "published_at", "type": "STRING", "filterable": True},
            {"name": "embedding", "type": "VECTOR", "dimensions": 16},
        ],
        "hnswConfig": {"maxConn": 16, "beamWidth": 100},
    })
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=schema))
        print("   ✓ Created\n")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("   ✓ Already exists\n")
        else:
            raise

    # ── 2. Index documents from JSONL ────────────────────────────────
    print("2. Index articles with embeddings")
    with open(DATA_FILE) as f:
        docs = [json.loads(line) for line in f if line.strip()]
    items = [pb.IndexItem(document=json.dumps(d)) for d in docs]
    resp = stub.Index(pb.IndexRequest(item=items, collection=COLLECTION))
    print(f"   ✓ Indexed {len(resp.itemResponse)} documents\n")

    time.sleep(1)

    # ── 3. Pure KNN vector search ────────────────────────────────────
    print("3. Pure KNN vector search (query ≈ article a4 embedding)")
    vq = pb.VectorQuery(field="embedding", vector=QUERY_VECTOR, k=5)
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        vector_query=vq,
        include_source=True,
        top_k=5,
    ))
    print_hits(resp, "Vector-only results")

    # ── 4. Pure text search (BM25) ───────────────────────────────────
    print("4. Pure BM25 text search for 'LLM agents memory planning'")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="LLM agents memory planning",
        include_source=True,
        top_k=5,
    ))
    print_hits(resp, "Text-only (BM25) results")

    # ── 5. Hybrid search (text_weight=0.5) ───────────────────────────
    print("5. Hybrid search: text_weight=0.5 (balanced RRF fusion)")
    vq = pb.VectorQuery(field="embedding", vector=QUERY_VECTOR, k=5)
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="LLM agents memory planning",
        vector_query=vq,
        text_weight=0.5,
        include_source=True,
        top_k=5,
    ))
    print_hits(resp, "Hybrid (0.5) results")

    # ── 6. Compare different text_weight values ──────────────────────
    print("6. Hybrid with different text_weight values")
    for weight in [0.3, 0.7]:
        vq = pb.VectorQuery(field="embedding", vector=QUERY_VECTOR, k=5)
        resp = stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            text_query="LLM agents memory planning",
            vector_query=vq,
            text_weight=weight,
            include_source=True,
            top_k=5,
        ))
        print_hits(resp, f"text_weight={weight} ({'more vector' if weight < 0.5 else 'more text'})")

    # ── 7. Vector search with filter ─────────────────────────────────
    print("7. Vector search + filter (category='ai')")
    vq = pb.VectorQuery(field="embedding", vector=QUERY_VECTOR, k=5)
    cat_filter = pb.FilterClause(
        field="category",
        term_filter=pb.TermFilter(value="ai"),
    )
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        vector_query=vq,
        filters=[cat_filter],
        include_source=True,
        top_k=5,
    ))
    print_hits(resp, "Vector + category='ai' filter")

    print("✅ Vector & hybrid search demo complete!")
    channel.close()


if __name__ == "__main__":
    main()
