"""
Vector Search SIMD Benchmark — measures KNN query latency with and without SIMD.
Run this twice: once with server started WITH --add-modules jdk.incubator.vector,
once WITHOUT. Compare results.
"""
import grpc
import json
import sys
import os
import time
import random
import statistics
import subprocess
import site

ADDR = "localhost:50051"
COLLECTION = "vector_bench"
NUM_DOCS = 5000
VECTOR_DIM = 128
NUM_QUERIES = 500
WARMUP = 50

def generate_stubs():
    proto_path = os.path.join(os.path.dirname(__file__), "..", "src", "main", "proto")
    out_path = os.path.join(os.path.dirname(__file__), "test-data", "gen")
    os.makedirs(out_path, exist_ok=True)
    import grpc_tools
    grpc_proto_include = os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')
    sp = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_path}", f"--proto_path={grpc_proto_include}", f"--proto_path={sp}",
        f"--python_out={out_path}", f"--grpc_python_out={out_path}", "dpSearch.proto"
    ], capture_output=True, check=True)

def main():
    generate_stubs()
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "test-data", "gen"))
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    random.seed(42)

    schema = {
        "fields": [
            {"name": "id", "type": "STRING", "key": True, "filterable": True},
            {"name": "title", "type": "STRING", "searchable": True},
            {"name": "embedding", "type": "VECTOR", "searchable": True,
             "vectorDimension": VECTOR_DIM, "vectorSimilarity": "COSINE"}
        ],
        "hnswConfig": {"maxConn": 16, "beamWidth": 100}
    }

    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    print("=" * 70)
    print("  VECTOR SEARCH SIMD BENCHMARK")
    print(f"  {NUM_DOCS} docs, {VECTOR_DIM}-dim vectors, {NUM_QUERIES} queries")
    print("=" * 70)

    # Create collection
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(schema)))
        print(f"  Collection '{COLLECTION}' created")
    except:
        print(f"  Collection '{COLLECTION}' already exists")

    # Generate and index documents with random vectors
    print(f"  Indexing {NUM_DOCS} documents with {VECTOR_DIM}-dim vectors...")
    for batch_start in range(0, NUM_DOCS, 500):
        batch_end = min(batch_start + 500, NUM_DOCS)
        items = []
        for i in range(batch_start, batch_end):
            vec = [random.gauss(0, 1) for _ in range(VECTOR_DIM)]
            # Normalize for cosine
            norm = sum(v * v for v in vec) ** 0.5
            vec = [v / norm for v in vec]
            doc = {"id": f"doc-{i}", "title": f"Document {i}", "embedding": vec}
            items.append(pb.IndexItem(document=json.dumps(doc)))
        stub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE))

    time.sleep(2)
    stub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=1))
    count = stub.Count(pb.CountRequest(collection=COLLECTION)).count
    print(f"  Indexed: {count} docs, merged to 1 segment")

    # Generate query vectors
    query_vectors = []
    for _ in range(NUM_QUERIES + WARMUP):
        vec = [random.gauss(0, 1) for _ in range(VECTOR_DIM)]
        norm = sum(v * v for v in vec) ** 0.5
        query_vectors.append([v / norm for v in vec])

    # --- Benchmark 1: Pure KNN (top-10) ---
    print(f"\n  Pure KNN top-10 ({NUM_QUERIES} queries, {WARMUP} warmup)...")
    for i in range(WARMUP):
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=10),
            top_k=10))

    latencies_knn10 = []
    for i in range(WARMUP, WARMUP + NUM_QUERIES):
        t0 = time.perf_counter()
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=10),
            top_k=10))
        latencies_knn10.append((time.perf_counter() - t0) * 1000)

    # --- Benchmark 2: Pure KNN (top-50) ---
    print(f"  Pure KNN top-50 ({NUM_QUERIES} queries, {WARMUP} warmup)...")
    for i in range(WARMUP):
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=50),
            top_k=50))

    latencies_knn50 = []
    for i in range(WARMUP, WARMUP + NUM_QUERIES):
        t0 = time.perf_counter()
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=50),
            top_k=50))
        latencies_knn50.append((time.perf_counter() - t0) * 1000)

    # --- Benchmark 3: Hybrid BM25 + KNN ---
    search_terms = ["document", "data", "search", "vector", "index"]
    print(f"  Hybrid BM25+KNN ({NUM_QUERIES} queries, {WARMUP} warmup)...")
    for i in range(WARMUP):
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            text_query=random.choice(search_terms),
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=10),
            text_weight=0.5, top_k=10))

    latencies_hybrid = []
    for i in range(WARMUP, WARMUP + NUM_QUERIES):
        t0 = time.perf_counter()
        stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            text_query=random.choice(search_terms),
            vector_query=pb.VectorQuery(field="embedding", vector=query_vectors[i], k=10),
            text_weight=0.5, top_k=10))
        latencies_hybrid.append((time.perf_counter() - t0) * 1000)

    # --- Results ---
    def stats(lat):
        lat.sort()
        return {
            "p50": lat[len(lat)//2],
            "p90": lat[int(len(lat)*0.9)],
            "p99": lat[int(len(lat)*0.99)],
            "mean": statistics.mean(lat),
            "qps": 1000.0 / statistics.mean(lat)
        }

    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)
    for name, lat in [("KNN top-10", latencies_knn10), ("KNN top-50", latencies_knn50), ("Hybrid BM25+KNN", latencies_hybrid)]:
        s = stats(lat)
        print(f"  {name:<20s}  p50={s['p50']:>6.2f}ms  p90={s['p90']:>6.2f}ms  p99={s['p99']:>6.2f}ms  mean={s['mean']:>6.2f}ms  qps={s['qps']:>7.1f}")

    print(f"\n  Dataset: {NUM_DOCS} docs, {VECTOR_DIM}-dim COSINE, HNSW(maxConn=16, beamWidth=100)")
    print(f"  Queries: {NUM_QUERIES} per type, {WARMUP} warmup")
    channel.close()

if __name__ == "__main__":
    main()
