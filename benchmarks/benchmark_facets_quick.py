"""Jigyasa-only facets benchmark — runs against localhost:50051"""
import grpc, json, sys, os, time, random, statistics, subprocess, site

PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)
import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]
subprocess.run([sys.executable, "-m", "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}", f"--proto_path={grpc_include}", f"--proto_path={site_pkgs}",
    f"--python_out={GEN_DIR}", f"--grpc_python_out={GEN_DIR}", "dpSearch.proto"],
    capture_output=True, text=True, check=True)
sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb, dpSearch_pb2_grpc as pb_grpc

COLLECTION = "bench_facets"
NUM_DOCS = 10000
ITERATIONS = 100
WARMUP = 20

random.seed(42)
CATEGORIES = ["Electronics", "Books", "Clothing", "Home & Kitchen", "Sports",
              "Toys", "Automotive", "Health", "Garden", "Food"]
BRANDS = ["BrandA","BrandB","BrandC","BrandD","BrandE","BrandF","BrandG",
          "BrandH","BrandI","BrandJ","BrandK","BrandL","BrandM","BrandN","BrandO"]
COLORS = ["Red","Blue","Green","Black","White","Yellow","Gray","Orange"]
WORDS = ["laptop","phone","tablet","camera","headphones","speaker","keyboard",
         "mouse","monitor","charger","cable","adapter","case","stand"]
SUFFIXES = ["pro", "lite", "max", "mini"]

def gen_doc(i):
    return {
        "id": f"doc-{i}",
        "title": f"{random.choice(WORDS)} {random.choice(WORDS)} {random.choice(SUFFIXES)}",
        "category": random.choice(CATEGORIES),
        "brand": random.choice(BRANDS),
        "color": random.choice(COLORS),
        "price": round(random.uniform(5.0, 999.99), 2),
        "rating": random.randint(1, 5),
        "stock_qty": random.randint(0, 500),
    }

DOCS = [gen_doc(i) for i in range(NUM_DOCS)]

channel = grpc.insecure_channel("localhost:50051")
stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

# Setup
schema = {"fields": [
    {"name": "id", "type": "STRING", "key": True, "filterable": True},
    {"name": "title", "type": "STRING", "searchable": True},
    {"name": "category", "type": "STRING", "filterable": True, "sortable": True, "facetable": True},
    {"name": "brand", "type": "STRING", "filterable": True, "facetable": True},
    {"name": "color", "type": "STRING", "filterable": True, "facetable": True},
    {"name": "price", "type": "DOUBLE", "filterable": True, "sortable": True, "facetable": True},
    {"name": "rating", "type": "INT32", "filterable": True, "sortable": True, "facetable": True},
    {"name": "stock_qty", "type": "INT32", "filterable": True, "facetable": True},
]}
try:
    stub.CreateCollection(pb.CreateCollectionRequest(collection=COLLECTION, indexSchema=json.dumps(schema)))
except Exception:
    pass

print(f"Indexing {NUM_DOCS} docs...")
for start in range(0, NUM_DOCS, 500):
    batch = DOCS[start:start + 500]
    items = [pb.IndexItem(document=json.dumps(d), action=pb.UPDATE) for d in batch]
    stub.Index(pb.IndexRequest(collection=COLLECTION, item=items, refresh=pb.NONE))
stub.Index(pb.IndexRequest(collection=COLLECTION, item=[
    pb.IndexItem(document=json.dumps({"id": "flush"}))], refresh=pb.WAIT_FOR))
count = stub.Count(pb.CountRequest(collection=COLLECTION))
print(f"Indexed: {count.count} docs\n")

def timed(fn, iters, warmup):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iters):
        s = time.perf_counter()
        fn()
        times.append((time.perf_counter() - s) * 1000)
    return times

def show(label, times):
    p50 = statistics.median(times)
    p90 = sorted(times)[int(len(times) * 0.9)]
    p99 = sorted(times)[int(len(times) * 0.99)]
    avg = statistics.mean(times)
    print(f"  {label:<42} avg={avg:>7.2f}ms  p50={p50:>7.2f}ms  p90={p90:>7.2f}ms  p99={p99:>7.2f}ms")
    return p50

print(f"=== Jigyasa Facets Benchmark ({NUM_DOCS} docs, {ITERATIONS} iters, {WARMUP} warmup) ===\n")

show("Terms: 1 field (category, 10 values)", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, top_k=1,
    facets=[pb.FacetRequest(field="category", count=10)])), ITERATIONS, WARMUP))

show("Terms: 3 fields (cat+brand+color)", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, top_k=1,
    facets=[pb.FacetRequest(field="category", count=10),
            pb.FacetRequest(field="brand", count=15),
            pb.FacetRequest(field="color", count=8)])), ITERATIONS, WARMUP))

show("Terms: + text query (laptop)", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, text_query="laptop", top_k=5,
    facets=[pb.FacetRequest(field="category", count=10),
            pb.FacetRequest(field="brand", count=10)])), ITERATIONS, WARMUP))

show("Range: price interval=100", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, top_k=1,
    facets=[pb.FacetRequest(field="price", interval=100)])), ITERATIONS, WARMUP))

show("Numeric terms: rating (5 values)", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, top_k=1,
    facets=[pb.FacetRequest(field="rating", count=5)])), ITERATIONS, WARMUP))

show("Combined: text+filter+3 facets", timed(lambda: stub.Query(pb.QueryRequest(
    collection=COLLECTION, text_query="phone", top_k=10, include_source=True,
    filters=[pb.FilterClause(field="price",
             range_filter=pb.RangeFilter(min="10", max="500"))],
    facets=[pb.FacetRequest(field="category", count=10),
            pb.FacetRequest(field="brand", count=10),
            pb.FacetRequest(field="price", interval=50)])), ITERATIONS, WARMUP))

# Correctness check
resp = stub.Query(pb.QueryRequest(collection=COLLECTION, top_k=1,
    facets=[pb.FacetRequest(field="category", count=10, sort=pb.COUNT_DESC)]))
print(f"\n--- Correctness: category facet ---")
total = 0
for b in resp.facets["category"].buckets:
    print(f"  {b.value}: {b.count}")
    total += b.count
print(f"  Sum: {total} (index has {count.count} docs)")

channel.close()
print("\nDone.")
