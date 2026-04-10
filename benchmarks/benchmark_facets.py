"""
Jigyasa vs Elasticsearch — Facets / Aggregations Benchmark
===========================================================
Same dataset, same facet queries, same hardware, same machine.

- Jigyasa: gRPC on localhost:50051 (Lucene 10.4, direct DocValues iteration)
- Elasticsearch 8.13.0: REST on localhost:9201 (Docker, single-node)
- Both: 512MB heap, single node, no replicas

Run:
  1. Start Jigyasa:  cd C:\engram\jigyasa && .\gradlew run
  2. Start ES:       docker run -d --name es-facet -p 9201:9200 -e discovery.type=single-node -e xpack.security.enabled=false -e ES_JAVA_OPTS="-Xms512m -Xmx512m" elasticsearch:8.13.0
  3. Run benchmark:   python benchmark_facets.py
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
import urllib.request

# ---- Config ----
JIGYASA_ADDR = "localhost:50051"
ES_URL = "http://localhost:9201"
COLLECTION = "bench_facets"
ES_INDEX = "bench_facets"
NUM_DOCS = 1000000
ITERATIONS = 100
WARMUP = 20

# ---- Data generation ----
random.seed(42)

CATEGORIES = ["Electronics", "Books", "Clothing", "Home & Kitchen", "Sports",
              "Toys", "Automotive", "Health", "Garden", "Food"]
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE",
          "BrandF", "BrandG", "BrandH", "BrandI", "BrandJ",
          "BrandK", "BrandL", "BrandM", "BrandN", "BrandO"]
COLORS = ["Red", "Blue", "Green", "Black", "White", "Yellow", "Gray", "Orange"]
CITIES = ["New York", "London", "Tokyo", "Mumbai", "Sydney", "Berlin",
          "Paris", "Toronto", "Singapore", "Dubai"]
WORDS = ["laptop", "phone", "tablet", "camera", "headphones", "speaker",
         "keyboard", "mouse", "monitor", "charger", "cable", "adapter",
         "case", "stand", "dock", "hub", "drive", "memory", "processor", "board"]

def gen_doc(i):
    return {
        "id": f"doc-{i}",
        "title": f"{random.choice(WORDS)} {random.choice(WORDS)} {random.choice(['pro', 'lite', 'max', 'mini', 'ultra'])}",
        "category": random.choice(CATEGORIES),
        "brand": random.choice(BRANDS),
        "color": random.choice(COLORS),
        "price": round(random.uniform(5.0, 999.99), 2),
        "rating": random.randint(1, 5),
        "stock_qty": random.randint(0, 500),
        "city": random.choice(CITIES),
    }

DOCS = [gen_doc(i) for i in range(NUM_DOCS)]

# ---- gRPC stub setup ----
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "main", "proto")
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

# ---- Helper functions ----
def es_request(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(f"{ES_URL}/{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

def timed(fn, iterations, warmup):
    """Run fn for warmup + iterations, return list of elapsed_ms for iterations only."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        times.append((time.perf_counter() - start) * 1000)
    return times

def print_stats(label, times):
    p50 = statistics.median(times)
    p90 = sorted(times)[int(len(times) * 0.9)]
    p99 = sorted(times)[int(len(times) * 0.99)]
    avg = statistics.mean(times)
    print(f"  {label:<40} avg={avg:>7.2f}ms  p50={p50:>7.2f}ms  p90={p90:>7.2f}ms  p99={p99:>7.2f}ms")
    return {"avg": avg, "p50": p50, "p90": p90, "p99": p99}

# ---- Index data into Jigyasa ----
def setup_jigyasa(stub):
    print("Setting up Jigyasa...")
    schema = {
        "fields": [
            {"name": "id", "type": "STRING", "key": True, "filterable": True},
            {"name": "title", "type": "STRING", "searchable": True},
            {"name": "category", "type": "STRING", "filterable": True, "sortable": True, "facetable": True},
            {"name": "brand", "type": "STRING", "filterable": True, "facetable": True},
            {"name": "color", "type": "STRING", "filterable": True, "facetable": True},
            {"name": "price", "type": "DOUBLE", "filterable": True, "sortable": True, "facetable": True},
            {"name": "rating", "type": "INT32", "filterable": True, "sortable": True, "facetable": True},
            {"name": "stock_qty", "type": "INT32", "filterable": True, "facetable": True},
            {"name": "city", "type": "STRING", "filterable": True, "facetable": True},
        ]
    }
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(schema)))
    except grpc.RpcError:
        pass  # collection may already exist

    # Bulk index in batches of 500
    batch_size = 500
    for start in range(0, NUM_DOCS, batch_size):
        batch = DOCS[start:start + batch_size]
        items = [pb.IndexItem(document=json.dumps(d), action=pb.UPDATE) for d in batch]
        stub.Index(pb.IndexRequest(collection=COLLECTION, item=items, refresh=pb.NONE))
    # Wait for NRT
    stub.Index(pb.IndexRequest(collection=COLLECTION, item=[
        pb.IndexItem(document=json.dumps({"id": "nrt-flush"}), action=pb.UPDATE)
    ], refresh=pb.WAIT_FOR))
    count = stub.Count(pb.CountRequest(collection=COLLECTION))
    print(f"  Jigyasa: {count.count} docs indexed")

# ---- Index data into Elasticsearch ----
def setup_es():
    print("Setting up Elasticsearch...")
    # Delete + create index
    es_request("DELETE", ES_INDEX)
    es_request("PUT", ES_INDEX, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "category": {"type": "keyword"},
                "brand": {"type": "keyword"},
                "color": {"type": "keyword"},
                "price": {"type": "double"},
                "rating": {"type": "integer"},
                "stock_qty": {"type": "integer"},
                "city": {"type": "keyword"},
            }
        }
    })

    # Bulk index
    batch_size = 500
    for start in range(0, NUM_DOCS, batch_size):
        batch = DOCS[start:start + batch_size]
        lines = []
        for d in batch:
            lines.append(json.dumps({"index": {"_id": d["id"]}}))
            lines.append(json.dumps(d))
        body = "\n".join(lines) + "\n"
        req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_bulk", data=body.encode())
        req.add_header("Content-Type", "application/x-ndjson")
        urllib.request.urlopen(req, timeout=30)

    es_request("POST", f"{ES_INDEX}/_refresh")
    count = es_request("GET", f"{ES_INDEX}/_count")
    print(f"  Elasticsearch: {count['count']} docs indexed")

# ---- Facet benchmarks ----
def benchmark_jigyasa_facets(stub):
    print("\n=== Jigyasa Facets ===")
    results = {}

    # 1. Terms facet — single field (category, 10 unique values)
    def terms_single():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, top_k=1,
            facets=[pb.FacetRequest(field="category", count=10)]))
    results["terms_1field"] = print_stats("Terms (category, 10 values)", timed(terms_single, ITERATIONS, WARMUP))

    # 2. Terms facet — 3 fields simultaneously
    def terms_multi():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, top_k=1,
            facets=[
                pb.FacetRequest(field="category", count=10),
                pb.FacetRequest(field="brand", count=15),
                pb.FacetRequest(field="color", count=8),
            ]))
    results["terms_3fields"] = print_stats("Terms (category+brand+color)", timed(terms_multi, ITERATIONS, WARMUP))

    # 3. Terms facet with text query
    def terms_filtered():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, text_query="laptop", top_k=5,
            facets=[pb.FacetRequest(field="category", count=10),
                    pb.FacetRequest(field="brand", count=10)]))
    results["terms_filtered"] = print_stats("Terms + text query (laptop)", timed(terms_filtered, ITERATIONS, WARMUP))

    # 4. Numeric range facet (price, interval 100)
    def range_facet():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, top_k=1,
            facets=[pb.FacetRequest(field="price", interval=100)]))
    results["range_price"] = print_stats("Range (price, interval=100)", timed(range_facet, ITERATIONS, WARMUP))

    # 5. Numeric terms facet (rating — 5 distinct values)
    def numeric_terms():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, top_k=1,
            facets=[pb.FacetRequest(field="rating", count=5)]))
    results["numeric_terms"] = print_stats("Numeric terms (rating)", timed(numeric_terms, ITERATIONS, WARMUP))

    # 6. Combined: text + filter + 3 facets
    def combined():
        stub.Query(pb.QueryRequest(
            collection=COLLECTION, text_query="phone", top_k=10, include_source=True,
            filters=[pb.FilterClause(field="price",
                     range_filter=pb.RangeFilter(min="10", max="500"))],
            facets=[
                pb.FacetRequest(field="category", count=10),
                pb.FacetRequest(field="brand", count=10),
                pb.FacetRequest(field="price", interval=50),
            ]))
    results["combined"] = print_stats("Text+filter+3 facets", timed(combined, ITERATIONS, WARMUP))

    return results

def benchmark_es_facets():
    print("\n=== Elasticsearch Aggregations ===")
    results = {}

    # 1. Terms agg — single field
    def terms_single():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 1, "aggs": {"category": {"terms": {"field": "category", "size": 10}}}})
    results["terms_1field"] = print_stats("Terms (category, 10 values)", timed(terms_single, ITERATIONS, WARMUP))

    # 2. Terms agg — 3 fields
    def terms_multi():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 1, "aggs": {
                "category": {"terms": {"field": "category", "size": 10}},
                "brand": {"terms": {"field": "brand", "size": 15}},
                "color": {"terms": {"field": "color", "size": 8}},
            }})
    results["terms_3fields"] = print_stats("Terms (category+brand+color)", timed(terms_multi, ITERATIONS, WARMUP))

    # 3. Terms agg with text query
    def terms_filtered():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 5, "query": {"match": {"title": "laptop"}},
            "aggs": {
                "category": {"terms": {"field": "category", "size": 10}},
                "brand": {"terms": {"field": "brand", "size": 10}},
            }})
    results["terms_filtered"] = print_stats("Terms + text query (laptop)", timed(terms_filtered, ITERATIONS, WARMUP))

    # 4. Range agg (price, interval 100 = histogram)
    def range_facet():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 1, "aggs": {"price_hist": {"histogram": {"field": "price", "interval": 100}}}})
    results["range_price"] = print_stats("Histogram (price, interval=100)", timed(range_facet, ITERATIONS, WARMUP))

    # 5. Numeric terms agg (rating)
    def numeric_terms():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 1, "aggs": {"rating": {"terms": {"field": "rating", "size": 5}}}})
    results["numeric_terms"] = print_stats("Numeric terms (rating)", timed(numeric_terms, ITERATIONS, WARMUP))

    # 6. Combined: text + filter + 3 aggs
    def combined():
        es_request("POST", f"{ES_INDEX}/_search", {
            "size": 10, "_source": True,
            "query": {"bool": {
                "must": [{"match": {"title": "phone"}}],
                "filter": [{"range": {"price": {"gte": 10, "lte": 500}}}],
            }},
            "aggs": {
                "category": {"terms": {"field": "category", "size": 10}},
                "brand": {"terms": {"field": "brand", "size": 10}},
                "price_hist": {"histogram": {"field": "price", "interval": 50}},
            }})
    results["combined"] = print_stats("Text+filter+3 aggs", timed(combined, ITERATIONS, WARMUP))

    return results

# ---- Comparison table ----
def print_comparison(j_results, es_results):
    print("\n" + "=" * 85)
    print(f"{'Facet Type':<40} {'Jigyasa p50':>12} {'ES p50':>12} {'Speedup':>10}")
    print("=" * 85)

    labels = {
        "terms_1field": "Terms (1 field, 10 values)",
        "terms_3fields": "Terms (3 fields simultaneous)",
        "terms_filtered": "Terms + text query",
        "range_price": "Numeric range/histogram",
        "numeric_terms": "Numeric terms (rating)",
        "combined": "Text + filter + 3 facets",
    }

    for key, label in labels.items():
        jp50 = j_results[key]["p50"]
        ep50 = es_results[key]["p50"]
        speedup = ep50 / jp50 if jp50 > 0 else float("inf")
        winner = "Jigyasa" if speedup > 1 else "ES"
        print(f"  {label:<38} {jp50:>8.2f}ms   {ep50:>8.2f}ms   {speedup:>5.1f}x {winner}")

    # Overall average
    j_avg = statistics.mean([v["p50"] for v in j_results.values()])
    es_avg = statistics.mean([v["p50"] for v in es_results.values()])
    speedup = es_avg / j_avg if j_avg > 0 else float("inf")
    print("-" * 85)
    print(f"  {'OVERALL AVERAGE':<38} {j_avg:>8.2f}ms   {es_avg:>8.2f}ms   {speedup:>5.1f}x")
    print("=" * 85)

# ---- Main ----
def main():
    print(f"Facets Benchmark: {NUM_DOCS} docs, {ITERATIONS} iterations, {WARMUP} warmup\n")

    channel = grpc.insecure_channel(JIGYASA_ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    setup_jigyasa(stub)
    setup_es()

    j_results = benchmark_jigyasa_facets(stub)
    es_results = benchmark_es_facets()

    print_comparison(j_results, es_results)

    channel.close()

if __name__ == "__main__":
    main()
