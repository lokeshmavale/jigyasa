"""
Jigyasa vs Elasticsearch 8.13 — 1M Document Benchmark
======================================================
Both engines in Linux containers with EQUAL resources:
- 4 CPUs, 4GB memory, 2GB JVM heap
- 1 shard, 0 replicas
- 1,000,000 HTTP log documents (Rally http_logs pattern)

Usage:
    python benchmark_1m.py
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
import concurrent.futures

# --- Config ---
JIGYASA_ADDR = "localhost:50051"
ES_URL = "http://localhost:9201"
COLLECTION = "bench_1m"
ES_INDEX = "bench_1m"
NUM_DOCS = 1_000_000
BATCH_SIZE = 1000
CPUS = 4
CONTAINER_MEM = "12g"
HEAP = "8g"

# --- Data generation (seeded, deterministic) ---
random.seed(42)

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
HTTP_STATUSES = [200, 200, 200, 200, 201, 301, 302, 400, 401, 403, 404, 500, 502, 503]
PATHS = ["/index.html", "/api/users", "/api/search", "/login", "/dashboard", "/api/data",
         "/static/main.js", "/health", "/api/v2/query", "/docs", "/api/metrics",
         "/checkout", "/api/orders", "/products", "/api/inventory", "/api/auth/token",
         "/api/v3/recommendations", "/static/style.css", "/api/notifications", "/favicon.ico"]
USER_AGENTS = ["Mozilla/5.0 Chrome/120", "Mozilla/5.0 Firefox/121", "curl/8.4",
               "python-requests/2.31", "Go-http-client/2.0", "Googlebot/2.1",
               "Mozilla/5.0 Safari/17", "Apache-HttpClient/5.3"]
CITIES = ["New York", "London", "Tokyo", "Mumbai", "Sydney", "Berlin", "Paris",
          "Toronto", "Singapore", "Dubai", "Hyderabad", "Pune", "San Francisco",
          "Chicago", "Seattle", "Boston", "Austin", "Amsterdam", "Stockholm", "Seoul"]
CATEGORIES = ["web", "api", "static", "auth", "data", "admin", "cdn", "monitoring"]
TEXT_CORPUS = [
    "Apache Lucene is a high-performance full-text search engine library",
    "Elasticsearch is a distributed search and analytics engine",
    "Vector search using HNSW graphs provides approximate nearest neighbor",
    "Machine learning models require efficient storage and retrieval",
    "Kubernetes orchestrates containerized applications across clusters",
    "GraphQL provides a flexible query language for APIs",
    "Microservices architecture enables independent deployment of services",
    "Real-time analytics pipelines process millions of events per second",
    "Natural language processing transforms unstructured text into insights",
    "Database indexing strategies significantly impact query performance",
    "Load balancing distributes traffic across multiple server instances",
    "Content delivery networks cache static assets at edge locations",
    "OAuth 2.0 provides secure delegated access to server resources",
    "Distributed tracing helps debug issues across microservices",
    "Container orchestration platforms manage application lifecycle",
]

def generate_doc(i):
    ts = 1700000000 + i
    return {
        "id": f"log-{i:07d}",
        "method": random.choice(HTTP_METHODS),
        "path": random.choice(PATHS),
        "status": random.choice(HTTP_STATUSES),
        "response_time_ms": random.randint(1, 5000),
        "bytes": random.randint(100, 500000),
        "user_agent": random.choice(USER_AGENTS),
        "city": random.choice(CITIES),
        "category": random.choice(CATEGORIES),
        "message": random.choice(TEXT_CORPUS) + f" request_{i}",
        "timestamp": ts
    }

SCHEMA = {
    "fields": [
        {"name": "id", "type": "STRING", "key": True, "filterable": True},
        {"name": "method", "type": "STRING", "filterable": True},
        {"name": "path", "type": "STRING", "searchable": True},
        {"name": "status", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "response_time_ms", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "bytes", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "user_agent", "type": "STRING", "searchable": True},
        {"name": "city", "type": "STRING", "filterable": True},
        {"name": "category", "type": "STRING", "filterable": True},
        {"name": "message", "type": "STRING", "searchable": True},
        {"name": "timestamp", "type": "INT64", "filterable": True, "sortable": True}
    ]
}

ES_MAPPINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
    "mappings": {"properties": {
        "id": {"type": "keyword"}, "method": {"type": "keyword"},
        "path": {"type": "text"}, "status": {"type": "integer"},
        "response_time_ms": {"type": "integer"}, "bytes": {"type": "integer"},
        "user_agent": {"type": "text"}, "city": {"type": "keyword"},
        "category": {"type": "keyword"}, "message": {"type": "text"},
        "timestamp": {"type": "long"}
    }}
}

# --- Helpers ---
def es_request(method, path, body=None):
    url = f"{ES_URL}/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
                                headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())

def setup_stubs():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)  # benchmarks/ -> repo root
    proto_path = os.path.join(repo_root, "src", "main", "proto")
    out_path = os.path.join(script_dir, "test-data", "gen")
    os.makedirs(out_path, exist_ok=True)
    import grpc_tools
    grpc_proto_include = os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')
    sp = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_path}", f"--proto_path={grpc_proto_include}", f"--proto_path={sp}",
        f"--python_out={out_path}", f"--grpc_python_out={out_path}", "dpSearch.proto"
    ], capture_output=True, check=True)
    sys.path.insert(0, out_path)

SEARCH_TERMS = ["Lucene", "search", "Kubernetes", "microservices", "analytics",
                "machine learning", "GraphQL", "database", "container", "distributed"]

def run_query_bench(name, fn, iterations=500, warmup=50):
    for _ in range(warmup):
        fn()
    latencies = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        latencies.append((time.perf_counter() - t0) * 1000)
    latencies.sort()
    p50 = latencies[len(latencies)//2]
    p90 = latencies[int(len(latencies)*0.9)]
    p99 = latencies[int(len(latencies)*0.99)]
    mean = statistics.mean(latencies)
    qps = 1000 / mean
    print(f"  {name:<35s} p50={p50:>7.2f}ms  p90={p90:>7.2f}ms  p99={p99:>7.2f}ms  mean={mean:>7.2f}ms  qps={qps:>7.1f}")
    return {"name": name, "p50": p50, "p90": p90, "p99": p99, "mean": mean, "qps": qps}

# ============================================================
# MAIN
# ============================================================
def main():
    setup_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    print("=" * 80)
    print("  JIGYASA vs ELASTICSEARCH 8.13 — 1M DOCUMENT BENCHMARK")
    print(f"  Resources: {CPUS} CPUs, {CONTAINER_MEM} memory, {HEAP} heap (both engines)")
    print(f"  Config: 1 shard, 0 replicas, {NUM_DOCS:,} HTTP log documents")
    print("=" * 80)

    # --- Start containers ---
    print("\n[1/6] Starting containers...")

    # Stop any existing
    subprocess.run(["docker", "rm", "-f", "jigyasa-1m", "es-1m"], capture_output=True)
    time.sleep(2)

    # Start ES
    jar_path = None
    libs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "libs")
    for f in os.listdir(libs_dir):
        if f.endswith("-all.jar"):
            jar_path = os.path.abspath(os.path.join(libs_dir, f))
            break

    print("  Starting Elasticsearch 8.13...")
    subprocess.run([
        "docker", "run", "-d", "--name", "es-1m",
        f"--cpus={CPUS}", f"--memory={CONTAINER_MEM}",
        "-p", "9201:9200",
        "-e", "discovery.type=single-node",
        "-e", "xpack.security.enabled=false",
        "-e", f"ES_JAVA_OPTS=-Xms{HEAP} -Xmx{HEAP}",
        "-e", "bootstrap.memory_lock=true",
        "--ulimit", "memlock=-1:-1",
        "elasticsearch:8.13.0"
    ], capture_output=True)

    print("  Starting Jigyasa...")
    subprocess.run([
        "docker", "run", "-d", "--name", "jigyasa-1m",
        f"--cpus={CPUS}", f"--memory={CONTAINER_MEM}",
        "-p", "50051:50051",
        "-v", f"{jar_path}:/app/jigyasa.jar",
        "-e", "BOOTSTRAP_MEMORY_LOCK=true",
        "eclipse-temurin:21-jre",
        "java", f"-Xms{HEAP}", f"-Xmx{HEAP}",
        "--add-modules", "jdk.incubator.vector",
        "-Dlucene.useScalarFMA=true", "-Dlucene.useVectorFMA=true",
        "-XX:+AlwaysPreTouch",
        "-jar", "/app/jigyasa.jar"
    ], capture_output=True)

    # Wait for both
    print("  Waiting for servers...")
    for label, check_fn in [
        ("ES", lambda: urllib.request.urlopen(f"{ES_URL}", timeout=2)),
        ("Jigyasa", lambda: __import__('socket').create_connection(("localhost", 50051), timeout=2))
    ]:
        for i in range(60):
            try:
                check_fn()
                print(f"  {label} ready ({i*2}s)")
                break
            except:
                time.sleep(2)
        else:
            print(f"  {label} NOT READY — aborting")
            # Print container logs for debugging
            logs = subprocess.run(["docker", "logs", "--tail", "20",
                                   "jigyasa-1m" if label == "Jigyasa" else "es-1m"],
                                  capture_output=True, text=True)
            print(logs.stdout[-500:] if logs.stdout else "no logs")
            return

    channel = grpc.insecure_channel(JIGYASA_ADDR)
    jstub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # --- Index 1M docs ---
    print(f"\n[2/6] Indexing {NUM_DOCS:,} documents...")

    # Jigyasa
    try:
        jstub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(SCHEMA)), timeout=10)
    except: pass

    t0 = time.perf_counter()
    for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
        items = [pb.IndexItem(document=json.dumps(generate_doc(i)))
                 for i in range(batch_start, batch_end)]
        jstub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE), timeout=60)
        if batch_start % 100000 == 0 and batch_start > 0:
            print(f"    Jigyasa: {batch_start:,} / {NUM_DOCS:,}")
    j_index_time = time.perf_counter() - t0
    j_dps = NUM_DOCS / j_index_time
    print(f"  Jigyasa: {NUM_DOCS:,} docs in {j_index_time:.1f}s = {j_dps:,.0f} docs/sec")

    # ES
    try: es_request("DELETE", ES_INDEX)
    except: pass
    es_request("PUT", ES_INDEX, ES_MAPPINGS)

    t0 = time.perf_counter()
    for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
        bulk_body = ""
        for i in range(batch_start, batch_end):
            doc = generate_doc(i)
            bulk_body += json.dumps({"index": {"_id": doc["id"]}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"
        req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_bulk",
                                    data=bulk_body.encode(),
                                    headers={"Content-Type": "application/x-ndjson"},
                                    method="POST")
        urllib.request.urlopen(req, timeout=120)
        if batch_start % 100000 == 0 and batch_start > 0:
            print(f"    ES: {batch_start:,} / {NUM_DOCS:,}")
    e_index_time = time.perf_counter() - t0
    e_dps = NUM_DOCS / e_index_time
    print(f"  ES:      {NUM_DOCS:,} docs in {e_index_time:.1f}s = {e_dps:,.0f} docs/sec")

    # --- Force merge ---
    print("\n[3/6] Force merging to 1 segment...")
    jstub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=1), timeout=300)
    print("  Jigyasa merged")
    es_request("POST", f"{ES_INDEX}/_forcemerge?max_num_segments=1")
    es_request("POST", f"{ES_INDEX}/_refresh")
    print("  ES merged")

    # Verify counts
    j_count = jstub.Count(pb.CountRequest(collection=COLLECTION), timeout=10).count
    e_count = es_request("GET", f"{ES_INDEX}/_count")["count"]
    print(f"  Jigyasa: {j_count:,} docs | ES: {e_count:,} docs")

    # --- Query benchmarks ---
    print(f"\n[4/6] Query latency benchmarks (500 iterations, 50 warmup)...")

    j_results = []
    e_results = []

    # BM25 text
    j_results.append(run_query_bench("Jigyasa — BM25 text",
        lambda: jstub.Query(pb.QueryRequest(collection=COLLECTION,
            text_query=random.choice(SEARCH_TERMS), top_k=10), timeout=5)))
    e_results.append(run_query_bench("ES — BM25 text",
        lambda: es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"match": {"message": random.choice(SEARCH_TERMS)}}, "size": 10})))

    # Term filter
    j_results.append(run_query_bench("Jigyasa — Term filter",
        lambda: jstub.Query(pb.QueryRequest(collection=COLLECTION, top_k=10,
            filters=[pb.FilterClause(field="category",
                term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))]), timeout=5)))
    e_results.append(run_query_bench("ES — Term filter",
        lambda: es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"term": {"category": random.choice(CATEGORIES)}}, "size": 10})))

    # Range filter
    j_results.append(run_query_bench("Jigyasa — Range filter",
        lambda: jstub.Query(pb.QueryRequest(collection=COLLECTION, top_k=10,
            filters=[pb.FilterClause(field="status",
                range_filter=pb.RangeFilter(min="400", max="503"))]), timeout=5)))
    e_results.append(run_query_bench("ES — Range filter",
        lambda: es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"range": {"status": {"gte": 400, "lte": 503}}}, "size": 10})))

    # Count
    j_results.append(run_query_bench("Jigyasa — Count",
        lambda: jstub.Count(pb.CountRequest(collection=COLLECTION), timeout=5)))
    e_results.append(run_query_bench("ES — Count",
        lambda: es_request("POST", f"{ES_INDEX}/_count", {"query": {"match_all": {}}})))

    # Sort
    j_results.append(run_query_bench("Jigyasa — Sort (response_time)",
        lambda: jstub.Query(pb.QueryRequest(collection=COLLECTION, top_k=10,
            sort=[pb.SortClause(field="response_time_ms", descending=True)]), timeout=5)))
    e_results.append(run_query_bench("ES — Sort (response_time)",
        lambda: es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"match_all": {}}, "sort": [{"response_time_ms": "desc"}], "size": 10})))

    # Text + filter
    j_results.append(run_query_bench("Jigyasa — Text+filter",
        lambda: jstub.Query(pb.QueryRequest(collection=COLLECTION,
            text_query=random.choice(SEARCH_TERMS), top_k=10,
            filters=[pb.FilterClause(field="city",
                term_filter=pb.TermFilter(value=random.choice(CITIES)))]), timeout=5)))
    e_results.append(run_query_bench("ES — Text+filter",
        lambda: es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"bool": {"must": {"match": {"message": random.choice(SEARCH_TERMS)}},
                                "filter": {"term": {"city": random.choice(CITIES)}}}}, "size": 10})))

    # --- Comparison table ---
    print(f"\n[5/6] COMPARISON TABLE")
    print("=" * 90)
    print(f"  {'Query Type':<25s} {'Jigyasa p50':>12s} {'ES p50':>10s} {'Ratio':>8s} {'J p99':>10s} {'ES p99':>10s}")
    print("  " + "-" * 85)
    for j, e in zip(j_results, e_results):
        name = j["name"].replace("Jigyasa — ", "")
        ratio = e["p50"] / j["p50"] if j["p50"] > 0 else 0
        print(f"  {name:<25s} {j['p50']:>9.2f}ms {e['p50']:>8.2f}ms {ratio:>6.1f}x {j['p99']:>8.2f}ms {e['p99']:>8.2f}ms")

    # --- Concurrent throughput ---
    print(f"\n[6/6] Concurrent throughput (4 threads, 10s)...")

    def j_mixed():
        jstub.Query(pb.QueryRequest(collection=COLLECTION,
            text_query=random.choice(SEARCH_TERMS), top_k=10), timeout=5)

    def e_mixed():
        es_request("POST", f"{ES_INDEX}/_search",
            {"query": {"match": {"message": random.choice(SEARCH_TERMS)}}, "size": 10})

    for label, fn in [("Jigyasa", j_mixed), ("ES", e_mixed)]:
        ops = [0]
        errors = [0]
        stop_time = time.perf_counter() + 10

        def worker():
            while time.perf_counter() < stop_time:
                try:
                    fn()
                    ops[0] += 1
                except:
                    errors[0] += 1

        threads = []
        for _ in range(4):
            import threading
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        qps = ops[0] / 10
        print(f"  {label:<10s} {qps:>7,.0f} qps  ({ops[0]:,} ops, {errors[0]} errors)")

    # --- Summary ---
    j_avg_p50 = statistics.mean(r["p50"] for r in j_results)
    e_avg_p50 = statistics.mean(r["p50"] for r in e_results)
    print(f"\n{'='*80}")
    print(f"  SUMMARY — 1M docs, {CPUS} CPUs, {HEAP} heap, 1 shard")
    print(f"  Jigyasa avg p50: {j_avg_p50:.2f}ms | ES avg p50: {e_avg_p50:.2f}ms | Ratio: {e_avg_p50/j_avg_p50:.1f}x")
    print(f"  Indexing: Jigyasa {j_dps:,.0f} docs/s | ES {e_dps:,.0f} docs/s")
    print(f"{'='*80}")

    channel.close()

    # Cleanup
    print("\nCleaning up containers...")
    subprocess.run(["docker", "rm", "-f", "jigyasa-1m", "es-1m"], capture_output=True)


if __name__ == "__main__":
    main()
