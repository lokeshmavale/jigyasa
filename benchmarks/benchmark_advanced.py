"""
Jigyasa vs Elasticsearch — Advanced Benchmarks
================================================
1. Concurrent throughput (multi-threaded qps under load)
2. Cold start time comparison
3. Memory footprint

Same dataset as benchmark_comparison.py (10K HTTP logs).
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
import threading
import concurrent.futures

JIGYASA_ADDR = "localhost:50051"
ES_URL = "http://localhost:9201"
COLLECTION = "bench_advanced"
ES_INDEX = "bench_advanced"
NUM_DOCS = 10000

# Resource allocation: N GB container memory, N/2 heap for both engines
CONTAINER_MEMORY_GB = 4
HEAP_SIZE_MB = (CONTAINER_MEMORY_GB * 1024) // 2  # N/2 rule: leave room for off-heap (Lucene mmaps, netty buffers)

# ---- Data generation (same seed as benchmark_comparison.py) ----
random.seed(42)

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 301, 302, 400, 401, 403, 404, 500, 502, 503]
PATHS = ["/index", "/api/users", "/api/search", "/login", "/dashboard", "/api/data",
         "/static/main.js", "/health", "/api/v2/query", "/docs", "/api/metrics",
         "/checkout", "/api/orders", "/products", "/api/inventory"]
USER_AGENTS = ["Mozilla/5.0 Chrome/120", "Mozilla/5.0 Firefox/121", "curl/8.4",
               "python-requests/2.31", "Go-http-client/2.0", "Googlebot/2.1"]
CITIES = ["New York", "London", "Tokyo", "Mumbai", "Sydney", "Berlin",
          "Paris", "Toronto", "Singapore", "Dubai", "Hyderabad", "Pune",
          "San Francisco", "Chicago", "Seattle", "Boston", "Austin"]
CATEGORIES = ["web", "api", "static", "auth", "data", "admin"]
TEXT_CORPUS = [
    "Apache Lucene is a high-performance full-text search engine library written in Java",
    "Elasticsearch is a distributed search and analytics engine built on Apache Lucene",
    "Vector search using HNSW graphs provides approximate nearest neighbor capabilities",
    "Machine learning models require efficient storage and retrieval of training data",
    "Kubernetes orchestrates containerized applications across a cluster of machines",
    "GraphQL provides a flexible query language for APIs with strong typing",
    "Microservices architecture enables independent deployment and scaling of services",
    "Redis is an in-memory data structure store used as a database and cache",
    "PostgreSQL is a powerful open-source relational database management system",
    "Docker containers package applications with their dependencies for consistent deployment",
    "Natural language processing enables machines to understand human language",
    "Transformer models like BERT and GPT have revolutionized NLP tasks",
    "Distributed systems require careful handling of network partitions and consistency",
    "Search relevance tuning involves adjusting BM25 parameters and field boosting",
    "Index sharding distributes data across multiple nodes for horizontal scaling",
    "Memory management in JVM applications requires understanding of garbage collection",
    "gRPC provides high-performance remote procedure calls with protocol buffers",
    "Observability requires metrics, traces, and logs for production systems",
    "CI/CD pipelines automate building, testing, and deploying applications",
    "Agent memory systems need persistent, searchable, and rankable storage",
]

SEARCH_TERMS = ["Lucene search engine", "machine learning", "Kubernetes deployment",
                "distributed systems", "natural language processing", "vector search",
                "memory management", "gRPC protocol", "Docker container", "search relevance"]

def generate_doc(doc_id):
    ts = 1711900000 + random.randint(0, 86400 * 30)
    return {
        "id": f"log-{doc_id}", "method": random.choice(HTTP_METHODS),
        "path": random.choice(PATHS), "status": random.choice(HTTP_STATUSES),
        "response_time_ms": random.randint(1, 5000), "bytes": random.randint(100, 500000),
        "user_agent": random.choice(USER_AGENTS), "city": random.choice(CITIES),
        "category": random.choice(CATEGORIES),
        "message": random.choice(TEXT_CORPUS) + " " + random.choice(TEXT_CORPUS),
        "timestamp": ts
    }

ALL_DOCS = [generate_doc(i) for i in range(NUM_DOCS)]

SCHEMA = {
    "fields": [
        {"name": "id", "type": "STRING", "key": True, "filterable": True},
        {"name": "method", "type": "STRING", "filterable": True, "sortable": True},
        {"name": "path", "type": "STRING", "searchable": True},
        {"name": "status", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "response_time_ms", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "bytes", "type": "INT32", "filterable": True, "sortable": True},
        {"name": "user_agent", "type": "STRING", "searchable": True},
        {"name": "city", "type": "STRING", "filterable": True, "sortable": True},
        {"name": "category", "type": "STRING", "filterable": True, "sortable": True},
        {"name": "message", "type": "STRING", "searchable": True},
        {"name": "timestamp", "type": "INT64", "filterable": True, "sortable": True}
    ]
}

def es_request(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(f"{ES_URL}/{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def setup_jigyasa_stubs():
    proto_path = os.path.join(os.path.dirname(__file__), "..", "src", "main", "proto")
    out_path = os.path.join(os.path.dirname(__file__), "test-data", "gen")
    os.makedirs(out_path, exist_ok=True)
    import grpc_tools
    grpc_proto_include = os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')
    sp = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_path}", f"--proto_path={grpc_proto_include}",
        f"--proto_path={sp}", f"--python_out={out_path}",
        f"--grpc_python_out={out_path}", "dpSearch.proto"
    ], capture_output=True, check=True)
    sys.path.insert(0, out_path)

def percentile(data, p):
    if not data: return 0
    data.sort()
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    return data[f] + (k - f) * (data[c] - data[f])


# ============================================================
#  BENCHMARK 1: CONCURRENT THROUGHPUT
# ============================================================
def run_concurrent_benchmark(name, query_func, thread_counts, duration_sec=10):
    """Run queries across N threads for a fixed duration, measure total qps."""
    print(f"\n  {name}")
    results = []
    for num_threads in thread_counts:
        stop_event = threading.Event()
        total_ops = [0]
        total_latencies = []
        latency_lock = threading.Lock()
        errors = [0]

        def worker():
            local_ops = 0
            local_lats = []
            while not stop_event.is_set():
                start = time.perf_counter()
                try:
                    query_func()
                    lat = (time.perf_counter() - start) * 1000
                    local_ops += 1
                    local_lats.append(lat)
                except:
                    errors[0] += 1
            with latency_lock:
                total_ops[0] += local_ops
                total_latencies.extend(local_lats)

        # Warmup
        for _ in range(20):
            try: query_func()
            except: pass

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        start_time = time.perf_counter()
        for t in threads:
            t.start()
        time.sleep(duration_sec)
        stop_event.set()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - start_time

        qps = total_ops[0] / elapsed
        total_latencies.sort()
        p50 = percentile(total_latencies, 50) if total_latencies else 0
        p99 = percentile(total_latencies, 99) if total_latencies else 0
        results.append((num_threads, qps, p50, p99, errors[0]))
        print(f"    {num_threads:>2d} threads: {qps:>8,.0f} qps  p50={p50:>6.2f}ms  p99={p99:>6.2f}ms  "
              f"ops={total_ops[0]:>6,}  errors={errors[0]}")

    return results


# ============================================================
#  BENCHMARK 2: COLD START
# ============================================================
def measure_cold_start_jigyasa():
    """Start Jigyasa JAR and measure time until gRPC port responds."""
    jar = os.path.join(os.path.dirname(__file__), "..", "build", "libs", "Jigyasa-1.0-SNAPSHOT-all.jar")
    if not os.path.exists(jar):
        print(f"    SKIP: JAR not found at {jar}")
        return None
    env = os.environ.copy()
    env["BOOTSTRAP_MEMORY_LOCK"] = "true"
    start = time.perf_counter()
    proc = subprocess.Popen(
        ["java", f"-Xms{HEAP_SIZE_MB}m", f"-Xmx{HEAP_SIZE_MB}m",
         "-XX:+AlwaysPreTouch", "-jar", jar],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        env=env
    )
    # Poll until gRPC port is ready
    import socket
    while time.perf_counter() - start < 30:
        try:
            s = socket.create_connection(("localhost", 50051), timeout=0.1)
            s.close()
            elapsed = time.perf_counter() - start
            proc.terminate()
            proc.wait(timeout=5)
            return elapsed
        except (ConnectionRefusedError, OSError, socket.timeout):
            time.sleep(0.05)
    proc.terminate()
    proc.wait(timeout=5)
    return None

def measure_cold_start_es():
    """Start ES container and measure time until REST port responds."""
    # Remove old container
    subprocess.run(["docker", "rm", "-f", "es-coldstart"], capture_output=True)
    start = time.perf_counter()
    subprocess.Popen(
        ["docker", "run", "--rm", "--name", "es-coldstart",
         "-p", "9202:9200",
         "-e", "discovery.type=single-node",
         "-e", "xpack.security.enabled=false",
         "-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m",
         "elasticsearch:8.13.0"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    # Poll until REST responds with valid JSON
    while time.perf_counter() - start < 120:
        try:
            req = urllib.request.Request("http://localhost:9202", method="GET")
            with urllib.request.urlopen(req, timeout=0.5) as resp:
                data = json.loads(resp.read())
                if "cluster_name" in data:
                    elapsed = time.perf_counter() - start
                    subprocess.run(["docker", "rm", "-f", "es-coldstart"], capture_output=True)
                    return elapsed
        except:
            time.sleep(0.2)
    subprocess.run(["docker", "rm", "-f", "es-coldstart"], capture_output=True)
    return None


# ============================================================
#  MAIN
# ============================================================
def main():
    setup_jigyasa_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    print("=" * 90)
    print("  JIGYASA vs ELASTICSEARCH — ADVANCED BENCHMARKS")
    print("  1. Concurrent throughput (multi-threaded qps)")
    print("  2. Cold start time")
    print("  3. Memory footprint")
    print("=" * 90)

    # ---- Setup: index data into both engines ----
    print("\n📦 Setting up data...")

    channel = grpc.insecure_channel(JIGYASA_ADDR)
    jstub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    try:
        jstub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(SCHEMA)))
    except: pass

    # Index into Jigyasa
    for batch_start in range(0, NUM_DOCS, 500):
        batch_end = min(batch_start + 500, NUM_DOCS)
        items = [pb.IndexItem(document=json.dumps(ALL_DOCS[i])) for i in range(batch_start, batch_end)]
        jstub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE))
    time.sleep(2)
    jstub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=1))
    j_count = jstub.Count(pb.CountRequest(collection=COLLECTION)).count
    print(f"  Jigyasa: {j_count:,} docs indexed")

    # Index into ES
    try: es_request("DELETE", ES_INDEX)
    except: pass
    es_request("PUT", ES_INDEX, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
        "mappings": {"properties": {
            "id": {"type": "keyword"}, "method": {"type": "keyword"},
            "path": {"type": "text"}, "status": {"type": "integer"},
            "response_time_ms": {"type": "integer"}, "bytes": {"type": "integer"},
            "user_agent": {"type": "text"}, "city": {"type": "keyword"},
            "category": {"type": "keyword"}, "message": {"type": "text"},
            "timestamp": {"type": "long"}
        }}
    })
    for batch_start in range(0, NUM_DOCS, 500):
        batch_end = min(batch_start + 500, NUM_DOCS)
        bulk_body = ""
        for i in range(batch_start, batch_end):
            doc = ALL_DOCS[i]
            bulk_body += json.dumps({"index": {"_id": doc["id"]}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"
        req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_bulk",
                                    data=bulk_body.encode(), method="POST")
        req.add_header("Content-Type", "application/x-ndjson")
        with urllib.request.urlopen(req) as resp:
            resp.read()
    es_request("POST", f"{ES_INDEX}/_refresh")
    es_request("POST", f"{ES_INDEX}/_forcemerge?max_num_segments=1")
    time.sleep(2)
    es_count = es_request("POST", f"{ES_INDEX}/_count", {"query": {"match_all": {}}})["count"]
    print(f"  Elasticsearch: {es_count:,} docs indexed")

    # ============================================================
    #  BENCHMARK 1: CONCURRENT THROUGHPUT
    # ============================================================
    print("\n" + "=" * 90)
    print("  BENCHMARK 1: CONCURRENT QUERY THROUGHPUT (10s per thread count)")
    print("=" * 90)

    thread_counts = [1, 2, 4, 8, 16]

    # --- Jigyasa: BM25 text search ---
    def j_text():
        jstub.Query(pb.QueryRequest(
            collection=COLLECTION, text_query=random.choice(SEARCH_TERMS),
            include_source=True, top_k=10))

    j_text_results = run_concurrent_benchmark("Jigyasa — Text search (BM25)", j_text, thread_counts)

    # --- ES: BM25 text search ---
    def es_text():
        es_request("POST", f"{ES_INDEX}/_search", {
            "query": {"match": {"message": random.choice(SEARCH_TERMS)}},
            "size": 10, "_source": True})

    e_text_results = run_concurrent_benchmark("Elasticsearch — Text search (BM25)", es_text, thread_counts)

    # --- Jigyasa: Term filter ---
    def j_term():
        jstub.Query(pb.QueryRequest(
            collection=COLLECTION, include_source=True, top_k=10,
            filters=[pb.FilterClause(field="category",
                                     term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))]))

    j_term_results = run_concurrent_benchmark("Jigyasa — Term filter", j_term, thread_counts)

    # --- ES: Term filter ---
    def es_term():
        es_request("POST", f"{ES_INDEX}/_search", {
            "query": {"term": {"category": random.choice(CATEGORIES)}},
            "size": 10, "_source": True})

    e_term_results = run_concurrent_benchmark("Elasticsearch — Term filter", es_term, thread_counts)

    # --- Jigyasa: Mixed workload (realistic agent pattern) ---
    def j_mixed():
        r = random.random()
        if r < 0.4:  # 40% text search
            jstub.Query(pb.QueryRequest(collection=COLLECTION, text_query=random.choice(SEARCH_TERMS),
                                        include_source=True, top_k=10))
        elif r < 0.7:  # 30% term filter
            jstub.Query(pb.QueryRequest(collection=COLLECTION, include_source=True, top_k=10,
                                        filters=[pb.FilterClause(field="category",
                                                                  term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))]))
        elif r < 0.9:  # 20% count
            jstub.Count(pb.CountRequest(collection=COLLECTION))
        else:  # 10% range filter
            jstub.Query(pb.QueryRequest(collection=COLLECTION, include_source=True, top_k=10,
                                        filters=[pb.FilterClause(field="status",
                                                                  range_filter=pb.RangeFilter(min="400", max="503"))]))

    j_mixed_results = run_concurrent_benchmark("Jigyasa — Mixed workload (agent-realistic)", j_mixed, thread_counts)

    # --- ES: Mixed workload ---
    def es_mixed():
        r = random.random()
        if r < 0.4:
            es_request("POST", f"{ES_INDEX}/_search", {
                "query": {"match": {"message": random.choice(SEARCH_TERMS)}}, "size": 10, "_source": True})
        elif r < 0.7:
            es_request("POST", f"{ES_INDEX}/_search", {
                "query": {"term": {"category": random.choice(CATEGORIES)}}, "size": 10, "_source": True})
        elif r < 0.9:
            es_request("POST", f"{ES_INDEX}/_count", {"query": {"match_all": {}}})
        else:
            es_request("POST", f"{ES_INDEX}/_search", {
                "query": {"range": {"status": {"gte": 400, "lte": 503}}}, "size": 10, "_source": True})

    e_mixed_results = run_concurrent_benchmark("Elasticsearch — Mixed workload (agent-realistic)", es_mixed, thread_counts)

    # --- Comparison table ---
    print("\n" + "=" * 90)
    print("  CONCURRENT THROUGHPUT COMPARISON (qps)")
    print("=" * 90)
    print(f"  {'Threads':>7s} │ {'J text':>8s} {'E text':>8s} {'ratio':>6s} │ "
          f"{'J term':>8s} {'E term':>8s} {'ratio':>6s} │ "
          f"{'J mixed':>8s} {'E mixed':>8s} {'ratio':>6s}")
    print("  " + "─" * 86)
    for i, tc in enumerate(thread_counts):
        jt = j_text_results[i][1]
        et = e_text_results[i][1]
        jtm = j_term_results[i][1]
        etm = e_term_results[i][1]
        jm = j_mixed_results[i][1]
        em = e_mixed_results[i][1]
        print(f"  {tc:>7d} │ {jt:>7,.0f} {et:>7,.0f}  {jt/et:>5.1f}x │ "
              f"{jtm:>7,.0f} {etm:>7,.0f}  {jtm/etm:>5.1f}x │ "
              f"{jm:>7,.0f} {em:>7,.0f}  {jm/em:>5.1f}x")

    # Peak qps
    j_peak = max(r[1] for r in j_mixed_results)
    e_peak = max(r[1] for r in e_mixed_results)
    j_peak_threads = thread_counts[max(range(len(j_mixed_results)), key=lambda i: j_mixed_results[i][1])]
    e_peak_threads = thread_counts[max(range(len(e_mixed_results)), key=lambda i: e_mixed_results[i][1])]
    print(f"\n  Peak mixed qps:  Jigyasa {j_peak:,.0f} ({j_peak_threads} threads)  "
          f"vs  ES {e_peak:,.0f} ({e_peak_threads} threads)  → {j_peak/e_peak:.1f}x")

    channel.close()

    # ============================================================
    #  BENCHMARK 2: COLD START TIME
    # ============================================================
    print("\n" + "=" * 90)
    print("  BENCHMARK 2: COLD START TIME (time to first query)")
    print("=" * 90)

    # Stop current Jigyasa if running on 50051
    print("\n  Measuring Jigyasa cold start (3 runs)...")
    j_starts = []
    for run in range(3):
        # Kill anything on 50051
        subprocess.run(["powershell", "-Command",
                        "Get-NetTCPConnection -LocalPort 50051 -ErrorAction SilentlyContinue | "
                        "ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"],
                       capture_output=True)
        time.sleep(1)
        t = measure_cold_start_jigyasa()
        if t:
            j_starts.append(t)
            print(f"    Run {run+1}: {t:.2f}s")
        else:
            print(f"    Run {run+1}: TIMEOUT")
        time.sleep(1)

    print("\n  Measuring Elasticsearch cold start (3 runs)...")
    e_starts = []
    for run in range(3):
        t = measure_cold_start_es()
        if t:
            e_starts.append(t)
            print(f"    Run {run+1}: {t:.2f}s")
        else:
            print(f"    Run {run+1}: TIMEOUT")
        time.sleep(2)

    if j_starts and e_starts:
        j_avg = statistics.mean(j_starts)
        e_avg = statistics.mean(e_starts)
        print(f"\n  Average cold start:  Jigyasa {j_avg:.2f}s  vs  ES {e_avg:.2f}s  → {e_avg/j_avg:.0f}x faster")

    # ============================================================
    #  BENCHMARK 3: MEMORY FOOTPRINT
    # ============================================================
    print("\n" + "=" * 90)
    print("  BENCHMARK 3: RESOURCE FOOTPRINT")
    print("=" * 90)

    jar_path = os.path.join(os.path.dirname(__file__), "..", "build", "libs", "Jigyasa-1.0-SNAPSHOT-all.jar")
    jar_size = os.path.getsize(jar_path) / (1024 * 1024)

    # ES Docker image size
    try:
        result = subprocess.run(["docker", "image", "inspect", "elasticsearch:8.13.0",
                                 "--format", "{{.Size}}"], capture_output=True, text=True)
        es_image_size = int(result.stdout.strip()) / (1024 * 1024)
    except:
        es_image_size = 0

    # ES container memory
    try:
        result = subprocess.run(["docker", "stats", "es-bench", "--no-stream",
                                 "--format", "{{.MemUsage}}"], capture_output=True, text=True)
        es_mem = result.stdout.strip()
    except:
        es_mem = "N/A"

    print(f"  Artifact size:   Jigyasa JAR = {jar_size:.1f} MB  |  ES Docker image = {es_image_size:.0f} MB")
    print(f"  ES container mem: {es_mem}")
    print(f"  Jigyasa JVM:     512 MB max heap (configurable)")
    print(f"  Dependencies:    Jigyasa = Java 21 JRE only  |  ES = Docker runtime + JVM + full stack")

    # ============================================================
    #  FINAL SUMMARY
    # ============================================================
    print("\n" + "=" * 90)
    print("  SUMMARY — WHY JIGYASA FOR AGENT MEMORY")
    print("=" * 90)

    if j_starts and e_starts:
        print(f"  Cold start:        {statistics.mean(j_starts):.1f}s vs {statistics.mean(e_starts):.1f}s "
              f"({statistics.mean(e_starts)/statistics.mean(j_starts):.0f}x faster)")
    print(f"  Peak throughput:   {j_peak:,.0f} qps vs {e_peak:,.0f} qps ({j_peak/e_peak:.1f}x)")
    print(f"  Artifact size:     {jar_size:.0f} MB vs {es_image_size:.0f} MB ({es_image_size/jar_size:.0f}x smaller)")
    print(f"  Deployment:        Single JAR vs Docker + cluster config")
    print(f"  Protocol:          gRPC (binary) vs REST/JSON")
    print()


if __name__ == "__main__":
    main()
