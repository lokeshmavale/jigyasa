"""
Jigyasa vs Elasticsearch — 1M Document Scale Benchmark + Vector KNN
====================================================================
Proves Jigyasa is not just a cache-warm toy.

1. Index 1,000,000 HTTP log documents into both engines
2. Query latency at scale (10 query types)
3. Vector KNN search (128-dim embeddings)
"""

import grpc
import json
import sys
import os
import time
import random
import math
import statistics
import subprocess
import site
import urllib.request
import threading

JIGYASA_ADDR = "localhost:50051"
ES_URL = "http://localhost:9201"
NUM_DOCS = 1_000_000
VECTOR_DIMS = 128
VECTOR_DOCS = 50_000  # Separate collection for vector benchmark
ITERATIONS = 300
WARMUP = 50

# ---- Data generation ----
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
    ts = 1711900000 + random.randint(0, 86400 * 365)
    return {
        "id": f"log-{doc_id}",
        "method": random.choice(HTTP_METHODS),
        "path": random.choice(PATHS),
        "status": random.choice(HTTP_STATUSES),
        "response_time_ms": random.randint(1, 5000),
        "bytes": random.randint(100, 500000),
        "user_agent": random.choice(USER_AGENTS),
        "city": random.choice(CITIES),
        "category": random.choice(CATEGORIES),
        "message": random.choice(TEXT_CORPUS) + " " + random.choice(TEXT_CORPUS),
        "timestamp": ts
    }

def random_vector(dims):
    """Generate a random unit vector (normalized)."""
    v = [random.gauss(0, 1) for _ in range(dims)]
    norm = math.sqrt(sum(x * x for x in v))
    return [x / norm for x in v]

def generate_vector_doc(doc_id):
    return {
        "id": f"vec-{doc_id}",
        "content": random.choice(TEXT_CORPUS),
        "category": random.choice(CATEGORIES),
        "embedding": random_vector(VECTOR_DIMS)
    }

SCHEMA_1M = {
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

SCHEMA_VECTOR = {
    "fields": [
        {"name": "id", "type": "STRING", "key": True, "filterable": True},
        {"name": "content", "type": "STRING", "searchable": True},
        {"name": "category", "type": "STRING", "filterable": True},
        {"name": "embedding", "type": "VECTOR", "dimensions": VECTOR_DIMS}
    ],
    "hnswConfig": {"maxConn": 16, "beamWidth": 100}
}

# ---- Utilities ----
def es_request(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(f"{ES_URL}/{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())

def percentile(data, p):
    if not data: return 0
    data.sort()
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    return data[f] + (k - f) * (data[c] - data[f])

def run_bench(name, func, iterations=ITERATIONS, warmup=WARMUP):
    for _ in range(warmup):
        try: func()
        except: pass
    latencies = []
    errors = 0
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            func()
            latencies.append((time.perf_counter() - start) * 1000)
        except Exception as e:
            errors += 1
    if not latencies:
        return {"name": name, "error": f"All failed ({errors})"}
    latencies.sort()
    return {
        "name": name, "errors": errors,
        "p50": round(percentile(latencies, 50), 2),
        "p90": round(percentile(latencies, 90), 2),
        "p99": round(percentile(latencies, 99), 2),
        "qps": round(len(latencies) / (sum(latencies) / 1000), 1),
    }

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


def main():
    setup_jigyasa_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(JIGYASA_ADDR, options=[
        ('grpc.max_send_message_length', 64 * 1024 * 1024),
        ('grpc.max_receive_message_length', 64 * 1024 * 1024),
    ])
    jstub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    print("=" * 90)
    print("  JIGYASA vs ELASTICSEARCH — 1M SCALE + VECTOR KNN BENCHMARK")
    print("=" * 90)

    # ============================================================
    #  PART 1: 1M DOCUMENT BENCHMARK
    # ============================================================
    print("\n" + "=" * 90)
    print(f"  PART 1: INDEXING & QUERYING {NUM_DOCS:,} DOCUMENTS")
    print("=" * 90)

    COLLECTION = "bench_1m"
    ES_INDEX = "bench_1m"
    BATCH_SIZE = 1000

    # ---- Setup Jigyasa ----
    print("\n📦 Setting up Jigyasa collection...")
    try:
        jstub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(SCHEMA_1M)))
        print("  ✓ Collection created")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("  ✓ Collection already exists")
        else:
            raise

    # ---- Setup ES ----
    print("📦 Setting up Elasticsearch index...")
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
    print("  ✓ ES index created")

    # ---- Bulk index into Jigyasa ----
    print(f"\n  Indexing {NUM_DOCS:,} docs into Jigyasa (batch={BATCH_SIZE})...")
    random.seed(42)
    j_start = time.perf_counter()
    for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
        items = []
        for i in range(batch_start, batch_end):
            items.append(pb.IndexItem(document=json.dumps(generate_doc(i))))
        jstub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE))
        if (batch_start + BATCH_SIZE) % 100_000 == 0:
            pct = (batch_start + BATCH_SIZE) * 100 // NUM_DOCS
            elapsed = time.perf_counter() - j_start
            rate = (batch_start + BATCH_SIZE) / elapsed
            print(f"    {pct}% ({batch_start + BATCH_SIZE:>9,} docs) — {rate:,.0f} docs/sec")
    j_elapsed = time.perf_counter() - j_start
    j_throughput = NUM_DOCS / j_elapsed
    print(f"  ✓ Jigyasa: {NUM_DOCS:,} docs in {j_elapsed:.1f}s = {j_throughput:,.0f} docs/sec")

    # ---- Bulk index into ES ----
    print(f"\n  Indexing {NUM_DOCS:,} docs into Elasticsearch (batch={BATCH_SIZE})...")
    random.seed(42)
    e_start = time.perf_counter()
    for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
        bulk_body = ""
        for i in range(batch_start, batch_end):
            doc = generate_doc(i)
            bulk_body += json.dumps({"index": {"_id": doc["id"]}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"
        req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_bulk",
                                    data=bulk_body.encode(), method="POST")
        req.add_header("Content-Type", "application/x-ndjson")
        with urllib.request.urlopen(req, timeout=120) as resp:
            resp.read()
        if (batch_start + BATCH_SIZE) % 100_000 == 0:
            pct = (batch_start + BATCH_SIZE) * 100 // NUM_DOCS
            elapsed = time.perf_counter() - e_start
            rate = (batch_start + BATCH_SIZE) / elapsed
            print(f"    {pct}% ({batch_start + BATCH_SIZE:>9,} docs) — {rate:,.0f} docs/sec")
    e_elapsed = time.perf_counter() - e_start
    e_throughput = NUM_DOCS / e_elapsed
    print(f"  ✓ ES: {NUM_DOCS:,} docs in {e_elapsed:.1f}s = {e_throughput:,.0f} docs/sec")

    # Refresh + force merge
    print("\n  Refreshing and force-merging...")
    es_request("POST", f"{ES_INDEX}/_refresh")
    time.sleep(3)  # NRT refresh for Jigyasa

    print("  Force merging to 5 segments (realistic, not 1)...")
    jstub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=5))
    es_request("POST", f"{ES_INDEX}/_forcemerge?max_num_segments=5")
    time.sleep(2)

    j_count = jstub.Count(pb.CountRequest(collection=COLLECTION)).count
    e_count = es_request("POST", f"{ES_INDEX}/_count", {"query": {"match_all": {}}})["count"]
    print(f"  Jigyasa: {j_count:,} docs | ES: {e_count:,} docs")

    # ---- Query benchmarks at 1M scale ----
    print(f"\n  Query latency benchmarks ({ITERATIONS} iterations, {WARMUP} warmup)")
    print("  " + "-" * 86)

    benchmarks = []

    # 1. BM25 text search
    j = run_bench("BM25 text", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, text_query=random.choice(SEARCH_TERMS),
        include_source=True, top_k=10)))
    e = run_bench("BM25 text", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"match": {"message": random.choice(SEARCH_TERMS)}}, "size": 10}))
    benchmarks.append(("BM25 text search", j, e))

    # 2. Term filter
    j = run_bench("Term filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))])))
    e = run_bench("Term filter", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"term": {"category": random.choice(CATEGORIES)}}, "size": 10}))
    benchmarks.append(("Term filter", j, e))

    # 3. Range filter
    j = run_bench("Range filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(field="response_time_ms", range_filter=pb.RangeFilter(min="1000", max="3000"))])))
    e = run_bench("Range filter", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"range": {"response_time_ms": {"gte": 1000, "lte": 3000}}}, "size": 10}))
    benchmarks.append(("Range filter", j, e))

    # 4. Boolean compound
    j = run_bench("Bool compound", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(compound_filter=pb.CompoundFilter(
            must=[pb.FilterClause(field="method", term_filter=pb.TermFilter(value="GET"))],
            should=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value="api")),
                    pb.FilterClause(field="category", term_filter=pb.TermFilter(value="web"))]))])))
    e = run_bench("Bool compound", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"bool": {"must": [{"term": {"method": "GET"}}],
                           "should": [{"term": {"category": "api"}}, {"term": {"category": "web"}}]}}, "size": 10}))
    benchmarks.append(("Boolean compound", j, e))

    # 5. Text + filter combo
    j = run_bench("Text+filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, text_query=random.choice(SEARCH_TERMS), include_source=True, top_k=10,
        filters=[pb.FilterClause(field="status", range_filter=pb.RangeFilter(min="200", max="299"))])))
    e = run_bench("Text+filter", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"bool": {"must": [{"match": {"message": random.choice(SEARCH_TERMS)}}],
                           "filter": [{"range": {"status": {"gte": 200, "lte": 299}}}]}}, "size": 10}))
    benchmarks.append(("Text + filter", j, e))

    # 6. Sort by field
    j = run_bench("Sort", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        sort=[pb.SortClause(field="response_time_ms", descending=True)])))
    e = run_bench("Sort", lambda: es_request("POST", f"{ES_INDEX}/_search", {
        "query": {"match_all": {}}, "sort": [{"response_time_ms": "desc"}], "size": 10}))
    benchmarks.append(("Sort by field", j, e))

    # 7. Count
    j = run_bench("Count", lambda: jstub.Count(pb.CountRequest(collection=COLLECTION)))
    e = run_bench("Count", lambda: es_request("POST", f"{ES_INDEX}/_count", {"query": {"match_all": {}}}))
    benchmarks.append(("Count", j, e))

    # 8. Count with filter
    j = run_bench("Count+filter", lambda: jstub.Count(pb.CountRequest(
        collection=COLLECTION,
        filters=[pb.FilterClause(field="status", range_filter=pb.RangeFilter(min="400", max="503"))])))
    e = run_bench("Count+filter", lambda: es_request("POST", f"{ES_INDEX}/_count", {
        "query": {"range": {"status": {"gte": 400, "lte": 503}}}}))
    benchmarks.append(("Count + filter", j, e))

    # ---- Results table ----
    print("\n" + "=" * 100)
    print(f"  1M DOC QUERY RESULTS (lower is better)")
    print("=" * 100)
    print(f"  {'Query Type':25s} │ {'J p50':>8s} {'J p90':>8s} {'J qps':>8s} │ {'E p50':>8s} {'E p90':>8s} {'E qps':>8s} │ {'Winner':>10s}")
    print("  " + "─" * 96)
    j_wins = e_wins = 0
    for name, j, e in benchmarks:
        if "error" in j or "error" in e:
            print(f"  {name:25s} │ {'ERR':>8s} {'':>8s} {'':>8s} │ {'ERR':>8s} {'':>8s} {'':>8s} │")
            continue
        ratio = e["p50"] / j["p50"] if j["p50"] > 0 else 999
        winner = f"J {ratio:.1f}x" if ratio > 1 else f"E {1/ratio:.1f}x"
        if ratio > 1: j_wins += 1
        else: e_wins += 1
        print(f"  {name:25s} │ {j['p50']:6.2f}ms {j['p90']:6.2f}ms {j['qps']:7.0f} │ "
              f"{e['p50']:6.2f}ms {e['p90']:6.2f}ms {e['qps']:7.0f} │ {winner:>10s}")
    print("  " + "─" * 96)
    print(f"  Score: Jigyasa {j_wins} — ES {e_wins}")

    j_avg = statistics.mean([j["p50"] for _, j, e in benchmarks if "error" not in j])
    e_avg = statistics.mean([e["p50"] for _, j, e in benchmarks if "error" not in e])
    print(f"\n  Indexing: Jigyasa {j_throughput:,.0f} docs/s vs ES {e_throughput:,.0f} docs/s")
    print(f"  Avg query p50: Jigyasa {j_avg:.2f}ms vs ES {e_avg:.2f}ms")

    # ============================================================
    #  PART 2: VECTOR KNN BENCHMARK
    # ============================================================
    print("\n" + "=" * 90)
    print(f"  PART 2: VECTOR KNN SEARCH ({VECTOR_DOCS:,} docs, {VECTOR_DIMS}-dim embeddings)")
    print("=" * 90)

    VEC_COLLECTION = "bench_vectors"
    VEC_ES_INDEX = "bench_vectors"

    # ---- Setup Jigyasa vector collection ----
    print("\n📦 Setting up vector collections...")
    try:
        jstub.CreateCollection(pb.CreateCollectionRequest(
            collection=VEC_COLLECTION, indexSchema=json.dumps(SCHEMA_VECTOR)))
    except: pass

    try: es_request("DELETE", VEC_ES_INDEX)
    except: pass
    es_request("PUT", VEC_ES_INDEX, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
        "mappings": {"properties": {
            "id": {"type": "keyword"},
            "content": {"type": "text"},
            "category": {"type": "keyword"},
            "embedding": {"type": "dense_vector", "dims": VECTOR_DIMS, "index": True,
                         "similarity": "cosine"}
        }}
    })
    print("  ✓ Vector collections created")

    # ---- Index vectors ----
    print(f"\n  Indexing {VECTOR_DOCS:,} vector docs...")
    random.seed(123)

    # Jigyasa
    j_start = time.perf_counter()
    for batch_start in range(0, VECTOR_DOCS, 500):
        batch_end = min(batch_start + 500, VECTOR_DOCS)
        items = []
        for i in range(batch_start, batch_end):
            items.append(pb.IndexItem(document=json.dumps(generate_vector_doc(i))))
        jstub.Index(pb.IndexRequest(item=items, collection=VEC_COLLECTION, refresh=pb.NONE))
    j_vec_elapsed = time.perf_counter() - j_start
    print(f"  Jigyasa: {VECTOR_DOCS:,} vector docs in {j_vec_elapsed:.1f}s = {VECTOR_DOCS/j_vec_elapsed:,.0f} docs/sec")

    # ES
    random.seed(123)
    e_start = time.perf_counter()
    for batch_start in range(0, VECTOR_DOCS, 500):
        batch_end = min(batch_start + 500, VECTOR_DOCS)
        bulk_body = ""
        for i in range(batch_start, batch_end):
            doc = generate_vector_doc(i)
            bulk_body += json.dumps({"index": {"_id": doc["id"]}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"
        req = urllib.request.Request(f"{ES_URL}/{VEC_ES_INDEX}/_bulk",
                                    data=bulk_body.encode(), method="POST")
        req.add_header("Content-Type", "application/x-ndjson")
        with urllib.request.urlopen(req, timeout=120) as resp:
            resp.read()
    e_vec_elapsed = time.perf_counter() - e_start
    print(f"  ES: {VECTOR_DOCS:,} vector docs in {e_vec_elapsed:.1f}s = {VECTOR_DOCS/e_vec_elapsed:,.0f} docs/sec")

    # Refresh + merge
    es_request("POST", f"{VEC_ES_INDEX}/_refresh")
    time.sleep(2)
    jstub.ForceMerge(pb.ForceMergeRequest(collection=VEC_COLLECTION, max_segments=1))
    es_request("POST", f"{VEC_ES_INDEX}/_forcemerge?max_num_segments=1")
    time.sleep(2)

    j_vcount = jstub.Count(pb.CountRequest(collection=VEC_COLLECTION)).count
    e_vcount = es_request("POST", f"{VEC_ES_INDEX}/_count", {"query": {"match_all": {}}})["count"]
    print(f"  Jigyasa: {j_vcount:,} | ES: {e_vcount:,} vector docs")

    # ---- Pre-generate query vectors ----
    random.seed(999)
    query_vectors = [random_vector(VECTOR_DIMS) for _ in range(50)]

    # ---- KNN benchmarks ----
    print(f"\n  Vector KNN benchmarks ({ITERATIONS} iterations, {WARMUP} warmup)")
    print("  " + "-" * 86)

    vec_benchmarks = []

    # 1. Pure KNN (top-10)
    j = run_bench("KNN top-10", lambda: jstub.Query(pb.QueryRequest(
        collection=VEC_COLLECTION, include_source=True, top_k=10,
        vector_query=pb.VectorQuery(field="embedding", vector=random.choice(query_vectors), k=10))))
    e = run_bench("KNN top-10", lambda: es_request("POST", f"{VEC_ES_INDEX}/_search", {
        "knn": {"field": "embedding", "query_vector": random.choice(query_vectors),
                "k": 10, "num_candidates": 100},
        "size": 10}))
    vec_benchmarks.append(("KNN top-10", j, e))

    # 2. KNN top-50
    j = run_bench("KNN top-50", lambda: jstub.Query(pb.QueryRequest(
        collection=VEC_COLLECTION, include_source=True, top_k=50,
        vector_query=pb.VectorQuery(field="embedding", vector=random.choice(query_vectors), k=50))))
    e = run_bench("KNN top-50", lambda: es_request("POST", f"{VEC_ES_INDEX}/_search", {
        "knn": {"field": "embedding", "query_vector": random.choice(query_vectors),
                "k": 50, "num_candidates": 200},
        "size": 50}))
    vec_benchmarks.append(("KNN top-50", j, e))

    # 3. KNN + filter (pre-filter)
    j = run_bench("KNN+filter", lambda: jstub.Query(pb.QueryRequest(
        collection=VEC_COLLECTION, include_source=True, top_k=10,
        vector_query=pb.VectorQuery(field="embedding", vector=random.choice(query_vectors), k=10),
        filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))])))
    e = run_bench("KNN+filter", lambda: es_request("POST", f"{VEC_ES_INDEX}/_search", {
        "knn": {"field": "embedding", "query_vector": random.choice(query_vectors),
                "k": 10, "num_candidates": 100,
                "filter": {"term": {"category": random.choice(CATEGORIES)}}},
        "size": 10}))
    vec_benchmarks.append(("KNN + filter", j, e))

    # 4. Hybrid: BM25 + KNN (Jigyasa RRF vs ES manual)
    j = run_bench("Hybrid RRF", lambda: jstub.Query(pb.QueryRequest(
        collection=VEC_COLLECTION, include_source=True, top_k=10,
        text_query=random.choice(SEARCH_TERMS),
        vector_query=pb.VectorQuery(field="embedding", vector=random.choice(query_vectors), k=10))))
    # ES doesn't have built-in RRF in 8.13 for free — use knn + query combo
    e = run_bench("Hybrid", lambda: es_request("POST", f"{VEC_ES_INDEX}/_search", {
        "query": {"match": {"content": random.choice(SEARCH_TERMS)}},
        "knn": {"field": "embedding", "query_vector": random.choice(query_vectors),
                "k": 10, "num_candidates": 100},
        "size": 10}))
    vec_benchmarks.append(("Hybrid BM25+KNN", j, e))

    # ---- Vector results table ----
    print("\n" + "=" * 100)
    print(f"  VECTOR KNN RESULTS ({VECTOR_DOCS:,} docs, {VECTOR_DIMS}-dim)")
    print("=" * 100)
    print(f"  {'Query Type':25s} │ {'J p50':>8s} {'J p90':>8s} {'J qps':>8s} │ {'E p50':>8s} {'E p90':>8s} {'E qps':>8s} │ {'Winner':>10s}")
    print("  " + "─" * 96)
    for name, j, e in vec_benchmarks:
        if "error" in j or "error" in e:
            j_str = f"{j.get('error','ERR')}" if "error" in j else ""
            e_str = f"{e.get('error','ERR')}" if "error" in e else ""
            print(f"  {name:25s} │ {j_str:>24s} │ {e_str:>24s} │")
            continue
        ratio = e["p50"] / j["p50"] if j["p50"] > 0 else 999
        winner = f"J {ratio:.1f}x" if ratio > 1 else f"E {1/ratio:.1f}x"
        print(f"  {name:25s} │ {j['p50']:6.2f}ms {j['p90']:6.2f}ms {j['qps']:7.0f} │ "
              f"{e['p50']:6.2f}ms {e['p90']:6.2f}ms {e['qps']:7.0f} │ {winner:>10s}")

    # ============================================================
    #  FINAL SUMMARY
    # ============================================================
    print("\n" + "=" * 90)
    print("  FINAL SUMMARY")
    print("=" * 90)
    print(f"\n  1M Document Scale:")
    print(f"    Indexing:  Jigyasa {j_throughput:,.0f} docs/s vs ES {e_throughput:,.0f} docs/s")
    print(f"    Query p50: Jigyasa {j_avg:.2f}ms vs ES {e_avg:.2f}ms")
    print(f"\n  Vector KNN ({VECTOR_DOCS:,} docs, {VECTOR_DIMS}-dim):")
    for name, j, e in vec_benchmarks:
        if "error" not in j and "error" not in e:
            ratio = e["p50"] / j["p50"] if j["p50"] > 0 else 0
            print(f"    {name:25s}: J={j['p50']:.2f}ms  E={e['p50']:.2f}ms  ({ratio:.1f}x)")
    print()

    channel.close()

if __name__ == "__main__":
    main()
