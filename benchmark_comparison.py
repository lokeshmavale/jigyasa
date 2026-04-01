"""
Jigyasa vs Elasticsearch — Apples-to-Apples Benchmark
======================================================
Same dataset, same queries, same hardware, same machine.

- Jigyasa: gRPC on localhost:50051 (embedded Lucene 10.4)
- Elasticsearch 8.13.0: REST on localhost:9201 (Docker, single-node, Lucene 9.10)
- Both: 512MB heap, single node, no replicas
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
COLLECTION = "bench_compare"
ES_INDEX = "bench_compare"
NUM_DOCS = 10000
ITERATIONS = 200
WARMUP = 30

# ---- Data generation (identical for both) ----
random.seed(42)  # Reproducible

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 301, 302, 400, 401, 403, 404, 500, 502, 503]
PATHS = ["/index", "/api/users", "/api/search", "/login", "/dashboard", "/api/data",
         "/static/main.js", "/health", "/api/v2/query", "/docs", "/api/metrics",
         "/checkout", "/api/orders", "/products", "/api/inventory"]
USER_AGENTS = [
    "Mozilla/5.0 Chrome/120", "Mozilla/5.0 Firefox/121", "curl/8.4",
    "python-requests/2.31", "Go-http-client/2.0", "Googlebot/2.1"
]
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

def generate_doc(doc_id):
    ts = 1711900000 + random.randint(0, 86400 * 30)  # Fixed base for reproducibility
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

# Pre-generate all docs once
ALL_DOCS = [generate_doc(i) for i in range(NUM_DOCS)]

# ---- Utilities ----
def percentile(data, p):
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
        return {"name": name, "error": f"All failed"}
    latencies.sort()
    return {
        "name": name, "iterations": iterations, "errors": errors,
        "mean": round(statistics.mean(latencies), 2),
        "p50": round(percentile(latencies, 50), 2),
        "p90": round(percentile(latencies, 90), 2),
        "p99": round(percentile(latencies, 99), 2),
        "qps": round(len(latencies) / (sum(latencies) / 1000), 1),
    }

def es_request(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(f"{ES_URL}/{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def es_search(body):
    return es_request("POST", f"{ES_INDEX}/_search", body)

def es_count(body=None):
    return es_request("POST", f"{ES_INDEX}/_count", body or {"query": {"match_all": {}}})

# ---- Jigyasa gRPC setup ----
def setup_jigyasa_stubs():
    proto_path = os.path.join(os.path.dirname(__file__), "src", "main", "proto")
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

# ============================================================
#  MAIN BENCHMARK
# ============================================================
def main():
    setup_jigyasa_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(JIGYASA_ADDR)
    jstub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

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

    print("=" * 90)
    print("  JIGYASA vs ELASTICSEARCH — HEAD-TO-HEAD BENCHMARK")
    print("  Same data, same queries, same machine")
    print("  Jigyasa: gRPC localhost:50051 (Lucene 10.4, embedded)")
    print("  Elasticsearch 8.13.0: REST localhost:9201 (Docker, Lucene 9.10)")
    print("  Dataset: {:,} HTTP log documents | Iterations: {} | Warmup: {}".format(NUM_DOCS, ITERATIONS, WARMUP))
    print("=" * 90)

    # ---- SETUP JIGYASA ----
    print("\n📦 Setting up Jigyasa collection...")
    try:
        jstub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(SCHEMA)))
        print("  ✓ Collection created")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("  ✓ Collection already exists")
        else:
            raise

    # ---- SETUP ELASTICSEARCH ----
    print("\n📦 Setting up Elasticsearch index...")
    try:
        es_request("DELETE", ES_INDEX)
    except:
        pass
    es_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1"  # Manual refresh like Jigyasa NRT
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "method": {"type": "keyword"},
                "path": {"type": "text"},
                "status": {"type": "integer"},
                "response_time_ms": {"type": "integer"},
                "bytes": {"type": "integer"},
                "user_agent": {"type": "text"},
                "city": {"type": "keyword"},
                "category": {"type": "keyword"},
                "message": {"type": "text"},
                "timestamp": {"type": "long"}
            }
        }
    }
    es_request("PUT", ES_INDEX, es_mapping)
    print("  ✓ ES index created (1 shard, 0 replicas)")

    # ============================================================
    #  BENCHMARK 1: BULK INDEXING
    # ============================================================
    print("\n" + "=" * 90)
    print("  BENCHMARK 1: BULK INDEXING THROUGHPUT")
    print("=" * 90)

    for batch_size in [100, 500]:
        # --- Jigyasa ---
        random.seed(42)
        try:
            jstub.CreateCollection(pb.CreateCollectionRequest(
                collection=COLLECTION, indexSchema=json.dumps(SCHEMA)))
        except:
            pass

        start = time.perf_counter()
        for batch_start in range(0, NUM_DOCS, batch_size):
            batch_end = min(batch_start + batch_size, NUM_DOCS)
            items = [pb.IndexItem(document=json.dumps(ALL_DOCS[i])) for i in range(batch_start, batch_end)]
            jstub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE))
        j_elapsed = time.perf_counter() - start
        j_throughput = NUM_DOCS / j_elapsed

        # --- Elasticsearch ---
        try:
            es_request("DELETE", ES_INDEX)
            es_request("PUT", ES_INDEX, es_mapping)
        except:
            pass

        start = time.perf_counter()
        for batch_start in range(0, NUM_DOCS, batch_size):
            batch_end = min(batch_start + batch_size, NUM_DOCS)
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
        es_elapsed = time.perf_counter() - start
        es_throughput = NUM_DOCS / es_elapsed

        ratio = j_throughput / es_throughput
        winner = "Jigyasa" if ratio > 1 else "ES"
        print(f"  Batch {batch_size:>4d}: Jigyasa={j_throughput:>8,.0f} docs/s ({j_elapsed:.2f}s)  "
              f"ES={es_throughput:>8,.0f} docs/s ({es_elapsed:.2f}s)  "
              f"→ {winner} {max(ratio, 1/ratio):.1f}x faster")

    # Refresh ES to make docs searchable
    es_request("POST", f"{ES_INDEX}/_refresh")
    time.sleep(2)  # Jigyasa NRT refresh

    # Force merge both
    print("\n  Force-merging both engines to 1 segment...")
    jstub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=1))
    es_request("POST", f"{ES_INDEX}/_forcemerge?max_num_segments=1")
    time.sleep(2)

    # Verify counts
    j_count = jstub.Count(pb.CountRequest(collection=COLLECTION)).count
    es_count_val = es_count()["count"]
    print(f"  Jigyasa docs: {j_count:,} | ES docs: {es_count_val:,}")

    # ============================================================
    #  BENCHMARK 2: QUERY LATENCY
    # ============================================================
    print("\n" + "=" * 90)
    print("  BENCHMARK 2: QUERY LATENCY ({} iterations, {} warmup)".format(ITERATIONS, WARMUP))
    print("=" * 90)

    search_terms = ["Lucene search engine", "machine learning", "Kubernetes deployment",
                    "distributed systems", "natural language processing", "vector search",
                    "memory management", "gRPC protocol", "Docker container", "search relevance"]

    benchmarks = []

    # ---- 1. BM25 text search ----
    j = run_bench("Text search (BM25)", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, text_query=random.choice(search_terms),
        include_source=True, top_k=10)))
    e = run_bench("Text search (BM25)", lambda: es_search({
        "query": {"match": {"message": random.choice(search_terms)}},
        "size": 10, "_source": True}))
    benchmarks.append(("Text search (BM25)", j, e))

    # ---- 2. Term filter ----
    j = run_bench("Term filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(field="category",
                                 term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))])))
    e = run_bench("Term filter", lambda: es_search({
        "query": {"term": {"category": random.choice(CATEGORIES)}},
        "size": 10, "_source": True}))
    benchmarks.append(("Term filter (keyword)", j, e))

    # ---- 3. Range filter ----
    j = run_bench("Range filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(field="status",
                                 range_filter=pb.RangeFilter(min="400", max="503"))])))
    e = run_bench("Range filter", lambda: es_search({
        "query": {"range": {"status": {"gte": 400, "lte": 503}}},
        "size": 10, "_source": True}))
    benchmarks.append(("Range filter (numeric)", j, e))

    # ---- 4. Boolean compound ----
    j = run_bench("Bool compound", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        filters=[pb.FilterClause(compound_filter=pb.CompoundFilter(
            must=[pb.FilterClause(field="method", term_filter=pb.TermFilter(value="GET"))],
            should=[
                pb.FilterClause(field="category", term_filter=pb.TermFilter(value="api")),
                pb.FilterClause(field="category", term_filter=pb.TermFilter(value="web")),
            ]))])))
    e = run_bench("Bool compound", lambda: es_search({
        "query": {"bool": {
            "must": [{"term": {"method": "GET"}}],
            "should": [{"term": {"category": "api"}}, {"term": {"category": "web"}}]
        }}, "size": 10, "_source": True}))
    benchmarks.append(("Boolean compound", j, e))

    # ---- 5. Query string ----
    qs = ["message:lucene AND message:search", "message:machine AND message:learning",
          "message:distributed AND message:systems", "message:docker AND message:container"]
    j = run_bench("Query string", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, query_string=random.choice(qs),
        query_string_default_field="message", include_source=True, top_k=10)))
    e = run_bench("Query string", lambda: es_search({
        "query": {"query_string": {"query": random.choice(qs), "default_field": "message"}},
        "size": 10, "_source": True}))
    benchmarks.append(("Query string", j, e))

    # ---- 6. Match-all + sort ----
    j = run_bench("Match-all+sort", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, include_source=True, top_k=10,
        sort=[pb.SortClause(field="response_time_ms", descending=True)])))
    e = run_bench("Match-all+sort", lambda: es_search({
        "query": {"match_all": {}},
        "sort": [{"response_time_ms": "desc"}],
        "size": 10, "_source": True}))
    benchmarks.append(("Match-all + sort", j, e))

    # ---- 7. Count ----
    j = run_bench("Count", lambda: jstub.Count(pb.CountRequest(collection=COLLECTION)))
    e = run_bench("Count", lambda: es_count())
    benchmarks.append(("Count API", j, e))

    # ---- 8. Text + filter combo ----
    j = run_bench("Text+filter", lambda: jstub.Query(pb.QueryRequest(
        collection=COLLECTION, text_query=random.choice(search_terms),
        include_source=True, top_k=10,
        filters=[pb.FilterClause(field="status",
                                 range_filter=pb.RangeFilter(min="200", max="299"))])))
    e = run_bench("Text+filter", lambda: es_search({
        "query": {"bool": {
            "must": [{"match": {"message": random.choice(search_terms)}}],
            "filter": [{"range": {"status": {"gte": 200, "lte": 299}}}]
        }}, "size": 10, "_source": True}))
    benchmarks.append(("Text + filter combo", j, e))

    # ============================================================
    #  RESULTS TABLE
    # ============================================================
    print("\n" + "=" * 110)
    print("  RESULTS: JIGYASA vs ELASTICSEARCH (lower is better)")
    print("=" * 110)
    print(f"  {'Query Type':30s} │ {'Jigyasa p50':>11s} {'p90':>8s} {'qps':>8s} │ {'ES p50':>11s} {'p90':>8s} {'qps':>8s} │ {'Winner':>10s}")
    print("  " + "─" * 106)

    j_wins = 0
    e_wins = 0
    for name, j, e in benchmarks:
        if "error" in j or "error" in e:
            print(f"  {name:30s} │ {'ERROR':>11s} {'':>8s} {'':>8s} │ {'ERROR':>11s} {'':>8s} {'':>8s} │")
            continue
        ratio = e["p50"] / j["p50"] if j["p50"] > 0 else 999
        if ratio > 1:
            winner = f"J {ratio:.1f}x"
            j_wins += 1
        else:
            winner = f"ES {1/ratio:.1f}x"
            e_wins += 1
        print(f"  {name:30s} │ {j['p50']:8.2f}ms {j['p90']:6.2f}ms {j['qps']:7.0f} │ "
              f"{e['p50']:8.2f}ms {e['p90']:6.2f}ms {e['qps']:7.0f} │ {winner:>10s}")

    print("  " + "─" * 106)
    print(f"  Score: Jigyasa {j_wins} — Elasticsearch {e_wins}")

    # ============================================================
    #  SUMMARY
    # ============================================================
    j_avg_p50 = statistics.mean([j["p50"] for _, j, e in benchmarks if "error" not in j])
    e_avg_p50 = statistics.mean([e["p50"] for _, j, e in benchmarks if "error" not in e])
    j_avg_qps = statistics.mean([j["qps"] for _, j, e in benchmarks if "error" not in j])
    e_avg_qps = statistics.mean([e["qps"] for _, j, e in benchmarks if "error" not in e])

    print(f"\n  Average p50:  Jigyasa {j_avg_p50:.2f}ms  vs  ES {e_avg_p50:.2f}ms")
    print(f"  Average qps:  Jigyasa {j_avg_qps:,.0f}  vs  ES {e_avg_qps:,.0f}")
    print(f"\n  Config: {NUM_DOCS:,} docs, 1 shard, 0 replicas, force-merged to 1 segment")
    print(f"  ES 8.13.0 (Lucene 9.10) in Docker | Jigyasa (Lucene 10.4) native JVM")
    print(f"  Both on same machine, 512MB JVM heap")
    print()

    channel.close()

if __name__ == "__main__":
    main()
