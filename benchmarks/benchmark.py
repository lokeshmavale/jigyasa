"""
Jigyasa Performance Benchmark
==============================
Modeled after Elasticsearch Rally benchmark patterns.

Datasets simulated:
- HTTP logs (like Rally's http_logs track)
- Geonames-style POI data
- Text-heavy documents (like Rally's pmc/so tracks)

Metrics collected:
- Bulk indexing throughput (docs/sec)
- Query latency: p50, p90, p99, mean (ms)
- Query types: text search, term filter, range filter, 
  boolean compound, query string, match-all, sort, count
"""

import grpc
import json
import sys
import os
import time
import random
import string
import statistics
import subprocess
import site

ADDR = "localhost:50051"
COLLECTION = "benchmark"

# --- Generate Python stubs ---
def generate_stubs():
    proto_path = os.path.join(os.path.dirname(__file__), "..", "src", "main", "proto")
    out_path = os.path.join(os.path.dirname(__file__), "test-data", "gen")
    os.makedirs(out_path, exist_ok=True)
    
    import grpc_tools
    grpc_proto_include = os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')
    sp = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_path}",
        f"--proto_path={grpc_proto_include}",
        f"--proto_path={sp}",
        f"--python_out={out_path}",
        f"--grpc_python_out={out_path}",
        "dpSearch.proto"
    ], capture_output=True, check=True)
    
    sys.path.insert(0, out_path)

# --- Data generators (simulating Rally-style datasets) ---

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 301, 302, 400, 401, 403, 404, 500, 502, 503]
EXTENSIONS = ["html", "php", "js", "css", "json", "png", "jpg", "xml", "txt", "api"]
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

# Realistic text snippets for full-text search benchmarks
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

def generate_http_log(doc_id):
    """Generate a document similar to Rally's http_logs track"""
    ts = int(time.time()) - random.randint(0, 86400 * 30)
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

# --- Benchmark utilities ---

def percentile(data, p):
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[-1]
    return data[f] + (k - f) * (data[c] - data[f])

def run_latency_benchmark(name, func, iterations=200, warmup=20):
    """Run a query benchmark and collect latency stats"""
    # Warmup
    for _ in range(warmup):
        try:
            func()
        except:
            pass
    
    latencies = []
    errors = 0
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            func()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            latencies.append(elapsed)
        except Exception as e:
            errors += 1
    
    if not latencies:
        return {"name": name, "error": "All iterations failed"}
    
    latencies.sort()
    return {
        "name": name,
        "iterations": iterations,
        "errors": errors,
        "mean_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(percentile(latencies, 50), 2),
        "p90_ms": round(percentile(latencies, 90), 2),
        "p99_ms": round(percentile(latencies, 99), 2),
        "min_ms": round(min(latencies), 2),
        "max_ms": round(max(latencies), 2),
        "throughput_qps": round(len(latencies) / (sum(latencies) / 1000), 1),
    }

def print_result(r):
    if "error" in r:
        print(f"  {r['name']}: ERROR - {r['error']}")
        return
    print(f"  {r['name']:40s}  mean={r['mean_ms']:7.2f}ms  p50={r['p50_ms']:7.2f}ms  "
          f"p90={r['p90_ms']:7.2f}ms  p99={r['p99_ms']:7.2f}ms  "
          f"qps={r['throughput_qps']:8.1f}")

def print_comparison_table(results):
    """Print results with ES Rally reference numbers for context"""
    # Reference: ES Rally http_logs on single node (approximate, from public benchmarks)
    # These are ROUGH reference points — ES runs on a cluster, Jigyasa is embedded single-node
    es_reference = {
        "Text search (BM25)": {"p50": 5, "p90": 12, "note": "ES http_logs default query"},
        "Term filter (keyword)": {"p50": 3, "p90": 6, "note": "ES term query"},
        "Range filter (numeric)": {"p50": 4, "p90": 8, "note": "ES range query"},
        "Boolean compound filter": {"p50": 5, "p90": 10, "note": "ES bool query"},
        "Query string (Lucene syntax)": {"p50": 6, "p90": 15, "note": "ES query_string"},
        "Match-all + sort": {"p50": 2, "p90": 5, "note": "ES match_all sorted"},
        "Count API": {"p50": 2, "p90": 4, "note": "ES _count"},
        "Match-all (no source)": {"p50": 1, "p90": 3, "note": "ES match_all"},
    }
    
    print("\n" + "=" * 100)
    print("COMPARISON WITH ELASTICSEARCH REFERENCE (single-node, ~10K docs)")
    print("=" * 100)
    print(f"  {'Query Type':40s}  {'Jigyasa p50':>12s}  {'Jigyasa p90':>12s}  {'ES ref p50':>10s}  {'ES ref p90':>10s}")
    print("  " + "-" * 96)
    for r in results:
        name = r["name"]
        if "error" in r:
            continue
        es = es_reference.get(name, {})
        es_p50 = f"{es.get('p50', '—')}ms" if 'p50' in es else "—"
        es_p90 = f"{es.get('p90', '—')}ms" if 'p90' in es else "—"
        print(f"  {name:40s}  {r['p50_ms']:10.2f}ms  {r['p90_ms']:10.2f}ms  {es_p50:>10s}  {es_p90:>10s}")
    
    print("\n  ⚠ ES reference numbers are approximate single-node figures from public Rally benchmarks.")
    print("    Direct comparison is illustrative only — hardware, dataset size, and config differ.")


def main():
    generate_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc
    
    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)
    
    print("=" * 70)
    print("  JIGYASA PERFORMANCE BENCHMARK")
    print("  Modeled after Elasticsearch Rally (http_logs track)")
    print("=" * 70)
    
    # --- 1. Create collection ---
    print("\n📦 Setting up benchmark collection...")
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION,
            indexSchema=json.dumps(SCHEMA)
        ))
        print("  ✓ Collection created")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("  ✓ Collection already exists")
        else:
            raise
    
    # --- 2. Bulk indexing benchmark ---
    print("\n" + "=" * 70)
    print("  BENCHMARK 1: BULK INDEXING THROUGHPUT")
    print("=" * 70)
    
    for batch_size in [50, 100, 500]:
        total_docs = 10000
        batches = total_docs // batch_size
        
        start = time.perf_counter()
        indexed = 0
        for batch_num in range(batches):
            items = []
            for i in range(batch_size):
                doc_id = batch_num * batch_size + i
                doc = generate_http_log(doc_id)
                items.append(pb.IndexItem(document=json.dumps(doc)))
            stub.Index(pb.IndexRequest(item=items, collection=COLLECTION))
            indexed += batch_size
        
        elapsed = time.perf_counter() - start
        throughput = indexed / elapsed
        print(f"  Batch size {batch_size:>4d}: {indexed:>6d} docs in {elapsed:.2f}s = {throughput:,.0f} docs/sec")
    
    # Wait for NRT refresh
    time.sleep(2)
    
    # Verify count
    count = stub.Count(pb.CountRequest(collection=COLLECTION))
    print(f"\n  Total indexed: {count.count:,} documents")
    
    # --- 3. Force merge for consistent query benchmarks ---
    print("\n  Forcing merge for consistent query benchmarks...")
    stub.ForceMerge(pb.ForceMergeRequest(collection=COLLECTION, max_segments=1))
    time.sleep(1)
    
    health = stub.Health(pb.HealthRequest())
    for c in health.collections:
        if c.name == COLLECTION:
            print(f"  ✓ Segments after merge: {c.segment_count}, docs: {c.doc_count:,}")
    
    # --- 4. Query latency benchmarks ---
    print("\n" + "=" * 70)
    print("  BENCHMARK 2: QUERY LATENCY (200 iterations, 20 warmup)")
    print("=" * 70)
    
    results = []
    
    # 4a. Text search (BM25) — like Rally's "default" query
    search_terms = ["Lucene search engine", "machine learning", "Kubernetes deployment",
                    "distributed systems", "natural language processing", "vector search",
                    "memory management", "gRPC protocol", "Docker container", "search relevance"]
    r = run_latency_benchmark(
        "Text search (BM25)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            text_query=random.choice(search_terms),
            include_source=True,
            top_k=10
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4b. Term filter — like Rally's "term" query
    r = run_latency_benchmark(
        "Term filter (keyword)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            include_source=True,
            top_k=10,
            filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))]
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4c. Range filter — like Rally's "range" query
    r = run_latency_benchmark(
        "Range filter (numeric)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            include_source=True,
            top_k=10,
            filters=[pb.FilterClause(field="status", range_filter=pb.RangeFilter(min="400", max="499"))]
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4d. Boolean compound filter — like Rally's "bool" query
    r = run_latency_benchmark(
        "Boolean compound filter",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            include_source=True,
            top_k=10,
            filters=[pb.FilterClause(compound_filter=pb.CompoundFilter(
                must=[pb.FilterClause(field="method", term_filter=pb.TermFilter(value="GET"))],
                should=[
                    pb.FilterClause(field="category", term_filter=pb.TermFilter(value="api")),
                    pb.FilterClause(field="category", term_filter=pb.TermFilter(value="web")),
                ]
            ))]
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4e. Query string (Lucene syntax) — like Rally's "query_string"
    query_strings = [
        "message:lucene AND message:search",
        "message:machine AND message:learning",
        "message:distributed AND message:systems",
        "message:docker AND message:container",
        "message:vector AND message:search",
    ]
    r = run_latency_benchmark(
        "Query string (Lucene syntax)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            query_string=random.choice(query_strings),
            query_string_default_field="message",
            include_source=True,
            top_k=10
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4f. Match-all + sort — like Rally's "scroll" / sorted query
    r = run_latency_benchmark(
        "Match-all + sort",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            include_source=True,
            top_k=10,
            sort=[pb.SortClause(field="response_time_ms", descending=True)]
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4g. Count API
    r = run_latency_benchmark(
        "Count API",
        lambda: stub.Count(pb.CountRequest(collection=COLLECTION))
    )
    results.append(r)
    print_result(r)
    
    # 4h. Match-all without source (metadata only) — fastest possible
    r = run_latency_benchmark(
        "Match-all (no source)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            top_k=10
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4i. Field projection
    r = run_latency_benchmark(
        "Field projection (2 fields)",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            include_source=True,
            top_k=10,
            source_fields=["method", "status"],
            filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value="api"))]
        ))
    )
    results.append(r)
    print_result(r)
    
    # 4j. Text search + filter combo
    r = run_latency_benchmark(
        "Text + filter combo",
        lambda: stub.Query(pb.QueryRequest(
            collection=COLLECTION,
            text_query=random.choice(search_terms),
            include_source=True,
            top_k=10,
            filters=[pb.FilterClause(field="status", range_filter=pb.RangeFilter(min="200", max="299"))]
        ))
    )
    results.append(r)
    print_result(r)
    
    # --- 5. Comparison table ---
    print_comparison_table(results)
    
    # --- 6. Summary ---
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    avg_p50 = statistics.mean([r["p50_ms"] for r in results if "error" not in r])
    avg_p90 = statistics.mean([r["p90_ms"] for r in results if "error" not in r])
    avg_qps = statistics.mean([r["throughput_qps"] for r in results if "error" not in r])
    print(f"  Average query p50: {avg_p50:.2f}ms")
    print(f"  Average query p90: {avg_p90:.2f}ms")
    print(f"  Average throughput: {avg_qps:,.0f} qps")
    print(f"  Dataset: {count.count:,} HTTP log documents")
    print(f"  Hardware: Local machine (single JVM, single node)")
    print(f"  Lucene: 10.4, Java 21, gRPC 1.80")
    print()

    channel.close()

if __name__ == "__main__":
    main()
