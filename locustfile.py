"""
Jigyasa Locust Load Test
=========================
gRPC load testing for indexing and querying with Locust.

Usage:
    # Start Jigyasa server first, then:
    locust -f locustfile.py --headless -u 50 -r 10 -t 60s --host=localhost:50051
    
    # Or with web UI:
    locust -f locustfile.py --host=localhost:50051
    # Then open http://localhost:8089

Profiles:
    - IndexUser:  bulk indexing (batches of 10-100 docs)
    - QueryUser:  mixed queries (BM25, filter, count, lookup)
    - HybridUser: realistic mix (20% writes, 80% reads)
"""

import grpc
import json
import os
import sys
import time
import random
import string
import subprocess
import site

from locust import User, task, between, events, tag

# --- Generate gRPC stubs ---
def _generate_stubs():
    proto_path = os.path.join(os.path.dirname(__file__), "src", "main", "proto")
    out_path = os.path.join(os.path.dirname(__file__), "benchmarks", "test-data", "gen")
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

_generate_stubs()
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

# --- Test data ---
COLLECTION = "locust_bench"
SEARCH_TERMS = ["search", "engine", "database", "vector", "lucene", "query",
                "index", "document", "filter", "performance", "machine learning",
                "kubernetes", "microservice", "distributed", "analytics"]
CATEGORIES = ["tech", "science", "business", "health", "sports", "entertainment"]
DOC_COUNTER = 0

SCHEMA = {
    "fields": [
        {"name": "id", "type": "STRING", "key": True, "filterable": True},
        {"name": "title", "type": "STRING", "searchable": True, "filterable": True},
        {"name": "body", "type": "STRING", "searchable": True},
        {"name": "category", "type": "STRING", "filterable": True},
        {"name": "score", "type": "DOUBLE", "filterable": True, "sortable": True},
        {"name": "views", "type": "INT32", "filterable": True, "sortable": True}
    ]
}


def _random_doc():
    global DOC_COUNTER
    DOC_COUNTER += 1
    return {
        "id": f"loc-{DOC_COUNTER}-{random.randint(0, 999999)}",
        "title": " ".join(random.choices(SEARCH_TERMS, k=random.randint(3, 8))),
        "body": " ".join(random.choices(SEARCH_TERMS, k=random.randint(10, 50))),
        "category": random.choice(CATEGORIES),
        "score": round(random.uniform(0, 100), 2),
        "views": random.randint(0, 100000)
    }


# --- gRPC Locust integration ---
def _grpc_call(user, name, fn):
    """Execute a gRPC call and report timing to Locust."""
    start = time.perf_counter()
    try:
        result = fn()
        elapsed_ms = (time.perf_counter() - start) * 1000
        events.request.fire(
            request_type="gRPC",
            name=name,
            response_time=elapsed_ms,
            response_length=0,
            exception=None,
            context={}
        )
        return result
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        events.request.fire(
            request_type="gRPC",
            name=name,
            response_time=elapsed_ms,
            response_length=0,
            exception=e,
            context={}
        )
        return None


# --- Setup: create collection once ---
_setup_done = False

def _ensure_collection(host):
    global _setup_done
    if _setup_done:
        return
    _setup_done = True
    channel = grpc.insecure_channel(host)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=json.dumps(SCHEMA)), timeout=10)
        print(f"[SETUP] Created collection '{COLLECTION}'")
    except grpc.RpcError:
        print(f"[SETUP] Collection '{COLLECTION}' already exists")

    # Seed with 1000 docs for queries to find
    print("[SETUP] Seeding 1000 documents...")
    for batch_start in range(0, 1000, 100):
        items = [pb.IndexItem(action=pb.UPDATE, document=json.dumps(_random_doc()))
                 for _ in range(100)]
        stub.Index(pb.IndexRequest(item=items, collection=COLLECTION, refresh=pb.NONE), timeout=30)
    time.sleep(2)  # Wait for NRT refresh
    count = stub.Count(pb.CountRequest(collection=COLLECTION), timeout=10).count
    print(f"[SETUP] Seeded {count} documents")
    channel.close()


# ============================================================
# INDEX USER — bulk indexing workload
# ============================================================
class IndexUser(User):
    wait_time = between(0.1, 0.5)
    weight = 2  # 20% of users

    def on_start(self):
        _ensure_collection(self.host)
        self.channel = grpc.insecure_channel(self.host)
        self.stub = pb_grpc.JigyasaDataPlaneServiceStub(self.channel)

    def on_stop(self):
        self.channel.close()

    @task(3)
    @tag("index", "batch-small")
    def index_batch_10(self):
        """Index 10 documents in a batch."""
        items = [pb.IndexItem(action=pb.UPDATE, document=json.dumps(_random_doc()))
                 for _ in range(10)]
        _grpc_call(self, "Index (batch=10)",
                   lambda: self.stub.Index(pb.IndexRequest(
                       item=items, collection=COLLECTION, refresh=pb.NONE), timeout=10))

    @task(2)
    @tag("index", "batch-medium")
    def index_batch_50(self):
        """Index 50 documents in a batch."""
        items = [pb.IndexItem(action=pb.UPDATE, document=json.dumps(_random_doc()))
                 for _ in range(50)]
        _grpc_call(self, "Index (batch=50)",
                   lambda: self.stub.Index(pb.IndexRequest(
                       item=items, collection=COLLECTION, refresh=pb.NONE), timeout=15))

    @task(1)
    @tag("index", "batch-large")
    def index_batch_100(self):
        """Index 100 documents in a batch."""
        items = [pb.IndexItem(action=pb.UPDATE, document=json.dumps(_random_doc()))
                 for _ in range(100)]
        _grpc_call(self, "Index (batch=100)",
                   lambda: self.stub.Index(pb.IndexRequest(
                       item=items, collection=COLLECTION, refresh=pb.NONE), timeout=30))

    @task(1)
    @tag("index", "single")
    def index_single_immediate(self):
        """Index 1 document with IMMEDIATE refresh."""
        items = [pb.IndexItem(action=pb.UPDATE, document=json.dumps(_random_doc()))]
        _grpc_call(self, "Index (single, IMMEDIATE)",
                   lambda: self.stub.Index(pb.IndexRequest(
                       item=items, collection=COLLECTION, refresh=pb.IMMEDIATE), timeout=10))


# ============================================================
# QUERY USER — mixed read workload
# ============================================================
class QueryUser(User):
    wait_time = between(0.05, 0.2)
    weight = 8  # 80% of users

    def on_start(self):
        _ensure_collection(self.host)
        self.channel = grpc.insecure_channel(self.host)
        self.stub = pb_grpc.JigyasaDataPlaneServiceStub(self.channel)

    def on_stop(self):
        self.channel.close()

    @task(5)
    @tag("query", "text")
    def text_search(self):
        """BM25 text search."""
        term = random.choice(SEARCH_TERMS)
        _grpc_call(self, "Query (BM25 text)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, text_query=term, top_k=10), timeout=5))

    @task(3)
    @tag("query", "filter")
    def term_filter(self):
        """Term filter on category."""
        cat = random.choice(CATEGORIES)
        _grpc_call(self, "Query (term filter)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, top_k=10,
                       filters=[pb.FilterClause(field="category",
                           term_filter=pb.TermFilter(value=cat))]), timeout=5))

    @task(2)
    @tag("query", "range")
    def range_filter(self):
        """Range filter on score."""
        lo = random.randint(0, 50)
        hi = lo + random.randint(10, 50)
        _grpc_call(self, "Query (range filter)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, top_k=10,
                       filters=[pb.FilterClause(field="score",
                           range_filter=pb.RangeFilter(min=str(lo), max=str(hi)))]), timeout=5))

    @task(3)
    @tag("query", "text+filter")
    def text_plus_filter(self):
        """Text search + filter combo."""
        term = random.choice(SEARCH_TERMS)
        cat = random.choice(CATEGORIES)
        _grpc_call(self, "Query (text+filter)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, text_query=term, top_k=10,
                       filters=[pb.FilterClause(field="category",
                           term_filter=pb.TermFilter(value=cat))]), timeout=5))

    @task(2)
    @tag("query", "count")
    def count_query(self):
        """Count API."""
        _grpc_call(self, "Count",
                   lambda: self.stub.Count(pb.CountRequest(collection=COLLECTION), timeout=5))

    @task(1)
    @tag("query", "sort")
    def sorted_query(self):
        """Match-all with sort."""
        _grpc_call(self, "Query (match-all+sort)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, top_k=10,
                       sort=[pb.SortClause(field="score", descending=True)]), timeout=5))

    @task(1)
    @tag("query", "pagination")
    def paginated_query(self):
        """Paginated query with offset."""
        offset = random.randint(0, 100)
        _grpc_call(self, "Query (paginated)",
                   lambda: self.stub.Query(pb.QueryRequest(
                       collection=COLLECTION, top_k=10, offset=offset), timeout=5))

    @task(1)
    @tag("query", "lookup")
    def lookup_query(self):
        """Lookup by key (may miss — that's fine)."""
        keys = [f"loc-{random.randint(1, DOC_COUNTER or 1000)}-{random.randint(0, 999999)}"]
        _grpc_call(self, "Lookup",
                   lambda: self.stub.Lookup(pb.LookupRequest(
                       docKeys=keys, collection=COLLECTION), timeout=5))
