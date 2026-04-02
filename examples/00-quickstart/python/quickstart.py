"""
Jigyasa Quickstart — 5 minutes from zero to search.

Prerequisites:
    pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos

Usage:
    python quickstart.py
"""
import grpc
import json
import subprocess
import sys
import os
import time

ADDR = "localhost:50051"
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")


def generate_stubs():
    """Generate Python gRPC stubs from proto."""
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


def main():
    generate_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # 1. Health check
    print("1. Health check")
    health = stub.Health(pb.HealthRequest())
    print(f"   Status: {pb.HealthResponse.Status.Name(health.status)}")

    # 2. Create collection
    print("\n2. Create collection 'quickstart'")
    schema = json.dumps({"fields": [
        {"name": "id", "type": "STRING", "key": True, "filterable": True},
        {"name": "title", "type": "STRING", "searchable": True, "sortable": True},
        {"name": "body", "type": "STRING", "searchable": True},
        {"name": "category", "type": "STRING", "filterable": True},
    ]})
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection="quickstart", indexSchema=schema))
        print("   ✓ Created")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("   ✓ Already exists")
        else:
            raise

    # 3. Index documents
    print("\n3. Index 5 documents")
    docs = [
        {"id": "1", "title": "Getting Started with Lucene", "body": "Apache Lucene is a high-performance search engine library written in Java", "category": "search"},
        {"id": "2", "title": "Vector Search Fundamentals", "body": "HNSW algorithm enables fast approximate nearest neighbor search", "category": "search"},
        {"id": "3", "title": "Building AI Agents", "body": "Autonomous agents use LLMs to reason, plan, and take actions", "category": "ai"},
        {"id": "4", "title": "gRPC Protocol Guide", "body": "gRPC uses HTTP/2 and Protocol Buffers for efficient communication", "category": "networking"},
        {"id": "5", "title": "Memory for LLM Agents", "body": "Agents need persistent memory to recall past conversations and facts", "category": "ai"},
    ]
    items = [pb.IndexItem(document=json.dumps(d)) for d in docs]
    resp = stub.Index(pb.IndexRequest(item=items, collection="quickstart"))
    print(f"   ✓ Indexed {len(resp.itemResponse)} documents")

    time.sleep(1)  # Wait for NRT refresh

    # 4. Search
    print("\n4. Search for 'search engine library'")
    resp = stub.Query(pb.QueryRequest(
        collection="quickstart",
        text_query="search engine library",
        include_source=True,
        top_k=5
    ))
    print(f"   Total hits: {resp.total_hits}")
    for hit in resp.hits:
        doc = json.loads(hit.source)
        print(f"   [{hit.score:.3f}] {doc['title']}")

    # 5. Count
    print(f"\n5. Total documents in collection")
    count = stub.Count(pb.CountRequest(collection="quickstart"))
    print(f"   Count: {count.count}")

    print("\n✅ Quickstart complete!")
    channel.close()


if __name__ == "__main__":
    main()
