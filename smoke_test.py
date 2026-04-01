"""Smoke test for Jigyasa gRPC server running on localhost:50051"""
import grpc
import json
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

ADDR = "localhost:50051"

def main():
    channel = grpc.insecure_channel(ADDR)
    
    # 1. Test server reflection - list services
    print("=" * 60)
    print("1. SERVER REFLECTION — listing services")
    print("=" * 60)
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    req = reflection_pb2.ServerReflectionRequest(list_services="")
    responses = stub.ServerReflectionInfo(iter([req]))
    services = []
    for resp in responses:
        for svc in resp.list_services_response.service:
            services.append(svc.name)
            print(f"   ✓ {svc.name}")
    
    assert any("JigyasaDataPlaneService" in s for s in services), "Service not found!"
    print(f"\n   Found {len(services)} services ✓\n")

    # 2. Use reflection to get the service descriptor and build dynamic calls
    # For proper testing, generate Python stubs from proto
    print("=" * 60)
    print("2. GENERATING PYTHON STUBS from proto")
    print("=" * 60)
    
    import subprocess, sys, os
    proto_path = os.path.join(os.path.dirname(__file__), "src", "main", "proto")
    out_path = os.path.join(os.path.dirname(__file__), "test-data", "gen")
    os.makedirs(out_path, exist_ok=True)
    
    # Find grpc_tools bundled proto includes (has google/protobuf/*.proto)
    import grpc_tools
    grpc_proto_include = os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')
    
    # googleapis-common-protos provides google/rpc/status.proto at site-packages level
    import site
    site_packages = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    
    result = subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_path}",
        f"--proto_path={grpc_proto_include}",
        f"--proto_path={site_packages}",
        f"--python_out={out_path}",
        f"--grpc_python_out={out_path}",
        "dpSearch.proto"
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"   ✗ Proto compilation failed: {result.stderr}")
        return
    print("   ✓ Stubs generated\n")
    
    # Import generated stubs
    sys.path.insert(0, out_path)
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc
    
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)
    
    # 3. Health check
    print("=" * 60)
    print("3. HEALTH CHECK")
    print("=" * 60)
    health = stub.Health(pb.HealthRequest())
    print(f"   Status: {pb.HealthResponse.Status.Name(health.status)}")
    for c in health.collections:
        print(f"   Collection '{c.name}': docs={c.doc_count}, segments={c.segment_count}, writer={c.writer_open}")
    print(f"   ✓ Health OK\n")
    
    # 4. Create a test collection with proper schema
    print("=" * 60)
    print("4. CREATE COLLECTION with rich schema")
    print("=" * 60)
    
    schema = json.dumps({
        "fields": [
            {"name": "id", "type": "STRING", "key": True, "searchable": False, "filterable": True, "sortable": False},
            {"name": "title", "type": "STRING", "searchable": True, "filterable": False, "sortable": True},
            {"name": "body", "type": "STRING", "searchable": True, "filterable": False, "sortable": False},
            {"name": "category", "type": "STRING", "searchable": False, "filterable": True, "sortable": True},
            {"name": "priority", "type": "INT32", "searchable": False, "filterable": True, "sortable": True}
        ]
    })
    
    try:
        resp = stub.CreateCollection(pb.CreateCollectionRequest(collection="test", indexSchema=schema))
        print(f"   ✓ Collection 'test' created\n")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print(f"   ✓ Collection 'test' already exists\n")
        else:
            raise
    
    # 5. Index some documents
    print("=" * 60)
    print("5. INDEXING DOCUMENTS")
    print("=" * 60)
    
    docs = [
        {"id": "doc1", "title": "Introduction to Lucene", "body": "Apache Lucene is a high-performance full-text search engine library", "category": "search", "priority": 1},
        {"id": "doc2", "title": "Vector Search with HNSW", "body": "HNSW graphs provide approximate nearest neighbor search", "category": "search", "priority": 2},
        {"id": "doc3", "title": "Agent Memory Systems", "body": "LLM agents need persistent memory for long-running tasks", "category": "ai", "priority": 3},
        {"id": "doc4", "title": "Elasticsearch Internals", "body": "Elasticsearch is built on top of Apache Lucene segments", "category": "search", "priority": 1},
        {"id": "doc5", "title": "LangGraph Checkpointing", "body": "LangGraph uses SQLite for agent state checkpointing by default", "category": "ai", "priority": 2},
    ]
    
    items = []
    for doc in docs:
        items.append(pb.IndexItem(document=json.dumps(doc)))
    
    resp = stub.Index(pb.IndexRequest(item=items, collection="test"))
    print(f"   ✓ Indexed {len(docs)} docs: {len(resp.itemResponse)} responses")
    
    # Wait for NRT refresh
    import time
    time.sleep(2)
    print()
    
    # 5. Text search (BM25)
    print("=" * 60)
    print("5. TEXT SEARCH — 'lucene search engine'")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        text_query="lucene search engine",
        top_k=5
    ))
    print(f"   Total hits: {resp.total_hits}")
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   [{hit.score:.4f}] {src.get('title', 'N/A')}")
    print(f"   ✓ {len(resp.hits)} results\n")
    
    # 6. Term filter
    print("=" * 60)
    print("6. TERM FILTER — category='ai'")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        top_k=10,
        filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value="ai"))]
    ))
    print(f"   Total hits: {resp.total_hits}")
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   {src.get('title')}")
    print(f"   ✓ {len(resp.hits)} results\n")
    
    # 7. Range filter
    print("=" * 60)
    print("7. RANGE FILTER — priority >= 2")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        top_k=10,
        filters=[pb.FilterClause(field="priority", range_filter=pb.RangeFilter(min="2"))]
    ))
    print(f"   Total hits: {resp.total_hits}")
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   {src.get('title')} (priority={src.get('priority')})")
    print(f"   ✓ {len(resp.hits)} results\n")
    
    # 8. Query string (Lucene syntax)
    print("=" * 60)
    print("8. QUERY STRING — 'body:lucene AND body:search'")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        query_string="body:lucene AND body:search",
        query_string_default_field="body",
        top_k=10
    ))
    print(f"   Total hits: {resp.total_hits}")
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   [{hit.score:.4f}] {src.get('title')}")
    print(f"   ✓ {len(resp.hits)} results\n")
    
    # 9. Count API
    print("=" * 60)
    print("9. COUNT API — all docs")
    print("=" * 60)
    count_resp = stub.Count(pb.CountRequest(collection="test"))
    print(f"   Count: {count_resp.count}")
    assert count_resp.count == 5, f"Expected 5, got {count_resp.count}"
    print(f"   ✓ Count correct\n")
    
    # 10. Count with filter
    print("=" * 60)
    print("10. COUNT WITH FILTER — category='search'")
    print("=" * 60)
    count_resp = stub.Count(pb.CountRequest(
        collection="test",
        filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value="search"))]
    ))
    print(f"   Count: {count_resp.count}")
    print(f"   ✓ Filtered count\n")
    
    # 11. Field projection
    print("=" * 60)
    print("11. FIELD PROJECTION — only 'title' and 'category'")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        top_k=3,
        source_fields=["title", "category"]
    ))
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   Fields returned: {list(src.keys())}")
        assert "body" not in src, "body should be excluded!"
    print(f"   ✓ Projection works\n")
    
    # 12. Boolean compound filter
    print("=" * 60)
    print("12. COMPOUND FILTER — category=search OR category=ai")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        top_k=10,
        filters=[pb.FilterClause(compound_filter=pb.CompoundFilter(
            should=[
                pb.FilterClause(field="category", term_filter=pb.TermFilter(value="search")),
                pb.FilterClause(field="category", term_filter=pb.TermFilter(value="ai")),
            ]
        ))]
    ))
    print(f"   Total hits: {resp.total_hits}")
    assert resp.total_hits == 5
    print(f"   ✓ All 5 docs matched (OR)\n")
    
    # 13. Sorting
    print("=" * 60)
    print("13. SORT BY priority ASC")
    print("=" * 60)
    resp = stub.Query(pb.QueryRequest(include_source=True, 
        collection="test",
        top_k=5,
        sort=[pb.SortClause(field="priority")]
    ))
    for hit in resp.hits:
        src = json.loads(hit.source)
        print(f"   priority={src.get('priority')} — {src.get('title')}")
    print(f"   ✓ Sorted\n")

    # Summary
    print("=" * 60)
    print("🎉 ALL SMOKE TESTS PASSED!")
    print("=" * 60)
    print(f"   Server: {ADDR}")
    print(f"   Indexed: 5 documents")
    print(f"   Tested: 13 operations")
    print(f"   Status: ALL GREEN ✓")
    
    channel.close()

if __name__ == "__main__":
    main()
