#!/usr/bin/env python3
"""Jigyasa Example 05 — Ops & Multi-Tenancy."""

import subprocess, sys, os, time, json

# ── stub generation ──────────────────────────────────────────────────────────
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)
import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
import site
site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]
subprocess.run([sys.executable, "-m", "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}", f"--proto_path={grpc_include}", f"--proto_path={site_pkgs}",
    f"--python_out={GEN_DIR}", f"--grpc_python_out={GEN_DIR}", "dpSearch.proto"
], capture_output=True, text=True, check=True)
sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

import grpc

COLLECTION = "ops_demo"
ADDR = "localhost:50051"

SCHEMA = json.dumps({"fields": [
    {"name": "id", "type": "STRING", "key": True, "filterable": True},
    {"name": "title", "type": "STRING", "searchable": True},
    {"name": "category", "type": "STRING", "filterable": True},
]})

ACME_DOCS = [
    {"id": "a1", "title": "Acme Cloud Platform",      "category": "software"},
    {"id": "a2", "title": "Acme Rocket Engine",        "category": "hardware"},
    {"id": "a3", "title": "Acme Deployment Toolkit",   "category": "software"},
]

GLOBEX_DOCS = [
    {"id": "g1", "title": "Globex Strategy Report",    "category": "consulting"},
    {"id": "g2", "title": "Globex Analytics Suite",    "category": "software"},
]


def banner(msg: str):
    print(f"\n{'=' * 60}\n  {msg}\n{'=' * 60}")


def run():
    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # ── 1. Collection lifecycle ──────────────────────────────────────────────
    banner("1. Collection Lifecycle")

    print("Creating collection …")
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=SCHEMA))
        print(f"  ✓ Created '{COLLECTION}'")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print(f"  ✓ '{COLLECTION}' already exists")
        else:
            raise

    resp = stub.ListCollections(pb.ListCollectionsRequest())
    print(f"  Collections after create: {list(resp.collections)}")

    print("Closing collection …")
    stub.CloseCollection(pb.CloseCollectionRequest(collection=COLLECTION))
    print(f"  ✓ Closed '{COLLECTION}'")

    print("Reopening collection (schema from index) …")
    stub.OpenCollection(pb.OpenCollectionRequest(collection=COLLECTION))
    print(f"  ✓ Reopened '{COLLECTION}'")

    resp = stub.ListCollections(pb.ListCollectionsRequest())
    print(f"  Collections after reopen: {list(resp.collections)}")

    # ── 2. Health monitoring ─────────────────────────────────────────────────
    banner("2. Health Monitoring")

    health = stub.Health(pb.HealthRequest())
    print(f"  Server status : {pb.HealthResponse.Status.Name(health.status)}")
    for c in health.collections:
        print(f"  Collection '{c.name}': writer_open={c.writer_open}, "
              f"searcher_available={c.searcher_available}, "
              f"doc_count={c.doc_count}, segments={c.segment_count}")

    # ── 3. Multi-tenancy ─────────────────────────────────────────────────────
    banner("3. Multi-Tenancy — Indexing")

    acme_items = [pb.IndexItem(document=json.dumps(d), tenant_id="acme") for d in ACME_DOCS]
    stub.Index(pb.IndexRequest(item=acme_items, collection=COLLECTION))
    print(f"  Indexed {len(ACME_DOCS)} docs for tenant 'acme'")

    globex_items = [pb.IndexItem(document=json.dumps(d), tenant_id="globex") for d in GLOBEX_DOCS]
    stub.Index(pb.IndexRequest(item=globex_items, collection=COLLECTION))
    print(f"  Indexed {len(GLOBEX_DOCS)} docs for tenant 'globex'")

    time.sleep(2)  # NRT refresh

    banner("3b. Multi-Tenancy — Tenant-Scoped Queries")

    for tenant in ("acme", "globex"):
        count_resp = stub.Count(pb.CountRequest(
            collection=COLLECTION, tenant_id=tenant))
        print(f"  Tenant '{tenant}' doc count: {count_resp.count}")

    query_resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="platform",
        tenant_id="acme",
        top_k=10,
        include_source=True,
    ))
    print(f"\n  Query 'platform' scoped to 'acme' → {len(query_resp.hits)} hit(s):")
    for h in query_resp.hits:
        doc = json.loads(h.source)
        print(f"    {doc['id']}: {doc['title']}")

    query_resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="platform",
        tenant_id="globex",
        top_k=10,
        include_source=True,
    ))
    print(f"\n  Query 'platform' scoped to 'globex' → {len(query_resp.hits)} hit(s) (expected 0)")

    # ── 4. Delete by query ───────────────────────────────────────────────────
    banner("4. Delete By Query")

    del_resp = stub.DeleteByQuery(pb.DeleteByQueryRequest(
        collection=COLLECTION,
        tenant_id="acme",
        filters=[pb.FilterClause(field="category", term_filter=pb.TermFilter(value="hardware"))],
    ))
    print(f"  Deleted (category='hardware' for tenant 'acme'), count={del_resp.deleted_count}")

    time.sleep(1)

    count_resp = stub.Count(pb.CountRequest(
        collection=COLLECTION, tenant_id="acme"))
    print(f"  Tenant 'acme' doc count after delete: {count_resp.count}")

    # ── 5. Force merge ───────────────────────────────────────────────────────
    banner("5. Force Merge")

    merge_resp = stub.ForceMerge(pb.ForceMergeRequest(
        collection=COLLECTION, max_segments=1))
    print(f"  Segments before: {merge_resp.segments_before}")
    print(f"  Segments after : {merge_resp.segments_after}")

    # ── Cleanup ──────────────────────────────────────────────────────────────
    banner("Cleanup")
    stub.CloseCollection(pb.CloseCollectionRequest(collection=COLLECTION))
    print(f"  ✓ Closed '{COLLECTION}'")

    print("\n✅ Done.")
    channel.close()


if __name__ == "__main__":
    run()
