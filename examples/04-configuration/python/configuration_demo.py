"""
Jigyasa 04 — Configuration: Memory Tiers, TTL & Recency Decay.

Demonstrates memory tiers (SEMANTIC / EPISODIC / WORKING), custom TTL overrides,
refresh policies, and recency-decay boosted queries.

Prerequisites:
    pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos

Usage:
    python configuration_demo.py
"""

import json, time, subprocess, sys, os

import grpc

# ── Proto stub generation ────────────────────────────────────────────────────

ADDR = "localhost:50051"
COLLECTION = "config-demo"
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")


def generate_stubs():
    os.makedirs(GEN_DIR, exist_ok=True)
    import grpc_tools
    grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
    import site
    site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--proto_path={grpc_include}",
        f"--proto_path={site_pkgs}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        "dpSearch.proto",
    ], capture_output=True, text=True, check=True)
    sys.path.insert(0, GEN_DIR)


# ── Helpers ──────────────────────────────────────────────────────────────────

def print_header(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def print_hits(resp):
    print(f"  Total hits: {resp.total_hits}")
    for hit in resp.hits:
        doc = json.loads(hit.source)
        tier_label = doc.get("tier_label", "")
        print(f"  [{hit.score:.4f}] {doc['title']}"
              + (f"  (tier: {tier_label})" if tier_label else ""))


# ── Main demo ────────────────────────────────────────────────────────────────

def main():
    generate_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # ── 1. Health check ──────────────────────────────────────────────────────
    print_header("1. Health Check")
    health = stub.Health(pb.HealthRequest())
    print(f"  Status: {pb.HealthResponse.Status.Name(health.status)}")

    # ── 2. Create TTL-enabled collection ─────────────────────────────────────
    print_header("2. Create TTL-Enabled Collection")
    schema = json.dumps({
        "ttlEnabled": True,
        "fields": [
            {"name": "id",         "type": "STRING",  "key": True, "filterable": True},
            {"name": "title",      "type": "STRING",  "searchable": True, "sortable": True},
            {"name": "body",       "type": "STRING",  "searchable": True},
            {"name": "category",   "type": "STRING",  "filterable": True},
            {"name": "tier_label", "type": "STRING",  "filterable": True},
            {"name": "priority",   "type": "INT32",   "filterable": True, "sortable": True},
        ],
    })
    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection=COLLECTION, indexSchema=schema))
        print(f"  ✓ Collection '{COLLECTION}' created with ttlEnabled=true")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print(f"  ✓ Collection '{COLLECTION}' already exists")
        else:
            raise

    # ── 3. Index documents across memory tiers ───────────────────────────────
    print_header("3. Index Documents Across Memory Tiers")

    # SEMANTIC tier — permanent knowledge
    semantic_docs = [
        {"id": "s1", "title": "Company Mission Statement",
         "body": "Our mission is to organise the world's information and make it universally accessible.",
         "category": "corporate", "tier_label": "semantic", "priority": 10},
        {"id": "s2", "title": "Product Architecture Overview",
         "body": "The platform is built on a microservices architecture with gRPC communication.",
         "category": "engineering", "tier_label": "semantic", "priority": 9},
    ]
    semantic_items = [
        pb.IndexItem(
            document=json.dumps(d),
            memory_tier=pb.SEMANTIC,  # permanent — no expiry
        ) for d in semantic_docs
    ]

    # EPISODIC tier — session/conversation context (24h default TTL)
    episodic_docs = [
        {"id": "e1", "title": "User asked about pricing",
         "body": "The user inquired about enterprise pricing tiers during today's demo call.",
         "category": "conversation", "tier_label": "episodic", "priority": 7},
        {"id": "e2", "title": "Meeting notes: Q3 planning",
         "body": "Team agreed to prioritise search latency improvements in Q3.",
         "category": "meeting", "tier_label": "episodic", "priority": 6},
    ]
    episodic_items = [
        pb.IndexItem(
            document=json.dumps(d),
            memory_tier=pb.EPISODIC,  # 24-hour default TTL
        ) for d in episodic_docs
    ]

    # WORKING tier — transient scratchpad (5 min default TTL)
    working_docs = [
        {"id": "w1", "title": "Draft response to pricing question",
         "body": "Suggest the growth plan at $499/month with custom SLA.",
         "category": "draft", "tier_label": "working", "priority": 5},
        {"id": "w2", "title": "Temp calculation: projected users",
         "body": "Based on current growth rate, projected 50k users by end of Q3.",
         "category": "scratch", "tier_label": "working", "priority": 3},
    ]
    working_items = [
        pb.IndexItem(
            document=json.dumps(d),
            memory_tier=pb.WORKING,  # 5-minute default TTL
        ) for d in working_docs
    ]

    # Index all with WAIT_FOR refresh (default) — blocks until searchable
    all_items = semantic_items + episodic_items + working_items
    resp = stub.Index(pb.IndexRequest(
        item=all_items,
        collection=COLLECTION,
        refresh=pb.WAIT_FOR,
    ))
    print(f"  ✓ Indexed {len(resp.itemResponse)} documents (WAIT_FOR refresh)")
    print(f"    SEMANTIC: {len(semantic_items)} docs (permanent)")
    print(f"    EPISODIC: {len(episodic_items)} docs (24h TTL)")
    print(f"    WORKING:  {len(working_items)} docs (5min TTL)")

    # ── 4. Custom TTL override ───────────────────────────────────────────────
    print_header("4. Custom TTL Override")

    custom_ttl_doc = {"id": "e3", "title": "Urgent follow-up reminder",
                      "body": "Send pricing proposal to client within 1 hour.",
                      "category": "reminder", "tier_label": "episodic", "priority": 8}
    custom_item = pb.IndexItem(
        document=json.dumps(custom_ttl_doc),
        memory_tier=pb.EPISODIC,
        ttl_seconds=3600,  # override: 1 hour instead of 24h default
    )
    resp = stub.Index(pb.IndexRequest(
        item=[custom_item],
        collection=COLLECTION,
        refresh=pb.WAIT_FOR,
    ))
    print("  ✓ Indexed EPISODIC doc with custom TTL = 3600s (1 hour)")
    print("    Default EPISODIC TTL is 24h — this doc expires sooner")

    # ── 5. Refresh policies ──────────────────────────────────────────────────
    print_header("5. Refresh Policies")

    # NONE — fire-and-forget for bulk throughput
    bulk_doc = {"id": "b1", "title": "Bulk-loaded record",
                "body": "This document was indexed with NONE refresh policy.",
                "category": "bulk", "tier_label": "semantic", "priority": 1}
    resp = stub.Index(pb.IndexRequest(
        item=[pb.IndexItem(document=json.dumps(bulk_doc), memory_tier=pb.SEMANTIC)],
        collection=COLLECTION,
        refresh=pb.NONE,
    ))
    print("  ✓ NONE:      fire-and-forget (not immediately searchable)")

    # IMMEDIATE — force flush for highest consistency
    immediate_doc = {"id": "i1", "title": "Critical alert: system update",
                     "body": "Production deployment v2.5 completed successfully.",
                     "category": "alert", "tier_label": "semantic", "priority": 10}
    resp = stub.Index(pb.IndexRequest(
        item=[pb.IndexItem(document=json.dumps(immediate_doc), memory_tier=pb.SEMANTIC)],
        collection=COLLECTION,
        refresh=pb.IMMEDIATE,
    ))
    print("  ✓ IMMEDIATE: forced flush (searchable right now)")

    # WAIT_FOR — already demonstrated above
    print("  ✓ WAIT_FOR:  used in step 3 (blocks until searchable)")

    time.sleep(1)  # allow NONE-refreshed doc to become visible via NRT

    # ── 6. Basic query (no recency decay) ────────────────────────────────────
    print_header("6. Query Without Recency Decay")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="pricing",
        include_source=True,
        top_k=5,
    ))
    print_hits(resp)

    # ── 7. Query with recency decay ──────────────────────────────────────────
    print_header("7. Query With Recency Decay (half-life = 1 hour)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="pricing",
        include_source=True,
        top_k=5,
        recency_decay=pb.RecencyDecay(half_life_seconds=3600),
    ))
    print_hits(resp)
    print("\n  ↑ Recently indexed documents are boosted toward the top.")

    # ── 8. Aggressive recency decay ──────────────────────────────────────────
    print_header("8. Aggressive Recency Decay (half-life = 5 minutes)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="pricing",
        include_source=True,
        top_k=5,
        recency_decay=pb.RecencyDecay(half_life_seconds=300),
    ))
    print_hits(resp)
    print("\n  ↑ With a 5-minute half-life, only very recent docs keep high boost.")

    # ── 9. Query across all docs ─────────────────────────────────────────────
    print_header("9. Full Collection Query (all tiers)")
    resp = stub.Query(pb.QueryRequest(
        collection=COLLECTION,
        text_query="system architecture platform",
        include_source=True,
        top_k=10,
        recency_decay=pb.RecencyDecay(half_life_seconds=86400),  # gentle 1-day decay
    ))
    print_hits(resp)

    # ── 10. Document count ───────────────────────────────────────────────────
    print_header("10. Document Count")
    count = stub.Count(pb.CountRequest(collection=COLLECTION))
    print(f"  Total documents in '{COLLECTION}': {count.count}")
    print("  (WORKING-tier docs will disappear after ~5 minutes)")

    # ── Cleanup ──────────────────────────────────────────────────────────────
    channel.close()
    print(f"\n✓ Configuration demo complete.")


if __name__ == "__main__":
    main()
