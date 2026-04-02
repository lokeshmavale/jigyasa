#!/usr/bin/env python3
"""
Example 07 — Multi-Language Analyzers

Demonstrates:
  1. Per-field analyzer configuration (indexAnalyzer / searchAnalyzer)
  2. Language-specific stemming for French, German, Hindi, CJK
  3. Side-by-side comparison: standard vs language-tuned analyzer
  4. Keyword analyzer for exact-match fields
"""

import json
import os
import subprocess
import sys
import site
import time

import grpc

# ---------------------------------------------------------------------------
# Stub generation
# ---------------------------------------------------------------------------
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "main", "proto")
GEN_DIR = os.path.join(os.path.dirname(__file__), "_gen")
os.makedirs(GEN_DIR, exist_ok=True)

import grpc_tools
grpc_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
site_pkgs = [p for p in site.getsitepackages() if "site-packages" in p][0]

result = subprocess.run([
    sys.executable, "-m", "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}",
    f"--proto_path={grpc_include}",
    f"--proto_path={site_pkgs}",
    f"--python_out={GEN_DIR}",
    f"--grpc_python_out={GEN_DIR}",
    "dpSearch.proto",
], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Proto compilation failed: {result.stderr}")
    sys.exit(1)

sys.path.insert(0, GEN_DIR)
import dpSearch_pb2 as pb
import dpSearch_pb2_grpc as pb_grpc

ADDR = "localhost:50051"

# ---------------------------------------------------------------------------
# Multilingual test corpus — each entry has text in its native language
# ---------------------------------------------------------------------------
DOCS = [
    # French
    {"id": "fr1", "lang": "fr",
     "title": "Les maisons blanches sont belles",
     "body": "Les architectes français construisent de magnifiques maisons blanches dans le sud de la France."},
    {"id": "fr2", "lang": "fr",
     "title": "Architecture française moderne",
     "body": "La construction de bâtiments modernes utilise des matériaux écologiques et durables."},

    # German
    {"id": "de1", "lang": "de",
     "title": "Häuser und Gebäude in Deutschland",
     "body": "Die deutschen Architekten entwerfen energieeffiziente Wohnhäuser und Bürogebäude."},
    {"id": "de2", "lang": "de",
     "title": "Moderne Stadtentwicklung",
     "body": "Städtebauliche Entwicklung in Berlin umfasst nachhaltige Gebäude und grüne Infrastruktur."},

    # Hindi
    {"id": "hi1", "lang": "hi",
     "title": "भारतीय वास्तुकला की सुंदरता",
     "body": "भारतीय मंदिरों की वास्तुकला विश्व प्रसिद्ध है। पुराने मंदिर अद्भुत शिल्पकला के नमूने हैं।"},
    {"id": "hi2", "lang": "hi",
     "title": "आधुनिक भवन निर्माण",
     "body": "भारत में आधुनिक भवनों का निर्माण तेजी से हो रहा है। नई तकनीकें इमारतों को मजबूत बनाती हैं।"},

    # CJK (Japanese)
    {"id": "ja1", "lang": "cjk",
     "title": "日本の伝統的な建築",
     "body": "日本の伝統的な建築様式は、木造建築を基本としています。寺院や神社は美しい建築物です。"},
    {"id": "ja2", "lang": "cjk",
     "title": "東京の現代建築",
     "body": "東京には世界的に有名な現代建築が数多くあります。高層ビルやタワーが都市の景観を形成しています。"},

    # English (for comparison)
    {"id": "en1", "lang": "en",
     "title": "Modern Architecture Trends",
     "body": "Architects are designing sustainable buildings using renewable materials and energy-efficient systems."},
    {"id": "en2", "lang": "en",
     "title": "Historical Buildings of Europe",
     "body": "European cathedrals and castles represent centuries of architectural innovation and craftsmanship."},
]


def banner(text):
    print(f"\n{'=' * 64}")
    print(f"  {text}")
    print(f"{'=' * 64}")


def print_hits(resp, label=""):
    if label:
        print(f"  {label}")
    print(f"  Hits: {resp.total_hits}")
    for i, hit in enumerate(resp.hits):
        doc = json.loads(hit.source)
        # Find the best title field available
        title = (doc.get("title") or doc.get("title_std")
                 or doc.get("title_fr") or doc.get("title_de")
                 or doc.get("title_hi") or doc.get("title_cjk")
                 or doc.get("title_en") or doc.get("id", "?"))
        lang = doc.get("lang", "?")
        print(f"    {i+1}. [{hit.score:.4f}] [{lang}] {title}")
    if resp.total_hits == 0:
        print("    (none)")
    print()


def main():
    channel = grpc.insecure_channel(ADDR)
    stub = pb_grpc.JigyasaDataPlaneServiceStub(channel)

    # ── 1. Create collection with LANGUAGE-SPECIFIC analyzers ─────────
    banner("1. Create collection with per-field analyzers")

    schema = json.dumps({
        "fields": [
            {"name": "id",    "type": "STRING", "key": True, "filterable": True},
            {"name": "lang",  "type": "STRING", "filterable": True},
            # title_std — indexed with standard analyzer (no language stemming)
            {"name": "title_std", "type": "STRING", "searchable": True,
             "indexAnalyzer": "standard", "searchAnalyzer": "standard"},
            # Per-language fields with dedicated analyzers
            {"name": "title_fr", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.fr", "searchAnalyzer": "lucene.fr"},
            {"name": "title_de", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.de", "searchAnalyzer": "lucene.de"},
            {"name": "title_hi", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.hi", "searchAnalyzer": "lucene.hi"},
            {"name": "title_cjk", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.cjk", "searchAnalyzer": "lucene.cjk"},
            {"name": "title_en", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.en", "searchAnalyzer": "lucene.en"},
            # body fields
            {"name": "body_std", "type": "STRING", "searchable": True,
             "indexAnalyzer": "standard", "searchAnalyzer": "standard"},
            {"name": "body_lang", "type": "STRING", "searchable": True,
             "indexAnalyzer": "lucene.en", "searchAnalyzer": "lucene.en"},
            # keyword field — exact match, no tokenization
            {"name": "tag", "type": "STRING", "searchable": True,
             "indexAnalyzer": "keyword", "searchAnalyzer": "keyword"},
        ],
    })

    try:
        stub.CreateCollection(pb.CreateCollectionRequest(
            collection="analyzers_demo", indexSchema=schema))
        print("  ✓ Created collection 'analyzers_demo'")
    except grpc.RpcError as e:
        if "already exists" in str(e.details()):
            print("  ✓ Collection already exists — reusing")
        else:
            raise

    # ── 2. Index documents — copy title/body into language-specific fields ─
    banner("2. Index multilingual documents")

    items = []
    for doc in DOCS:
        lang = doc["lang"]
        indexed = {
            "id": doc["id"],
            "lang": lang,
            "title_std": doc["title"],      # always indexed with standard
            f"title_{lang}": doc["title"],   # indexed with language analyzer
            "body_std": doc["body"],         # always indexed with standard
            "body_lang": doc["body"],        # indexed with lucene.en (for EN docs)
            "tag": f"architecture-{lang}",   # exact keyword
        }
        # For the lang-specific body, we route through body_lang only for English
        # (since body_lang uses lucene.en). Other languages use body_std.
        items.append(pb.IndexItem(document=json.dumps(indexed)))

    resp = stub.Index(pb.IndexRequest(item=items, collection="analyzers_demo"))
    ok = sum(1 for r in resp.itemResponse if r.code == 0)
    print(f"  ✓ Indexed {ok}/{len(DOCS)} documents")

    time.sleep(1)

    # ── 3. French stemming demo ───────────────────────────────────────
    banner("3. French stemming: 'maison' (singular) → finds 'maisons' (plural)")

    # Standard analyzer: exact match only
    resp_std = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_std:maison",
        top_k=5, include_source=True,
    ))
    print_hits(resp_std, label="Standard analyzer on 'maison':")

    # French analyzer: stems maison/maisons to same root
    resp_fr = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_fr:maison",
        top_k=5, include_source=True,
    ))
    print_hits(resp_fr, label="French analyzer (lucene.fr) on 'maison':")

    # ── 4. German stemming demo ───────────────────────────────────────
    banner("4. German stemming: 'Haus' → finds 'Häuser' (plural with umlaut)")

    resp_std = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_std:Haus",
        top_k=5, include_source=True,
    ))
    print_hits(resp_std, label="Standard analyzer on 'Haus':")

    resp_de = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_de:Haus",
        top_k=5, include_source=True,
    ))
    print_hits(resp_de, label="German analyzer (lucene.de) on 'Haus':")

    # ── 5. Hindi demo ─────────────────────────────────────────────────
    banner("5. Hindi search: 'वास्तुकला' (architecture)")

    resp_hi = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_hi:वास्तुकला",
        top_k=5, include_source=True,
    ))
    print_hits(resp_hi, label="Hindi analyzer (lucene.hi) on 'वास्तुकला':")

    # ── 6. CJK bigram demo ────────────────────────────────────────────
    banner("6. CJK search: '建築' (architecture in Japanese)")

    resp_cjk = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_cjk:建築",
        top_k=5, include_source=True,
    ))
    print_hits(resp_cjk, label="CJK analyzer (lucene.cjk) on '建築':")

    # ── 7. English stemming demo ──────────────────────────────────────
    banner("7. English stemming: 'architectural' → finds 'architecture/architects'")

    resp_std = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_std:architectural",
        top_k=5, include_source=True,
    ))
    print_hits(resp_std, label="Standard analyzer on 'architectural':")

    resp_en = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="title_en:architectural",
        top_k=5, include_source=True,
    ))
    print_hits(resp_en, label="English analyzer (lucene.en) on 'architectural':")

    # ── 8. Keyword analyzer — exact match ─────────────────────────────
    banner("8. Keyword analyzer: exact-match tag search")

    # Partial match should NOT work with keyword analyzer
    resp_partial = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="tag:architecture",
        top_k=5, include_source=True,
    ))
    print_hits(resp_partial, label="Keyword field, query='architecture' (partial — no match expected):")

    # Exact match works
    resp_exact = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="tag:architecture-fr",
        top_k=5, include_source=True,
    ))
    print_hits(resp_exact, label="Keyword field, query='architecture-fr' (exact match):")

    # ── 9. Cross-language filter: combine text search + lang filter ───
    banner("9. Cross-language: French text search + lang filter")

    resp = stub.Query(pb.QueryRequest(
        collection="analyzers_demo",
        query_string="body_std:construction AND body_std:bâtiment",
        top_k=5, include_source=True,
        filters=[pb.FilterClause(
            field="lang", term_filter=pb.TermFilter(value="fr"),
        )],
    ))
    print_hits(resp, label="Body search 'construction bâtiment' filtered to lang=fr:")

    print("=" * 64)
    print("  ✅ Multi-language analyzer demo complete!")
    print()
    print("  Supported analyzers: standard, simple, keyword, whitespace,")
    print("  + 38 language analyzers: lucene.en, lucene.fr, lucene.de,")
    print("    lucene.hi, lucene.cjk, lucene.ar, lucene.ru, lucene.es, ...")
    print("  See AnalyzerNames.java for the full list.")
    print("=" * 64)


if __name__ == "__main__":
    main()
