"""
Sequential 1M benchmark — Jigyasa first, then ES.
Run from repo root: python benchmarks/benchmark_1m_sequential.py
"""
import grpc, json, sys, os, time, random, statistics, subprocess, site, urllib.request, threading

JIGYASA_ADDR = "localhost:50051"
ES_URL = "http://localhost:9201"
NUM_DOCS = 1_000_000
BATCH = 1000
ITERATIONS = 500
WARMUP = 50

random.seed(42)
METHODS = ["GET","POST","PUT","DELETE","PATCH"]
STATUSES = [200,200,200,201,301,400,404,500,503]
PATHS = ["/api/users","/api/search","/login","/health","/api/data","/docs","/api/metrics","/products"]
AGENTS = ["Chrome/120","Firefox/121","curl/8.4","python-requests/2.31","Googlebot/2.1"]
CITIES = ["New York","London","Tokyo","Mumbai","Sydney","Berlin","Paris","Toronto","Singapore","Seoul",
          "Hyderabad","Pune","San Francisco","Chicago","Seattle","Boston","Austin","Amsterdam"]
CATEGORIES = ["web","api","static","auth","data","admin","cdn","monitoring"]
TEXTS = [
    "Apache Lucene high-performance full-text search engine library",
    "Elasticsearch distributed search analytics engine built on Lucene",
    "Vector search HNSW graphs approximate nearest neighbor capabilities",
    "Machine learning models efficient storage retrieval training data",
    "Kubernetes orchestrates containerized applications across clusters",
    "Microservices architecture enables independent deployment services",
    "Real-time analytics pipelines process millions events per second",
    "Database indexing strategies significantly impact query performance",
    "Natural language processing transforms unstructured text insights",
]
SEARCH_TERMS = ["Lucene","search","Kubernetes","microservices","analytics","machine learning","database","distributed"]

def doc(i):
    return {"id":f"log-{i:07d}","method":random.choice(METHODS),"path":random.choice(PATHS),
            "status":random.choice(STATUSES),"response_time_ms":random.randint(1,5000),
            "bytes":random.randint(100,500000),"user_agent":random.choice(AGENTS),
            "city":random.choice(CITIES),"category":random.choice(CATEGORIES),
            "message":random.choice(TEXTS)+f" req_{i}","timestamp":1700000000+i}

def setup_stubs():
    sd = os.path.dirname(os.path.abspath(__file__))
    rr = os.path.dirname(sd)
    pp = os.path.join(rr,"src","main","proto")
    op = os.path.join(sd,"test-data","gen")
    os.makedirs(op, exist_ok=True)
    import grpc_tools
    gpi = os.path.join(os.path.dirname(grpc_tools.__file__),'_proto')
    sp = [p for p in site.getsitepackages() if 'site-packages' in p][0]
    subprocess.run([sys.executable,"-m","grpc_tools.protoc",f"--proto_path={pp}",f"--proto_path={gpi}",
                    f"--proto_path={sp}",f"--python_out={op}",f"--grpc_python_out={op}","dpSearch.proto"],
                   capture_output=True, check=True)
    sys.path.insert(0, op)

def es_req(method, path, body=None):
    url = f"{ES_URL}/{path}"
    data = json.dumps(body).encode() if body else None
    r = urllib.request.Request(url, data=data, method=method, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(r, timeout=120) as resp:
        return json.loads(resp.read())

def bench(name, fn):
    for _ in range(WARMUP): fn()
    lats = []
    for _ in range(ITERATIONS):
        t = time.perf_counter()
        fn()
        lats.append((time.perf_counter()-t)*1000)
    lats.sort()
    p50,p90,p99 = lats[len(lats)//2], lats[int(len(lats)*0.9)], lats[int(len(lats)*0.99)]
    m = statistics.mean(lats)
    print(f"  {name:<30s} p50={p50:>7.2f}ms  p90={p90:>7.2f}ms  p99={p99:>7.2f}ms  mean={m:>7.2f}ms  qps={1000/m:>7.1f}")
    return {"name":name,"p50":p50,"p90":p90,"p99":p99,"mean":m}

def concurrent_qps(name, fn, threads=4, duration=10):
    ops = [0]; errs = [0]; stop = [time.perf_counter()+duration]
    def w():
        while time.perf_counter() < stop[0]:
            try: fn(); ops[0]+=1
            except: errs[0]+=1
    ts = [threading.Thread(target=w) for _ in range(threads)]
    for t in ts: t.start()
    for t in ts: t.join()
    qps = ops[0]/duration
    print(f"  {name:<30s} {qps:>7,.0f} qps  ({ops[0]:,} ops, {errs[0]} errors, {threads} threads)")
    return qps

def main():
    setup_stubs()
    import dpSearch_pb2 as pb
    import dpSearch_pb2_grpc as pb_grpc

    SCHEMA = {"fields":[
        {"name":"id","type":"STRING","key":True,"filterable":True},
        {"name":"method","type":"STRING","filterable":True},
        {"name":"path","type":"STRING","searchable":True},
        {"name":"status","type":"INT32","filterable":True,"sortable":True},
        {"name":"response_time_ms","type":"INT32","filterable":True,"sortable":True},
        {"name":"bytes","type":"INT32","filterable":True,"sortable":True},
        {"name":"user_agent","type":"STRING","searchable":True},
        {"name":"city","type":"STRING","filterable":True},
        {"name":"category","type":"STRING","filterable":True},
        {"name":"message","type":"STRING","searchable":True},
        {"name":"timestamp","type":"INT64","filterable":True,"sortable":True}
    ]}

    # =============================================
    # JIGYASA
    # =============================================
    print("="*80)
    print("  JIGYASA — 1M DOCUMENTS (4 CPUs, 8GB heap, Linux container)")
    print("="*80)

    ch = grpc.insecure_channel(JIGYASA_ADDR, options=[('grpc.max_send_message_length',128*1024*1024),
                                                       ('grpc.max_receive_message_length',128*1024*1024)])
    js = pb_grpc.JigyasaDataPlaneServiceStub(ch)

    try: js.CreateCollection(pb.CreateCollectionRequest(collection="bench_1m",indexSchema=json.dumps(SCHEMA)),timeout=10)
    except: pass

    print(f"\n  Indexing {NUM_DOCS:,} docs (batch={BATCH})...")
    random.seed(42)
    t0 = time.perf_counter()
    for s in range(0, NUM_DOCS, BATCH):
        items = [pb.IndexItem(document=json.dumps(doc(i))) for i in range(s, min(s+BATCH,NUM_DOCS))]
        js.Index(pb.IndexRequest(item=items, collection="bench_1m", refresh=pb.NONE), timeout=60)
        if s % 100000 == 0 and s > 0: print(f"    {s:,}...")
    jt = time.perf_counter()-t0
    print(f"  Indexed: {NUM_DOCS:,} in {jt:.1f}s = {NUM_DOCS/jt:,.0f} docs/sec")

    print("  Force merging...")
    js.ForceMerge(pb.ForceMergeRequest(collection="bench_1m",max_segments=1),timeout=600)
    jc = js.Count(pb.CountRequest(collection="bench_1m"),timeout=10).count
    print(f"  Count: {jc:,} | Merged to 1 segment")

    print(f"\n  Query latency ({ITERATIONS} iterations, {WARMUP} warmup):")
    jr = []
    jr.append(bench("BM25 text", lambda: js.Query(pb.QueryRequest(collection="bench_1m",text_query=random.choice(SEARCH_TERMS),top_k=10),timeout=5)))
    jr.append(bench("Term filter", lambda: js.Query(pb.QueryRequest(collection="bench_1m",top_k=10,filters=[pb.FilterClause(field="category",term_filter=pb.TermFilter(value=random.choice(CATEGORIES)))]),timeout=5)))
    jr.append(bench("Range filter", lambda: js.Query(pb.QueryRequest(collection="bench_1m",top_k=10,filters=[pb.FilterClause(field="status",range_filter=pb.RangeFilter(min="400",max="503"))]),timeout=5)))
    jr.append(bench("Count", lambda: js.Count(pb.CountRequest(collection="bench_1m"),timeout=5)))
    jr.append(bench("Sort (resp_time desc)", lambda: js.Query(pb.QueryRequest(collection="bench_1m",top_k=10,sort=[pb.SortClause(field="response_time_ms",descending=True)]),timeout=5)))
    jr.append(bench("Text + filter", lambda: js.Query(pb.QueryRequest(collection="bench_1m",text_query=random.choice(SEARCH_TERMS),top_k=10,filters=[pb.FilterClause(field="city",term_filter=pb.TermFilter(value=random.choice(CITIES)))]),timeout=5)))

    print(f"\n  Concurrent throughput (4 threads, 10s):")
    j_qps = concurrent_qps("BM25 mixed", lambda: js.Query(pb.QueryRequest(collection="bench_1m",text_query=random.choice(SEARCH_TERMS),top_k=10),timeout=5))

    ch.close()
    print(f"\n  Jigyasa avg p50: {statistics.mean(r['p50'] for r in jr):.2f}ms")

    # =============================================
    # ELASTICSEARCH
    # =============================================
    print("\n" + "="*80)
    print("  ELASTICSEARCH 8.13 — 1M DOCUMENTS (4 CPUs, 8GB heap, Linux container)")
    print("="*80)

    # Start ES
    subprocess.run(["docker","rm","-f","es-1m"], capture_output=True)
    subprocess.run(["docker","run","-d","--name","es-1m","--cpus=4","--memory=12g",
                    "-p","9201:9200","-e","discovery.type=single-node","-e","xpack.security.enabled=false",
                    "-e","ES_JAVA_OPTS=-Xms8g -Xmx8g","-e","bootstrap.memory_lock=true",
                    "--ulimit","memlock=-1:-1","elasticsearch:8.13.0"], capture_output=True)

    print("  Waiting for ES...")
    for i in range(60):
        try:
            urllib.request.urlopen(ES_URL, timeout=2)
            print(f"  ES ready ({i*3}s)")
            break
        except: time.sleep(3)
    else:
        print("  ES NOT READY"); return

    try: es_req("DELETE","bench_1m")
    except: pass
    es_req("PUT","bench_1m",{"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"-1"},
        "mappings":{"properties":{"id":{"type":"keyword"},"method":{"type":"keyword"},"path":{"type":"text"},
            "status":{"type":"integer"},"response_time_ms":{"type":"integer"},"bytes":{"type":"integer"},
            "user_agent":{"type":"text"},"city":{"type":"keyword"},"category":{"type":"keyword"},
            "message":{"type":"text"},"timestamp":{"type":"long"}}}})

    print(f"\n  Indexing {NUM_DOCS:,} docs (batch={BATCH})...")
    random.seed(42)
    t0 = time.perf_counter()
    for s in range(0, NUM_DOCS, BATCH):
        bulk = ""
        for i in range(s, min(s+BATCH,NUM_DOCS)):
            d = doc(i)
            bulk += json.dumps({"index":{"_id":d["id"]}})+"\n"+json.dumps(d)+"\n"
        r = urllib.request.Request(f"{ES_URL}/bench_1m/_bulk", data=bulk.encode(),
                                  headers={"Content-Type":"application/x-ndjson"}, method="POST")
        urllib.request.urlopen(r, timeout=120)
        if s % 100000 == 0 and s > 0: print(f"    {s:,}...")
    et = time.perf_counter()-t0
    print(f"  Indexed: {NUM_DOCS:,} in {et:.1f}s = {NUM_DOCS/et:,.0f} docs/sec")

    print("  Force merging...")
    es_req("POST","bench_1m/_forcemerge?max_num_segments=1")
    es_req("POST","bench_1m/_refresh")
    ec = es_req("GET","bench_1m/_count")["count"]
    print(f"  Count: {ec:,} | Merged to 1 segment")

    print(f"\n  Query latency ({ITERATIONS} iterations, {WARMUP} warmup):")
    er = []
    er.append(bench("BM25 text", lambda: es_req("POST","bench_1m/_search",{"query":{"match":{"message":random.choice(SEARCH_TERMS)}},"size":10})))
    er.append(bench("Term filter", lambda: es_req("POST","bench_1m/_search",{"query":{"term":{"category":random.choice(CATEGORIES)}},"size":10})))
    er.append(bench("Range filter", lambda: es_req("POST","bench_1m/_search",{"query":{"range":{"status":{"gte":400,"lte":503}}},"size":10})))
    er.append(bench("Count", lambda: es_req("POST","bench_1m/_count",{"query":{"match_all":{}}})))
    er.append(bench("Sort (resp_time desc)", lambda: es_req("POST","bench_1m/_search",{"query":{"match_all":{}},"sort":[{"response_time_ms":"desc"}],"size":10})))
    er.append(bench("Text + filter", lambda: es_req("POST","bench_1m/_search",{"query":{"bool":{"must":{"match":{"message":random.choice(SEARCH_TERMS)}},"filter":{"term":{"city":random.choice(CITIES)}}}},"size":10})))

    print(f"\n  Concurrent throughput (4 threads, 10s):")
    e_qps = concurrent_qps("BM25 mixed", lambda: es_req("POST","bench_1m/_search",{"query":{"match":{"message":random.choice(SEARCH_TERMS)}},"size":10}))

    print(f"\n  ES avg p50: {statistics.mean(r['p50'] for r in er):.2f}ms")

    # =============================================
    # COMPARISON
    # =============================================
    print("\n" + "="*80)
    print("  HEAD-TO-HEAD COMPARISON (1M docs, 4 CPUs, 8GB heap, 1 shard)")
    print("="*80)
    print(f"\n  {'Query Type':<25s} {'Jigyasa p50':>12s} {'ES p50':>10s} {'Ratio':>8s} {'J p99':>10s} {'ES p99':>10s}")
    print("  "+"-"*75)
    for j,e in zip(jr,er):
        n = j["name"]
        ratio = e["p50"]/j["p50"] if j["p50"]>0 else 0
        print(f"  {n:<25s} {j['p50']:>9.2f}ms {e['p50']:>8.2f}ms {ratio:>6.1f}x {j['p99']:>8.2f}ms {e['p99']:>8.2f}ms")

    print(f"\n  Indexing:    Jigyasa {NUM_DOCS/jt:>10,.0f} docs/s  |  ES {NUM_DOCS/et:>10,.0f} docs/s  |  {(NUM_DOCS/jt)/(NUM_DOCS/et):.1f}x")
    print(f"  Throughput:  Jigyasa {j_qps:>10,.0f} qps     |  ES {e_qps:>10,.0f} qps     |  {j_qps/e_qps:.1f}x")
    print(f"  Avg p50:     Jigyasa {statistics.mean(r['p50'] for r in jr):>10.2f}ms  |  ES {statistics.mean(r['p50'] for r in er):>10.2f}ms  |  {statistics.mean(r['p50'] for r in er)/statistics.mean(r['p50'] for r in jr):.1f}x")

    # Cleanup ES
    subprocess.run(["docker","rm","-f","es-1m"], capture_output=True)
    print("\n  Done. ES container removed.")

if __name__ == "__main__":
    main()
