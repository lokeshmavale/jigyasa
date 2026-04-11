# Benchmarks

Reproduce all performance claims from the README.

## Prerequisites

- Java 21+
- Python 3.10+ with gRPC dependencies
- Docker (for Elasticsearch comparison)

```bash
pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos
```

## Scripts

| Script | What it does |
|---|---|
| `benchmark.py` | Core latency benchmarks (BM25, term, range, boolean, sort, count) |
| `benchmark_advanced.py` | Vector KNN, hybrid RRF, concurrent throughput |
| `benchmark_comparison.py` | Head-to-head Jigyasa vs Elasticsearch on identical data |
| `benchmark_scale.py` | Scale tests at 10K, 100K, 1M document counts |

## Running

### 1. Start Jigyasa

```bash
cd ..
./gradlew shadowJar
java -jar build/libs/Jigyasa-1.0.4-all.jar
```

### 2. Start Elasticsearch (for comparison only)

```bash
docker run -d --name es-bench \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  docker.elastic.co/elasticsearch/elasticsearch:8.13.0
```

### 3. Run benchmarks

```bash
# Jigyasa-only benchmarks
python benchmark.py
python benchmark_advanced.py
python benchmark_scale.py

# Jigyasa vs Elasticsearch comparison
python benchmark_comparison.py
```

Results print to stdout with p50/p95/p99 latencies. Redirect to a file if needed:

```bash
python benchmark_comparison.py | tee results.txt
```
