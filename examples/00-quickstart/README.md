# 00 — Quickstart

Get Jigyasa running and execute your first search in 5 minutes.

## Option A: Fat JAR

```bash
# From the jigyasa root directory
./gradlew shadowJar
java -jar build/libs/Jigyasa-1.0-SNAPSHOT-all.jar
```

## Option B: Docker

```bash
docker compose -f ../../docker-compose.yml up -d
```

Server starts on **localhost:50051** (gRPC).

## Run the Example

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:00-quickstart:run

# Python
cd examples/00-quickstart/python
pip install grpcio grpcio-tools grpcio-reflection googleapis-common-protos
python quickstart.py
```

### grpcurl (no code needed)

```bash
# Install: https://github.com/fullstorydev/grpcurl
# Health check
grpcurl -plaintext localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Health

# See grpcurl/ folder for more commands
```

## What This Example Does

1. Connects to Jigyasa on `localhost:50051`
2. Checks server health
3. Creates a collection with a simple schema
4. Indexes 5 documents
5. Runs a text search
6. Prints results
