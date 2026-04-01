# Stage 1: Build
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /build
COPY . .
RUN chmod +x gradlew && ./gradlew shadowJar --no-daemon -x test

# Stage 2: Runtime
FROM eclipse-temurin:21-jre
LABEL maintainer="Lokesh Mawale"
LABEL description="Jigyasa — Lucene 10.4 search engine for agent memory"

WORKDIR /app

# Copy fat JAR
COPY --from=builder /build/build/libs/*-all.jar jigyasa.jar

# Data directories
RUN mkdir -p /data/index /data/translog

# Environment defaults
ENV GRPC_SERVER_PORT=50051
ENV INDEX_CACHE_DIR=/data/index
ENV TRANSLOG_DIRECTORY=/data/translog
ENV SERVER_MODE=READ_WRITE
ENV JAVA_OPTS="-Xms256m -Xmx1g -XX:+UseG1GC"

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["java", "-cp", "jigyasa.jar", "com.jigyasa.dp.search.entrypoint.HealthCheck"]

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar jigyasa.jar"]
