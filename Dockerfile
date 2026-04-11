# Single-stage: copy pre-built fat JAR
FROM eclipse-temurin:21-jre
LABEL maintainer="Lokesh Mawale"
LABEL description="Jigyasa — Lucene 10.4 search engine for agent memory"

WORKDIR /app

# Copy pre-built fat JAR (build locally with: ./gradlew shadowJar)
COPY build/libs/Jigyasa-*-all.jar jigyasa.jar

# Data directories
RUN mkdir -p /data/index /data/translog

# Environment defaults
ENV GRPC_SERVER_PORT=50051
ENV INDEX_CACHE_DIR=/data/index
ENV TRANSLOG_DIRECTORY=/data/translog
ENV SERVER_MODE=READ_WRITE
ENV JAVA_OPTS="-Xms256m -Xmx1g -XX:+UseG1GC --add-modules jdk.incubator.vector -Dlucene.useScalarFMA=true -Dlucene.useVectorFMA=true"

EXPOSE 50051

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar jigyasa.jar"]
