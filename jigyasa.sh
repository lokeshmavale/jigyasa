#!/bin/bash
# Jigyasa production launcher — ensures SIMD vectorization and memory optimization
# Usage: ./jigyasa.sh [JVM_ARGS...]
#
# Environment variables:
#   BOOTSTRAP_MEMORY_LOCK=true  — pin JVM heap to RAM via mlockall
#   JIGYASA_HEAP_SIZE=1g        — JVM heap size (default: 1g)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$SCRIPT_DIR/build/libs/Jigyasa-1.0-SNAPSHOT-all.jar"

HEAP="${JIGYASA_HEAP_SIZE:-1g}"

exec java \
    -Xms${HEAP} -Xmx${HEAP} \
    --add-modules jdk.incubator.vector \
    -XX:+AlwaysPreTouch \
    -Dlucene.useScalarFMA=true \
    -Dlucene.useVectorFMA=true \
    "$@" \
    -jar "$JAR"
