@echo off
REM Jigyasa production launcher — ensures SIMD vectorization and memory optimization
REM Usage: jigyasa.bat [JVM_ARGS...]
REM
REM Environment variables:
REM   BOOTSTRAP_MEMORY_LOCK=true  — pin JVM heap to RAM via VirtualLock
REM   JIGYASA_HEAP_SIZE=1g        — JVM heap size (default: 1g)

set SCRIPT_DIR=%~dp0
set JAR=%SCRIPT_DIR%build\libs\Jigyasa-1.0-SNAPSHOT-all.jar

if "%JIGYASA_HEAP_SIZE%"=="" set JIGYASA_HEAP_SIZE=1g

java ^
    -Xms%JIGYASA_HEAP_SIZE% -Xmx%JIGYASA_HEAP_SIZE% ^
    --add-modules jdk.incubator.vector ^
    -XX:+AlwaysPreTouch ^
    -Dlucene.useScalarFMA=true ^
    -Dlucene.useVectorFMA=true ^
    %* ^
    -jar "%JAR%"
