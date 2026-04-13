#!/bin/bash
# build.sh — Compiles both Problem 2a and 2b and packages them into problem2.jar
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

echo "=== Building problem2 (2a + 2b) with Maven ==="
cd "$SCRIPT_DIR"
mvn clean package -q

echo ""
echo "Build successful → $SCRIPT_DIR/problem2.jar"
echo "  Entry points:"
echo "    parta.DocumentFrequency  (Problem 2a — Document Frequency)"
echo "    partb.TFIDFScorer        (Problem 2b — TF-IDF Scorer)"
