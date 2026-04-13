#!/bin/bash
# scripts/run_2b.sh — Runs Problem 2b: TF-IDF Scorer MapReduce job.
#
# Requires df_top100.tsv to already be on HDFS (produced by extract_top100.sh).
# Output saved locally to:  <project-root>/output/tfidf_output.tsv
#
# Usage:
#   ./scripts/run_2b.sh [input-hdfs-path] [hdfs-output-path] [hdfs-df_top100-path]
#
# Defaults to the 50-article test set.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # project root
HDFS_TOP100=${3:-/user/$USER/df_top100.tsv}

INPUT=${1:-/user/$USER/input/wiki50/wiki50}
HDFS_OUTPUT=${2:-/user/$USER/output/tfidf_output}
LOCAL_OUTPUT="$SCRIPT_DIR/output/tfidf_output.tsv"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/mnt/newstorage/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

mkdir -p "$SCRIPT_DIR/output"

# Verify df_top100.tsv is on HDFS
if ! hdfs dfs -test -e "$HDFS_TOP100" 2>/dev/null; then
    echo "ERROR: $HDFS_TOP100 not found on HDFS."
    echo "Run ./scripts/extract_top100.sh first to generate it."
    exit 1
fi

hdfs dfs -rm -r "$HDFS_OUTPUT" 2>/dev/null && echo "Removed old HDFS output" || true

echo ""
echo "=== [2b] Running TFIDFScorer ==="
echo "Input        : $INPUT"
echo "HDFS Output  : $HDFS_OUTPUT"
echo "DF Top-100   : $HDFS_TOP100"
echo "Local Output : $LOCAL_OUTPUT"
echo ""

START=$(date +%s)

hadoop jar "$SCRIPT_DIR/problem2.jar" partb.TFIDFScorer \
    "$INPUT" \
    "$HDFS_OUTPUT" \
    "$HDFS_TOP100"

END=$(date +%s)

# Fetch merged output to local filesystem via /tmp
TEMP="/tmp/tfidf_output_$$.tsv"
hdfs dfs -getmerge "$HDFS_OUTPUT" "$TEMP"
mv "$TEMP" "$LOCAL_OUTPUT"

echo ""
echo "=== [2b] Done in $((END-START))s ==="
echo "Output: $LOCAL_OUTPUT"
echo ""
echo "Sample rows (ID<tab>TERM<tab>SCORE):"
echo "-------------------------------------"
head -20 "$LOCAL_OUTPUT"
