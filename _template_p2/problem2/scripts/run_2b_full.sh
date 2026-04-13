#!/bin/bash
# scripts/run_2b_full.sh — Runs Problem 2b (TF-IDF Scorer) on the full Wikipedia dump.
#
# Requires df_top100_full.tsv to already be on HDFS.
# Run run_2a_full.sh and extract_top100_full.sh first.
#
# Output saved locally to:  <project-root>/output/tfidf_output_full.tsv
#
# Usage:
#   ./scripts/run_2b_full.sh [input-hdfs-path] [hdfs-output-path] [hdfs-df_top100-path]
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # project root
HDFS_TOP100=${3:-/user/$USER/df_top100_full.tsv}

INPUT=${1:-/user/$USER/input/wiki_full/Wikipedia-EN-20120601_ARTICLES}
HDFS_OUTPUT=${2:-/user/$USER/output/tfidf_output_full}
LOCAL_OUTPUT="$SCRIPT_DIR/output/tfidf_output_full.tsv"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/mnt/newstorage/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

mkdir -p "$SCRIPT_DIR/output"

# Verify df_top100_full.tsv is on HDFS
if ! hdfs dfs -test -e "$HDFS_TOP100" 2>/dev/null; then
    echo "ERROR: $HDFS_TOP100 not found on HDFS."
    echo "Run ./scripts/run_2a_full.sh then ./scripts/extract_top100_full.sh first."
    exit 1
fi

hdfs dfs -rm -r "$HDFS_OUTPUT" 2>/dev/null && echo "Removed old HDFS output" || true

echo ""
echo "=== [2b FULL] Running TFIDFScorer ==="
echo "Input        : $INPUT"
echo "HDFS Output  : $HDFS_OUTPUT"
echo "DF Top-100   : $HDFS_TOP100"
echo "Local Output : $LOCAL_OUTPUT"
echo ""

START=$(date +%s)

hadoop jar "$SCRIPT_DIR/problem2.jar" partb.TFIDFScorer \
    -D mapreduce.job.jvm.numtasks=-1 \
    -D mapreduce.map.speculative=false \
    -D mapreduce.reduce.speculative=false \
    "$INPUT" \
    "$HDFS_OUTPUT" \
    "$HDFS_TOP100"

END=$(date +%s)

# Fetch merged output to local filesystem via /tmp
TEMP="/tmp/tfidf_output_full_$$.tsv"
hdfs dfs -getmerge "$HDFS_OUTPUT" "$TEMP"
mv "$TEMP" "$LOCAL_OUTPUT"

echo ""
echo "=== [2b FULL] Done in $((END-START))s ==="
echo "Output: $LOCAL_OUTPUT"
echo ""
echo "Sample rows (ID<tab>TERM<tab>SCORE):"
echo "-------------------------------------"
head -20 "$LOCAL_OUTPUT"
