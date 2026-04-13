#!/bin/bash
# scripts/run_2a_full.sh — Runs Problem 2a (Document Frequency) on the full Wikipedia dump.
#
# Output saved locally to:  <project-root>/output/df_output_full.tsv
#
# Usage:
#   ./scripts/run_2a_full.sh [input-hdfs-path] [hdfs-output-path]
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # project root
ASSIGNMENT_DIR="/home/krish/Desktop/IIITB/6th semester/NoSQL/Assignment 2"
STOPWORDS_LOCAL="$ASSIGNMENT_DIR/stopwords.txt"
HDFS_STOPWORDS="/user/$USER/stopwords.txt"

INPUT=${1:-/user/$USER/input/wiki_full/Wikipedia-EN-20120601_ARTICLES}
HDFS_OUTPUT=${2:-/user/$USER/output/df_output_full}
LOCAL_OUTPUT="$SCRIPT_DIR/output/df_output_full.tsv"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/mnt/newstorage/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

mkdir -p "$SCRIPT_DIR/output"

# Upload stopwords to HDFS if not already present
if ! hdfs dfs -test -e "$HDFS_STOPWORDS" 2>/dev/null; then
    echo "Uploading stopwords.txt to HDFS ..."
    hdfs dfs -put "$STOPWORDS_LOCAL" "$HDFS_STOPWORDS"
fi

hdfs dfs -rm -r "$HDFS_OUTPUT" 2>/dev/null && echo "Removed old HDFS output" || true

echo ""
echo "=== [2a FULL] Running DocumentFrequency ==="
echo "Input       : $INPUT"
echo "HDFS Output : $HDFS_OUTPUT"
echo "Local Output: $LOCAL_OUTPUT"
echo ""

START=$(date +%s)

hadoop jar "$SCRIPT_DIR/problem2.jar" parta.DocumentFrequency \
    -D mapreduce.job.jvm.numtasks=-1 \
    -D mapreduce.map.speculative=false \
    -D mapreduce.reduce.speculative=false \
    "$INPUT" \
    "$HDFS_OUTPUT" \
    "$HDFS_STOPWORDS"

END=$(date +%s)

# Fetch merged output to local filesystem via /tmp (avoids spaces-in-path issue)
TEMP="/tmp/df_output_full_$$.tsv"
hdfs dfs -getmerge "$HDFS_OUTPUT" "$TEMP"
mv "$TEMP" "$LOCAL_OUTPUT"

echo ""
echo "=== [2a FULL] Done in $((END-START))s ==="
echo "Output: $LOCAL_OUTPUT"
echo ""
echo "Top 20 terms by Document Frequency:"
sort -t$'\t' -k2 -rn "$LOCAL_OUTPUT" | head -20
