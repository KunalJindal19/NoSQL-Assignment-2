#!/bin/bash
# scripts/extract_top100.sh — Extracts the top 100 terms by DF from 2a's output.
#
# Reads:   <project-root>/output/df_output.tsv  (local, produced by run_2a.sh)
# Writes:  <project-root>/output/df_top100.tsv  (local)
#          /user/$USER/df_top100.tsv             (HDFS, cached by run_2b.sh)
#
# Usage:
#   ./scripts/extract_top100.sh [local-df-tsv] [hdfs-top100-path]
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # project root
LOCAL_DF=${1:-$SCRIPT_DIR/output/df_output.tsv}
LOCAL_TOP100="$SCRIPT_DIR/output/df_top100.tsv"
HDFS_TOP100=${2:-/user/$USER/df_top100.tsv}

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/mnt/newstorage/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

if [ ! -f "$LOCAL_DF" ]; then
    echo "ERROR: $LOCAL_DF not found. Run ./scripts/run_2a.sh first."
    exit 1
fi

echo "=== Extracting top 100 terms from $LOCAL_DF ==="

sort -t$'\t' -k2 -rn "$LOCAL_DF" | head -100 > "$LOCAL_TOP100"

echo "Top 100 terms (TERM<tab>DF):"
echo "----------------------------"
cat "$LOCAL_TOP100"

# Upload to HDFS via /tmp to avoid spaces-in-path issue
TEMP="/tmp/df_top100_$$.tsv"
cp "$LOCAL_TOP100" "$TEMP"
hdfs dfs -rm -f "$HDFS_TOP100" 2>/dev/null || true
hdfs dfs -put "$TEMP" "$HDFS_TOP100"
rm -f "$TEMP"

echo ""
echo "Local : $LOCAL_TOP100"
echo "HDFS  : $HDFS_TOP100  ← pass this to run_2b.sh"
