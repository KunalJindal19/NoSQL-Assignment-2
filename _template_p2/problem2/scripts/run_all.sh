# run_all.sh — Builds the project and runs all steps in sequence:
#              build -> upload -> 2a -> top100 -> 2b
#
# Run from the project root:
#   ./scripts/run_all.sh
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo ""
echo "================================================"
echo " NoSQL Assignment 2 — Full Pipeline"
echo "================================================"

echo ""
# echo "--- Step 0/5: Build JAR ---"
# bash "$PROJECT_DIR/build.sh"

# echo ""
# echo "--- Step 1/5: Upload data to HDFS ---"
# bash "$SCRIPT_DIR/01_upload_data.sh"

echo ""
echo "--- Step 2/5: Run Problem 2a (Document Frequency) ---"
bash "$SCRIPT_DIR/run_2a_full.sh"

echo ""
echo "--- Step 3/5: Extract top 100 terms ---"
bash "$SCRIPT_DIR/extract_top100_full.sh"

echo ""
echo "--- Step 4/5: Run Problem 2b (TF-IDF Scorer) ---"
bash "$SCRIPT_DIR/run_2b_full.sh"

echo ""
echo "================================================"
echo " All done!"
echo " Results in: $PROJECT_DIR/output/"
echo "   df_output.tsv     — Document Frequency (all terms)"
echo "   df_top100.tsv     — Top 100 terms by DF"
echo "   tfidf_output.tsv  — TF-IDF scores (ID<tab>TERM<tab>SCORE)"
echo "================================================"