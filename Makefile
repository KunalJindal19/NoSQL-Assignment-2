HADOOP_BIN = /opt/homebrew/bin/hadoop
MVN_BIN = /opt/homebrew/bin/mvn
JAR_FILE = MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar
INPUT = Wikipedia-50-ARTICLES
TOP_50_CACHE = output_final/part-r-00000

# Export local jar to Hadoop's execution classpath
export HADOOP_CLASSPATH=$(PWD)/opennlp-tools-1.9.3.jar

# Default distance if none provided (e.g. `make 1b` runs with d=1, `make 1b d=3` runs with d=3)
d ?= 1

.PHONY: compile 1a 1b 1c 1d_pc 1d_pim 1d_sc 1d_sim clean run_benchmarks

# ---------------------------------------------------------
# Compilation
# ---------------------------------------------------------
compile:
	cd MapReduceWordCount && $(MVN_BIN) clean package

# ---------------------------------------------------------
# Problem 1a - Top 50 Words Extraction
# ---------------------------------------------------------
1a:
	rm -rf output_job1 output_final
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.Top50Words $(INPUT) output_job1 output_final stopwords.txt
	@echo "\n=> Check result: cat output_final/part-r-00000"

# ---------------------------------------------------------
# Problem 1b - Co-Occurrence Matrix (Pairs)
# Usage: make 1b d=3
# ---------------------------------------------------------
1b:
	rm -rf output_pairs
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairs $(INPUT) output_pairs $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_pairs/part-r-00000"

# ---------------------------------------------------------
# Problem 1c - Co-Occurrence Matrix (Stripes)
# Usage: make 1c d=4
# ---------------------------------------------------------
1c:
	rm -rf output_stripes
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripes $(INPUT) output_stripes $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_stripes/part-r-00000"

# ---------------------------------------------------------
# Problem 1d - Local Aggregations (4 variations)
# Usage: make 1d_pc d=2 (Pairs + Combiner)
#        make 1d_pim d=2 (Pairs + In-Mapper)
#        make 1d_sc d=2 (Stripes + Combiner)
#        make 1d_sim d=2 (Stripes + In-Mapper)
# ---------------------------------------------------------

1d_pc:
	rm -rf output_1d_pc
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairsCombiner $(INPUT) output_1d_pc $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_1d_pc/part-r-00000"

1d_pim:
	rm -rf output_1d_pim
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairsInMapper $(INPUT) output_1d_pim $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_1d_pim/part-r-00000"

1d_sc:
	rm -rf output_1d_sc
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripesCombiner $(INPUT) output_1d_sc $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_1d_sc/part-r-00000"

1d_sim:
	rm -rf output_1d_sim
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripesInMapper $(INPUT) output_1d_sim $(TOP_50_CACHE) $(d)
	@echo "\n=> Check result: cat output_1d_sim/part-r-00000"

# ---------------------------------------------------------
# Utilities
# ---------------------------------------------------------
clean:
	rm -rf output_* benchmark_results.txt
	@echo "Cleaned all output directories."

# ---------------------------------------------------------
# Problem 2a - Document Frequency (DF) & Top 100 Extraction
# ---------------------------------------------------------
2a:
	rm -rf output_2a_all output_2a_top100
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.DocumentFrequency $(INPUT) output_2a_all output_2a_top100 stopwords.txt
	@echo "\n=> Check result: cat output_2a_top100/part-r-00000"

# ---------------------------------------------------------
# Problem 2b - TF-IDF Indexing (Stripes)
# ---------------------------------------------------------
2b:
	rm -rf output_2b_tfidf
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.TFIDFIndexing $(INPUT) output_2b_tfidf output_2a_top100/part-r-00000
	@echo "\n=> Check result: cat output_2b_tfidf/part-r-00000"
