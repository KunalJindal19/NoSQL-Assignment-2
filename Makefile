HADOOP_BIN = /opt/homebrew/bin/hadoop
MVN_BIN = /opt/homebrew/bin/mvn
JAR_FILE = MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar
INPUT = Wikipedia-EN-20120601_ARTICLES
TOP_50_CACHE = output_final/part-r-00000

# Optimization for Local Mode
export HADOOP_HEAPSIZE_MAX=4096

# Export local jar to Hadoop's execution classpath
export HADOOP_CLASSPATH=$(PWD)/opennlp-tools-1.9.3.jar

# Default distance if none provided (e.g. `make 1b` runs with d=1, `make 1b d=3` runs with d=3)
d ?= 1

.PHONY: compile 1a 1b 1c 1d_pc 1d_pim 1d_sc 1d_sim 1d_pairs 1d_stripes clean run_benchmarks

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
	rm -rf output_pairs_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairs $(INPUT) output_pairs_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_pairs_$(d).log
	@grep "Execution Time" output_pairs_$(d).log > output_pairs_$(d)/runtime.txt
	@echo "\n=> Result saved in output_pairs_$(d)/part-r-00000"
	@echo "=> Runtime saved in output_pairs_$(d)/runtime.txt"

# ---------------------------------------------------------
# Problem 1c - Co-Occurrence Matrix (Stripes)
# Usage: make 1c d=4
# ---------------------------------------------------------
1c:
	rm -rf output_stripes_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripes $(INPUT) output_stripes_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_stripes_$(d).log
	@grep "Execution Time" output_stripes_$(d).log > output_stripes_$(d)/runtime.txt
	@echo "\n=> Result saved in output_stripes_$(d)/part-r-00000"
	@echo "=> Runtime saved in output_stripes_$(d)/runtime.txt"

# ---------------------------------------------------------
# Problem 1d - Local Aggregations (4 variations)
# Usage: make 1d_pc d=2 (Pairs + Combiner)
#        make 1d_pim d=2 (Pairs + In-Mapper)
#        make 1d_sc d=2 (Stripes + Combiner)
#        make 1d_sim d=2 (Stripes + In-Mapper)
# ---------------------------------------------------------

1d_pc:
	rm -rf output_1d_pc_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairsCombiner $(INPUT) output_1d_pc_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_pc_$(d).log
	@grep "Execution Time" output_1d_pc_$(d).log > output_1d_pc_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_pc_$(d)/part-r-00000"

1d_pim:
	rm -rf output_1d_pim_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairsInMapper $(INPUT) output_1d_pim_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_pim_$(d).log
	@grep "Execution Time" output_1d_pim_$(d).log > output_1d_pim_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_pim_$(d)/part-r-00000"

1d_sc:
	rm -rf output_1d_sc_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripesCombiner $(INPUT) output_1d_sc_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_sc_$(d).log
	@grep "Execution Time" output_1d_sc_$(d).log > output_1d_sc_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_sc_$(d)/part-r-00000"

1d_sim:
	rm -rf output_1d_sim_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripesInMapper $(INPUT) output_1d_sim_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_sim_$(d).log
	@grep "Execution Time" output_1d_sim_$(d).log > output_1d_sim_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_sim_$(d)/part-r-00000"

# ---------------------------------------------------------
# Problem 1d - Local Aggregations (Optimized)
# ---------------------------------------------------------

1d_pairs:
	rm -rf output_1d_pairs_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrencePairsInMapper $(INPUT) output_1d_pairs_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_pairs_$(d).log
	@grep "Execution Time" output_1d_pairs_$(d).log > output_1d_pairs_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_pairs_$(d)/part-r-00000"

1d_stripes:
	rm -rf output_1d_stripes_$(d)
	$(HADOOP_BIN) jar $(JAR_FILE) com.nosql.assignment2.CoOccurrenceStripesInMapper $(INPUT) output_1d_stripes_$(d) $(TOP_50_CACHE) $(d) 2>&1 | tee output_1d_stripes_$(d).log
	@grep "Execution Time" output_1d_stripes_$(d).log > output_1d_stripes_$(d)/runtime.txt
	@echo "\n=> Result saved in output_1d_stripes_$(d)/part-r-00000"

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
