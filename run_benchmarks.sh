#!/bin/bash
rm -f benchmark_results.txt

echo "=== Pairs + Combiner ===" >> benchmark_results.txt
rm -rf output_pc_1 output_pc_2 output_pc_3 output_pc_4
for d in 1 2 3 4; do
  /opt/homebrew/bin/hadoop jar MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar com.nosql.assignment2.CoOccurrencePairsCombiner Wikipedia-50-ARTICLES output_pc_$d output_final/part-r-00000 $d 2>&1 | grep "Execution Time" >> benchmark_results.txt
done

echo "=== Pairs + In-Mapper ===" >> benchmark_results.txt
rm -rf output_pim_1 output_pim_2 output_pim_3 output_pim_4
for d in 1 2 3 4; do
  /opt/homebrew/bin/hadoop jar MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar com.nosql.assignment2.CoOccurrencePairsInMapper Wikipedia-50-ARTICLES output_pim_$d output_final/part-r-00000 $d 2>&1 | grep "Execution Time" >> benchmark_results.txt
done

echo "=== Stripes + Combiner ===" >> benchmark_results.txt
rm -rf output_sc_1 output_sc_2 output_sc_3 output_sc_4
for d in 1 2 3 4; do
  /opt/homebrew/bin/hadoop jar MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar com.nosql.assignment2.CoOccurrenceStripesCombiner Wikipedia-50-ARTICLES output_sc_$d output_final/part-r-00000 $d 2>&1 | grep "Execution Time" >> benchmark_results.txt
done

echo "=== Stripes + In-Mapper ===" >> benchmark_results.txt
rm -rf output_sim_1 output_sim_2 output_sim_3 output_sim_4
for d in 1 2 3 4; do
  /opt/homebrew/bin/hadoop jar MapReduceWordCount/target/MapReduceWordCount-1.0-SNAPSHOT.jar com.nosql.assignment2.CoOccurrenceStripesInMapper Wikipedia-50-ARTICLES output_sim_$d output_final/part-r-00000 $d 2>&1 | grep "Execution Time" >> benchmark_results.txt
done
