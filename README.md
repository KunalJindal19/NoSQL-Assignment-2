# NoSQL Systems - Assignment 2 (MapReduce)

This repository contains the complete MapReduce implementation for the NoSQL Systems Assignment 2. The project involves massive text processing, co-occurrence matrix generation, and a search-engine style TF-IDF indexing architecture using Apache Hadoop MapReduce on a dataset of Wikipedia articles.

---

## 🛠️ Prerequisites & Installation
Before executing the code, ensure the following dependencies are installed on your local system (macOS/Homebrew):

1. **Java Development Kit (JDK 8 or 17)**
   ```bash
   brew install openjdk@17
   ```
2. **Apache Hadoop (3.3.6)**
   ```bash
   brew install hadoop
   ```
3. **Apache Maven**
   ```bash
   brew install maven
   ```

*Note: The `opennlp-tools-1.9.3.jar` required for natural language stemming in Problem 2 is already included securely inside the root directory and is mapped automatically.*

---

## 🚀 Execution Guide (Using Makefile)

A completely customized `Makefile` has been provided to abstract away complex Hadoop CLI instructions, directory deletion, paths, and caching. 

### 1. Compile the Project
Before running any tests, successfully compile the Java source code into the execution `.jar`.
```bash
make compile
```

### 2. Clean Environment
Sanitize the project directory by wiping all messy `output_*` Hadoop directories and temporary benchmarks.
```bash
make clean
```

---

## 📖 Problem 1: Co-Occurring Word Matrix

### Problem 1(a) - Top 50 Words Extraction
Extract the top 50 stop-word filtered most frequent words across the Wikipedia dataset.
```bash
make 1a
```
*(Result located securely in: `cat output_final/part-r-00000`)*

### Problem 1(b) - Co-Occurrence (Pairs Algorithm)
Compute the word co-occurrence matrix using exactly the Top 50 words via the Pair algorithm.
You can specify the word distance constraint `d` using the Makefile variable (Default is `d=1`).
```bash
make 1b d=3
```

### Problem 1(c) - Co-Occurrence (Stripes Algorithm)
Compute the word co-occurrence matrix using the dramatically faster Stripes associative array algorithm.
```bash
make 1c d=4
```

### Problem 1(d) / 1(e) - Local Aggregations
Run iterative tests spanning the Combiner vs In-Mapper aggregation logic across both algorithms organically.
*   **Pairs + Combiner:** `make 1d_pc d=2`
*   **Pairs + In-Mapper Combining:** `make 1d_pim d=2`
*   **Stripes + Combiner:** `make 1d_sc d=2`
*   **Stripes + In-Mapper Combining:** `make 1d_sim d=2`

*(You can also securely run all 16 iterative benchmarks for $d=1...4$ by executing `./run_benchmarks.sh`)*

---

## 🔍 Problem 2: Indexing Documents via Hadoop

### Problem 2(a) - Document Frequency (DF)
Count the Document Frequency natively across all terms utilizing the OpenNLP PorterStemmer, and mechanically extract the exact top 100 via PriorityQueue Min-Heap.
```bash
make 2a
```
*(Result: A TSV DataFrame formatting `TERM \t DF` stored in `output_2a_top100/part-r-00000`)*

### Problem 2(b) - TF-IDF Matrix (Stripes Engine)
Ingest the `Top100` dataset via MapReduce DistributedCache. Map native Term Frequencies internally per document block via Stripes, and accurately calculate the TF-IDF Index multiplier using the strict formula: `SCORE = TF * Math.log(10000 / DF + 1)`.
```bash
make 2b
```
*(Result: A massive TSV Index formatting `docID \t term \t SCORE` stored in `output_2b_tfidf/part-r-00000`)*