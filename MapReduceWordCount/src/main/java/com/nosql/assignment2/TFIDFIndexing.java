package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import opennlp.tools.stemmer.PorterStemmer;

public class TFIDFIndexing {

    // Separator used inside the stripe value to delimit term:count pairs.
    // Pipe is safe because stemmed terms are purely alphabetic.
    private static final String PAIR_SEP = "|";
    private static final String KV_SEP   = ":";

    // -------------------------------------------------------------------------
    // Mapper — stripes algorithm with in-mapper combining
    // Accumulates term counts per document across all map() calls,
    // emits one stripe per document in cleanup() to minimize shuffle volume.
    // -------------------------------------------------------------------------
    public static class TFMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Integer> dfMap = new HashMap<String, Integer>();
        private PorterStemmer stemmer;

        // docId → { term → count } accumulated across all map() calls
        private Map<String, Map<String, Integer>> docStripes = new HashMap<String, Map<String, Integer>>();

        private final Text outKey   = new Text();
        private final Text outValue = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new PorterStemmer();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    // Use getPath() to get the actual local filesystem path
                    parseTop100DFFile(cacheURI.getPath());
                }
            }
            
            if (dfMap.isEmpty()) {
                System.err.println("WARNING: dfMap is empty after setup! Check Distributed Cache paths.");
            }
        }

        private void parseTop100DFFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        try {
                            dfMap.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                        } catch (NumberFormatException ignored) { }
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception parsing Top 100 cache: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Document ID = filename without extension
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String docId = filename.replaceAll("\\.[^.]+$", "");

            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-z]+");

            // Get or create the stripe for this document
            Map<String, Integer> stripe = docStripes.get(docId);
            if (stripe == null) {
                stripe = new HashMap<String, Integer>();
                docStripes.put(docId, stripe);
            }
            
            for (String t : tokens) {
                if (t.length() < 2) continue;  // skip single chars / empty
                String stemmedTerm = stemmer.stem(t).toString();
                
                // Filter: Only process if the term is part of our Top 100 universe!
                if (dfMap.containsKey(stemmedTerm)) {
                    Integer count = stripe.get(stemmedTerm);
                    stripe.put(stemmedTerm, count == null ? 1 : count + 1);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emit ONE stripe per document — true in-mapper combining
            for (Map.Entry<String, Map<String, Integer>> docEntry : docStripes.entrySet()) {
                outKey.set(docEntry.getKey());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> termEntry : docEntry.getValue().entrySet()) {
                    if (sb.length() > 0) sb.append(PAIR_SEP);
                    sb.append(termEntry.getKey()).append(KV_SEP).append(termEntry.getValue());
                }
                outValue.set(sb.toString());
                context.write(outKey, outValue);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Reducer — merges stripes, computes TF-IDF, emits ID<tab>TERM<tab>SCORE
    // -------------------------------------------------------------------------
    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> dfMap = new HashMap<String, Integer>();
        private final Text outValue = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Reducer needs the exact same DF mapping to perform the scaling calculation
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    // Use getPath() to get the actual local filesystem path
                    parseTop100DFFile(cacheURI.getPath());
                }
            }
            
            if (dfMap.isEmpty()) {
                System.err.println("WARNING: dfMap is empty in Reducer! Check Distributed Cache paths.");
            }
        }

        private void parseTop100DFFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        try {
                            dfMap.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                        } catch (NumberFormatException ignored) { }
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception parsing Top 100 cache: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Merge all partial stripes for this document
            Map<String, Integer> termCounts = new HashMap<String, Integer>();
            for (Text stripeText : values) {
                String[] pairs = stripeText.toString().split("\\" + PAIR_SEP);
                for (String pair : pairs) {
                    int sep = pair.indexOf(KV_SEP);
                    if (sep < 0) continue;
                    String term = pair.substring(0, sep);
                    try {
                        int count = Integer.parseInt(pair.substring(sep + 1));
                        Integer existing = termCounts.get(term);
                        termCounts.put(term, existing == null ? count : existing + count);
                    } catch (NumberFormatException ignored) { }
                }
            }
            
            // Compute and emit TF-IDF score for each term
            for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
                String term = entry.getKey();
                int tf = entry.getValue();
                int df = dfMap.containsKey(term) ? dfMap.get(term) : 1;
                
                double score = tf * Math.log(10000.0 / df + 1.0);
                
                // key = docId, value = "term\tscore"
                // TextOutputFormat writes: docId\tterm\tscore  →  ID<tab>TERM<tab>SCORE ✓
                outValue.set(term + "\t" + String.format("%.6f", score));
                context.write(key, outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TFIDFIndexing <input path> <output path> <top100 cached file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate TF-IDF Indexing via Stripes");
        job.setJarByClass(TFIDFIndexing.class);
        
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
        
        // Both mapper and reducer now use Text for key and value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Cache the Top 100 DF file generated by subproblem 2(a)
        // Using makeQualified to ensure the path is absolute for LocalJobRunner
        Path cachePath = new Path(args[2]);
        job.addCacheFile(cachePath.getFileSystem(conf).makeQualified(cachePath).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        System.out.println(">>> Total Task 2b Execution Time: " + (System.currentTimeMillis() - startTime) + " ms <<<");

        System.exit(success ? 0 : 1);
    }
}
