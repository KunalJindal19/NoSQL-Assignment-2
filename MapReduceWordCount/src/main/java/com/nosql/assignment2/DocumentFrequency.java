package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {

    // -------------------------------------------------------------
    // JOB 1: Document Frequency (DF) Calculation
    // -------------------------------------------------------------
    public static class DFMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<String>();
        private PorterStemmer stemmer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new PorterStemmer();
            URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    Path path = new Path(cacheURI.getPath());
                    parseStopWordsFile(path.getName());
                }
            }
        }

        private void parseStopWordsFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception parsing stopwords: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-z]+");
            
            // To compute DF, we only care if the term appears AT LEAST once in this document.
            // Using a HashSet automatically removes duplicate terms in the same document!
            Set<String> uniqueTermsInDoc = new HashSet<String>();
            
            for (String t : tokens) {
                if (!t.isEmpty() && !stopWords.contains(t)) {
                    // Stem the valid token using OpenNLP
                    uniqueTermsInDoc.add(stemmer.stem(t).toString());
                }
            }
            
            // Emit each unique stemmed term exactly once for this document
            for (String term : uniqueTermsInDoc) {
                word.set(term);
                context.write(word, one);
            }
        }
    }

    public static class DFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // -------------------------------------------------------------
    // JOB 2: Extract Top 100 High-DF Terms
    // -------------------------------------------------------------
    public static class DFPair implements Comparable<DFPair> {
        String term;
        int df;

        DFPair(String term, int df) {
            this.term = term;
            this.df = df;
        }

        @Override
        public int compareTo(DFPair other) {
            if (this.df != other.df) {
                return Integer.compare(this.df, other.df); // Ascending (min-heap)
            }
            return this.term.compareTo(other.term);
        }
    }

    public static class Top100Mapper extends Mapper<Object, Text, NullWritable, Text> {
        private PriorityQueue<DFPair> minHeap;
        private final int K = 100;

        @Override
        public void setup(Context context) {
            minHeap = new PriorityQueue<>();
        }

        @Override
        public void map(Object key, Text value, Context context) {
            String[] parts = value.toString().split("\\s+");
            if (parts.length >= 2) {
                String term = parts[0];
                int df = Integer.parseInt(parts[1]);

                minHeap.add(new DFPair(term, df));
                if (minHeap.size() > K) {
                    minHeap.poll(); // Discard the smallest
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (DFPair pair : minHeap) {
                context.write(NullWritable.get(), new Text(pair.term + "\t" + pair.df));
            }
        }
    }

    public static class Top100Reducer extends Reducer<NullWritable, Text, Text, IntWritable> {
        private PriorityQueue<DFPair> minHeap;
        private final int K = 100;

        @Override
        public void setup(Context context) {
            minHeap = new PriorityQueue<>();
        }

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) {
            for (Text value : values) {
                String[] parts = value.toString().split("\\t");
                if (parts.length >= 2) {
                    String term = parts[0];
                    int df = Integer.parseInt(parts[1]);
                    minHeap.add(new DFPair(term, df));
                    if (minHeap.size() > K) {
                        minHeap.poll();
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Unload the min-heap into an array to print descending
            DFPair[] sorted = new DFPair[minHeap.size()];
            for (int i = minHeap.size() - 1; i >= 0; i--) {
                sorted[i] = minHeap.poll();
            }
            for (DFPair pair : sorted) {
                context.write(new Text(pair.term), new IntWritable(pair.df));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DocumentFrequency <input path> <intermediate output> <final output> <stopwords path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        // --- JOB 1 Configurations ---
        Job job1 = Job.getInstance(conf, "Calculate Document Frequency");
        job1.setJarByClass(DocumentFrequency.class);
        job1.setMapperClass(DFMapper.class);
        job1.setCombinerClass(DFReducer.class);
        job1.setReducerClass(DFReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        // Cache stopwords exactly as requested:
        job1.addCacheFile(new Path(args[3]).toUri());
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        long startJob1 = System.currentTimeMillis();
        boolean success1 = job1.waitForCompletion(true);
        System.out.println("Job 1 (DF Extraction) Execution Time: " + (System.currentTimeMillis() - startJob1) + " ms");

        if (!success1) {
            System.exit(1);
        }

        // --- JOB 2 Configurations ---
        Job job2 = Job.getInstance(conf, "Extract Top 100 DF");
        job2.setJarByClass(DocumentFrequency.class);
        job2.setMapperClass(Top100Mapper.class);
        job2.setReducerClass(Top100Reducer.class);
        job2.setNumReduceTasks(1); // Force global Top-100 to one reducer
        
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1])); // Reads the full DF directory
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        long startJob2 = System.currentTimeMillis();
        boolean success2 = job2.waitForCompletion(true);
        System.out.println("Job 2 (Top 100 Filtering) Execution Time: " + (System.currentTimeMillis() - startJob2) + " ms");

        System.exit(success2 ? 0 : 1);
    }
}
