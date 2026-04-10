package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import opennlp.tools.stemmer.PorterStemmer;

public class TFIDFIndexing {

    public static class TFMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Map<String, Integer> dfMap = new HashMap<String, Integer>();
        private PorterStemmer stemmer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new PorterStemmer();
            URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    Path path = new Path(cacheURI.getPath());
                    parseTop100DFFile(path.getName());
                }
            }
        }

        private void parseTop100DFFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        dfMap.put(parts[0], Integer.parseInt(parts[1]));
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception parsing Top 100 cache: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-z]+");
            
            // Extract the Document ID (the actual filename)
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String docId = fileSplit.getPath().getName();
            
            MapWritable stripe = new MapWritable();
            
            for (String t : tokens) {
                if (!t.isEmpty()) {
                    String stemmedTerm = stemmer.stem(t).toString();
                    
                    // Filter: Only process if the term is part of our Top 100 universe!
                    if (dfMap.containsKey(stemmedTerm)) {
                        Text termText = new Text(stemmedTerm);
                        if (stripe.containsKey(termText)) {
                            IntWritable count = (IntWritable) stripe.get(termText);
                            count.set(count.get() + 1);
                        } else {
                            stripe.put(termText, new IntWritable(1));
                        }
                    }
                }
            }
            
            // Emit <DocID, {Term -> TF}>
            if (!stripe.isEmpty()) {
                context.write(new Text(docId), stripe);
            }
        }
    }

    public static class TFIDFReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private Map<String, Integer> dfMap = new HashMap<String, Integer>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Reducer needs the exact same DF mapping to perform the scaling calculation
            URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    Path path = new Path(cacheURI.getPath());
                    parseTop100DFFile(path.getName());
                }
            }
        }

        private void parseTop100DFFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        dfMap.put(parts[0], Integer.parseInt(parts[1]));
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception parsing Top 100 cache: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregatedDocStripe = new MapWritable();
            
            // Combine all MapWritables mapped to this document
            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text term = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    
                    if (aggregatedDocStripe.containsKey(term)) {
                        IntWritable finalCount = (IntWritable) aggregatedDocStripe.get(term);
                        finalCount.set(finalCount.get() + count.get());
                    } else {
                        aggregatedDocStripe.put(new Text(term), new IntWritable(count.get()));
                    }
                }
            }
            
            // Complete mathematically scaled output
            for (Map.Entry<Writable, Writable> entry : aggregatedDocStripe.entrySet()) {
                String term = ((Text) entry.getKey()).toString();
                int tf = ((IntWritable) entry.getValue()).get();
                
                if (dfMap.containsKey(term)) {
                    int df = dfMap.get(term);
                    
                    // Enforce the required log function strictly via Math.log() base e
                    double score = tf * Math.log(10000.0 / df + 1.0);
                    
                    // Format Output strictly as "ID \t TERM" -> "SCORE"
                    String formattedKey = key.toString() + "\t" + term;
                    context.write(new Text(formattedKey), new DoubleWritable(score));
                }
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
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Cache the Top 100 DF file generated by subproblem 2(a)
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) + " ms");

        System.exit(success ? 0 : 1);
    }
}
