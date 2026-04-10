package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class CoOccurrencePairsInMapper {

    public static class PairsInMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Set<String> frequentWords = new HashSet<String>();
        private int distance;
        
        // --- 1(e) REQUIRED ADDITION: MAP-FUNCTION LEVEL AGGREGATION ---
        private Map<String, Integer> localAggregationMap;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("cooccurrence.distance", 1);
            localAggregationMap = new HashMap<String, Integer>();
            
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    Path path = new Path(cacheURI.getPath());
                    parseTop50File(path.getName());
                }
            }
        }

        private void parseTop50File(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 1) frequentWords.add(parts[0].trim().toLowerCase());
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Exception: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-z]+");
            for (int i = 0; i < tokens.length; i++) {
                String u = tokens[i];
                if (u.isEmpty() || !frequentWords.contains(u)) continue; 
                int end = Math.min(i + distance, tokens.length - 1);
                for (int j = i + 1; j <= end; j++) {
                    String v = tokens[j];
                    if (v.isEmpty() || !frequentWords.contains(v) || u.equals(v)) continue;
                    String pairStr = (u.compareTo(v) < 0) ? (u + "," + v) : (v + "," + u);
                    
                    // INSTANT IN-MEMORY AGGREGATION. NO NETWORK EMISSION HERE.
                    int count = localAggregationMap.containsKey(pairStr) ? localAggregationMap.get(pairStr) : 0;
                    localAggregationMap.put(pairStr, count + 1);
                }
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emits exactly once per mapper shutdown
            Text pairText = new Text();
            IntWritable countWritable = new IntWritable();
            for (Map.Entry<String, Integer> entry : localAggregationMap.entrySet()) {
                pairText.set(entry.getKey());
                countWritable.set(entry.getValue());
                context.write(pairText, countWritable);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) System.exit(-1);
        int distance = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        conf.setInt("cooccurrence.distance", distance);
        
        Job job = Job.getInstance(conf, "Co-Occurrence Pairs In-Mapper d=" + distance);
        job.setJarByClass(CoOccurrencePairsInMapper.class);
        
        job.setMapperClass(PairsInMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) + " ms");
        System.exit(success ? 0 : 1);
    }
}
