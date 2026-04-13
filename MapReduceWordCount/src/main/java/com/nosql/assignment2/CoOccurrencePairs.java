package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
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

public class CoOccurrencePairs {

    public static class PairsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pairText = new Text();
        private Set<String> frequentWords = new HashSet<String>();
        private int distance;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("cooccurrence.distance", 1);
            
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheURI : cacheFiles) {
                    parseTop50File(cacheURI.getPath());
                }
            }
        }

        private void parseTop50File(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 1) {
                        frequentWords.add(parts[0].trim().toLowerCase());
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Caught exception parsing cached file: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-z]+");

            for (int i = 0; i < tokens.length; i++) {
                String u = tokens[i];
                if (u.isEmpty() || !frequentWords.contains(u)) {
                    continue; 
                }

                // Check window of size d
                int end = Math.min(i + distance, tokens.length - 1);
                for (int j = i + 1; j <= end; j++) {
                    String v = tokens[j];
                    if (v.isEmpty() || !frequentWords.contains(v)) {
                        continue;
                    }
                    if (u.equals(v)) {
                        continue; // skip self co-occurrence
                    }

                    // Enforce lexical order to prevent (a,b) and (b,a) being separate keys
                    String pairStr = (u.compareTo(v) < 0) ? (u + "," + v) : (v + "," + u);
                    pairText.set(pairStr);
                    context.write(pairText, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CoOccurrencePairs <input path> <output path> <top50 file path> <distance>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String top50FilePath = args[2];
        int distance = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        conf.setInt("cooccurrence.distance", distance);
        
        Job job = Job.getInstance(conf, "Co-Occurrence Pairs d=" + distance);
        job.setJarByClass(CoOccurrencePairs.class);
        
        job.setMapperClass(PairsMapper.class);
        // Note: Problem 1(b) asks for pure pairs approach, so we DO NOT use a Combiner here (that is saved for 1e)
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path(top50FilePath).toUri());

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        
        System.out.println("=================================================");
        System.out.println("PAIRS APPROACH RUNTIME REPORT for d=" + distance);
        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println("=================================================");
        
        System.exit(success ? 0 : 1);
    }
}
