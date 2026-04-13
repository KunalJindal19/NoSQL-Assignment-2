package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Top50Words {

    // ========== JOB 1: Word Count Mapper & Reducer ========== //

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                if (patternsURIs != null) {
                    for (URI patternsURI : patternsURIs) {
                        parseSkipFile(patternsURI.getPath());
                    }
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = reader.readLine()) != null) {
                    if (!pattern.trim().isEmpty()) {
                        stopWords.add(pattern.trim().toLowerCase());
                    }
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            // Split by non-alphabetic characters
            String[] tokens = line.split("[^a-z]+");
            for (String token : tokens) {
                if (token.isEmpty() || stopWords.contains(token)) {
                    continue;
                }
                word.set(token);
                context.write(word, one);
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

    // ========== JOB 2: Top-K Mapper & Reducer ========== //

    // A helper class to store word counts
    static class WordCountPair {
        String word;
        int count;

        WordCountPair(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }

    public static class TopKMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
        private PriorityQueue<WordCountPair> pq;

        @Override
        public void setup(Context context) {
            pq = new PriorityQueue<>(50, new Comparator<WordCountPair>() {
                public int compare(WordCountPair p1, WordCountPair p2) {
                    return Integer.compare(p1.count, p2.count); // Min-heap based on count
                }
            });
        }

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            pq.add(new WordCountPair(key.toString(), value.get()));
            if (pq.size() > 50) {
                pq.poll(); // Remove the smallest element to keep size exactly 50
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            while (!pq.isEmpty()) {
                WordCountPair pair = pq.poll();
                // Send word and count encoded as string to reducer
                context.write(NullWritable.get(), new Text(pair.word + "\t" + pair.count));
            }
        }
    }

    public static class TopKReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
        private PriorityQueue<WordCountPair> pq;

        @Override
        public void setup(Context context) {
            pq = new PriorityQueue<>(50, new Comparator<WordCountPair>() {
                public int compare(WordCountPair p1, WordCountPair p2) {
                    return Integer.compare(p1.count, p2.count); // Min-heap
                }
            });
        }

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length == 2) {
                    pq.add(new WordCountPair(parts[0], Integer.parseInt(parts[1])));
                    if (pq.size() > 50) {
                        pq.poll(); // Maintain top 50 globally
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Since PriorityQueue pulls out lowest first, let's reverse it to output from highest to lowest
            WordCountPair[] sortedPairs = new WordCountPair[pq.size()];
            int index = pq.size() - 1;
            while (!pq.isEmpty()) {
                sortedPairs[index--] = pq.poll();
            }
            
            for (WordCountPair pair : sortedPairs) {
                context.write(new Text(pair.word), new IntWritable(pair.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Top50Words <input path> <job1 output path> <final output path> <stopwords file path>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String job1OutputPath = args[1];
        String finalOutputPath = args[2];
        String stopWordsPath = args[3];

        Configuration conf1 = new Configuration();
        conf1.setBoolean("wordcount.skip.patterns", true);
        Job job1 = Job.getInstance(conf1, "Word Count 1");
        
        job1.setJarByClass(Top50Words.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        // Use SequenceFileOutputFormat for intermediate data to save space and I/O
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.addCacheFile(new Path(stopWordsPath).toUri());

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        SequenceFileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.err.println("Job 1 Failed!");
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 50 Selection");
        job2.setJarByClass(Top50Words.class);
        
        // Since input is SequenceFile, configure it accordingly
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        job2.setMapperClass(TopKMapper.class);
        job2.setReducerClass(TopKReducer.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Ensure a single reducer gets all mapping local-top-50 outputs
        job2.setNumReduceTasks(1);

        SequenceFileInputFormat.addInputPath(job2, new Path(job1OutputPath));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));

        boolean job2Success = job2.waitForCompletion(true);
        System.exit(job2Success ? 0 : 1);
    }
}
