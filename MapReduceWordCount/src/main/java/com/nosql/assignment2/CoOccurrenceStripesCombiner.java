package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class CoOccurrenceStripesCombiner {

    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Set<String> frequentWords = new HashSet<String>();
        private int distance;
        private Text wordText = new Text();

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
                MapWritable stripe = new MapWritable();
                int start = Math.max(0, i - distance);
                int end = Math.min(tokens.length - 1, i + distance);
                
                for (int j = start; j <= end; j++) {
                    if (i == j) continue;
                    String v = tokens[j];
                    if (v.isEmpty() || !frequentWords.contains(v) || u.equals(v)) continue;
                    Text vText = new Text(v);
                    if (stripe.containsKey(vText)) {
                        IntWritable count = (IntWritable) stripe.get(vText);
                        count.set(count.get() + 1);
                    } else stripe.put(vText, new IntWritable(1));
                }
                if (!stripe.isEmpty()) {
                    wordText.set(u);
                    context.write(wordText, stripe);
                }
            }
        }
    }

    // --- 1(e) REQUIRED ADDITION: LOCAL MAP-CLASS COMBINER ---
    // Specifically combines map writables locally before shuffle
    public static class StripesCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable localMergedStripe = new MapWritable();
            for (MapWritable stripe : values) {
                for (Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text neighbor = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    if (localMergedStripe.containsKey(neighbor)) {
                        IntWritable finalCount = (IntWritable) localMergedStripe.get(neighbor);
                        finalCount.set(finalCount.get() + count.get());
                    } else localMergedStripe.put(new Text(neighbor), new IntWritable(count.get()));
                }
            }
            context.write(key, localMergedStripe);
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable finalStripe = new MapWritable();
            for (MapWritable stripe : values) {
                for (Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text neighbor = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    if (finalStripe.containsKey(neighbor)) {
                        IntWritable finalCount = (IntWritable) finalStripe.get(neighbor);
                        finalCount.set(finalCount.get() + count.get());
                    } else finalStripe.put(new Text(neighbor), new IntWritable(count.get()));
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{ ");
            boolean first = true;
            for (Entry<Writable, Writable> entry : finalStripe.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(((Text)entry.getKey()).toString()).append(":").append(((IntWritable)entry.getValue()).get());
                first = false;
            }
            sb.append(" }");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) System.exit(-1);
        int distance = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        conf.setInt("cooccurrence.distance", distance);
        
        Job job = Job.getInstance(conf, "Co-Occur Stripes Combiner d=" + distance);
        job.setJarByClass(CoOccurrenceStripesCombiner.class);
        job.setMapperClass(StripesMapper.class);
        
        // Register the local Combiner
        job.setCombinerClass(StripesCombiner.class);
        
        job.setReducerClass(StripesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime) + " ms");
        System.exit(success ? 0 : 1);
    }
}
