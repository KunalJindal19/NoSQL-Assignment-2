package com.nosql.assignment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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

public class CoOccurrenceStripesInMapper {

    public static class StripesInMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Set<String> frequentWords = new HashSet<String>();
        private int distance;
        
        // --- 1(e) REQUIRED ADDITION: GLOBAL MAP-FUNCTION LAYER ---
        // A single map storing everything across all lines in this mapper snippet
        private Map<String, MapWritable> globalStripeMap;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("cooccurrence.distance", 1);
            globalStripeMap = new HashMap<String, MapWritable>();
            
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
                
                // Fetch the existing massive stripe map or create a fresh one for this word
                MapWritable stripe = globalStripeMap.containsKey(u) ? globalStripeMap.get(u) : new MapWritable();
                boolean stripeUpdated = false;
                
                int start = Math.max(0, i - distance);
                int end = Math.min(tokens.length - 1, i + distance);
                
                for (int j = start; j <= end; j++) {
                    if (i == j) continue;
                    String v = tokens[j];
                    if (v.isEmpty() || !frequentWords.contains(v) || u.equals(v)) continue;
                    
                    Text vText = new Text(v);
                    if (stripe.containsKey(vText)) {
                        IntWritable count = (IntWritable) stripe.get(vText);
                        stripe.put(vText, new IntWritable(count.get() + 1));
                    } else {
                        stripe.put(vText, new IntWritable(1));
                    }
                    stripeUpdated = true;
                }
                
                // Save it back locally. NO context.write().
                if (stripeUpdated) {
                    globalStripeMap.put(u, stripe);
                }
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emits the globally bundled stripes exclusively on Mapper destruct
            Text wordText = new Text();
            for (Map.Entry<String, MapWritable> entry : globalStripeMap.entrySet()) {
                wordText.set(entry.getKey());
                context.write(wordText, entry.getValue());
            }
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
        
        Job job = Job.getInstance(conf, "Co-Occur Stripes In-Mapper d=" + distance);
        job.setJarByClass(CoOccurrenceStripesInMapper.class);
        
        job.setMapperClass(StripesInMapper.class);
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
