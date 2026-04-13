package parta;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import opennlp.tools.stemmer.PorterStemmer;

/**
 * Problem 2a: Document Frequency (DF) computation over Wikipedia articles.
 *
 * For each distinct stemmed term (excluding stopwords), counts the number of
 * distinct documents in which it appears. Outputs a TSV file: TERM<tab>DF
 *
 * Usage:
 *   hadoop jar problem2.jar parta.DocumentFrequency \
 *     <input-dir> <output-dir> <hdfs-path-to-stopwords.txt>
 */
public class DocumentFrequency extends Configured implements Tool {

    // -------------------------------------------------------------------------
    // Mapper: emits (stemmed_term, doc_id) for each unique term per line
    // -------------------------------------------------------------------------
    public static class DFMapper extends Mapper<Object, Text, Text, Text> {

        private final Set<String> stopwords = new HashSet<>();
        private final PorterStemmer stemmer = new PorterStemmer();
        private final Text outTerm = new Text();
        private final Text outDocId = new Text();

        /**
         * Runs once per mapper task before any map() calls.
         * Loads stopwords from the distributed cache (added via job.addCacheFile()).
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) return;

            for (URI uri : cacheFiles) {
                Path filePath = new Path(uri.getPath());
                String fileName = filePath.getName();
                try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String w = line.trim().toLowerCase();
                        if (!w.isEmpty()) stopwords.add(w);
                    }
                }
            }
        }

        /**
         * Called once per input line.
         *
         * - Derives the document ID from the input filename (e.g. "107301.txt" → "107301").
         * - Tokenises, lowercases, and stems each token.
         * - Skips stopwords (both before and after stemming).
         * - Emits (stemmed_term, doc_id) at most once per unique term per line to
         *   reduce intermediate shuffle volume.
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Document ID = filename without extension
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String docId = filename.replaceAll("\\.[^.]+$", "");
            outDocId.set(docId);

            String line = value.toString().toLowerCase();
            // Split on anything that is not a letter
            String[] tokens = line.split("[^a-z]+");

            // Deduplicate within this line: a term seen twice in one line
            // should only contribute one emit toward DF for this document.
            Set<String> seenThisLine = new HashSet<>();

            for (String token : tokens) {
                if (token.length() < 2) continue;           // skip single chars / empty
                if (stopwords.contains(token)) continue;    // skip raw stopword

                String stemmed = stemmer.stem(token);
                if (stemmed.length() < 2) continue;         // skip degenerate stems
                if (stopwords.contains(stemmed)) continue;  // skip stemmed stopword

                if (seenThisLine.add(stemmed)) {            // emit only once per line
                    outTerm.set(stemmed);
                    context.write(outTerm, outDocId);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Reducer: counts distinct document IDs for each term  →  DF value
    // -------------------------------------------------------------------------
    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        /**
         * Receives all (doc_id) values for a single term.
         * Adds them to a HashSet to deduplicate across mapper splits,
         * then writes (term, distinct_doc_count).
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> distinctDocs = new HashSet<>();
            for (Text val : values) {
                distinctDocs.add(val.toString());
            }
            result.set(distinctDocs.size());
            context.write(key, result);
        }
    }

    // -------------------------------------------------------------------------
    // Tool interface entry point (handles -libjars, -D, -files etc.)
    // -------------------------------------------------------------------------
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: hadoop jar problem2.jar parta.DocumentFrequency <input> <output> <hdfs-stopwords-path>");
            return 1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "document_frequency");
        job.setJarByClass(DocumentFrequency.class);

        // Add stopwords file to distributed cache (localised to each task node)
        job.addCacheFile(new Path(args[2]).toUri());

        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        // Mapper output types differ from reducer output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer (final) output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    // -------------------------------------------------------------------------
    // Main: delegates to ToolRunner so generic Hadoop options are parsed first
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new DocumentFrequency(), args));
    }
}
