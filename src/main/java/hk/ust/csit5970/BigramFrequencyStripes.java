package hk.ust.csit5970;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Compute the bigram count using the "stripes" approach
 */
public class BigramFrequencyStripes extends Configured implements Tool {
	private static final Logger LOG = Logger
			.getLogger(BigramFrequencyStripes.class);

	/*
	 * Mapper: emits <word, stripe> where stripe is a hash map
	 */
	private static class MyMapper extends
			Mapper<LongWritable, Text, Text, HashMapStringIntWritable> {

		// Reuse objects to save overhead of object creation.
		private static final Text KEY = new Text();
		private static final HashMapStringIntWritable STRIPE = new HashMapStringIntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = ((Text) value).toString();
			String[] words = line.trim().split("\\s+");

			/*
			 * TODO: Your implementation goes here.
			 */
			java.util.HashMap<String, HashMapStringIntWritable> localStripes = new java.util.HashMap<String, HashMapStringIntWritable>();
			for (int i = 0; i < words.length - 1; i++) {
				String left = words[i];
				String right = words[i + 1];
				if (left.length() == 0 || right.length() == 0) continue;
				if (!localStripes.containsKey(left)) {
					localStripes.put(left, new HashMapStringIntWritable());
				}
				localStripes.get(left).increment(right, 1);
			}
			for (java.util.Map.Entry<String, HashMapStringIntWritable> entry : localStripes.entrySet()) {
				KEY.set(entry.getKey());
				context.write(KEY, entry.getValue());
			}
		}
	}

	/*
	 * TODO: write your reducer to aggregate all stripes associated with each key
	 */
	private static class MyReducer extends
			Reducer<Text, HashMapStringIntWritable, PairOfStrings, FloatWritable> {

		// Reuse objects.
		private final static HashMapStringIntWritable SUM_STRIPES = new HashMapStringIntWritable();
		private final static PairOfStrings BIGRAM = new PairOfStrings();
		private final static FloatWritable FREQ = new FloatWritable();

		@Override
		public void reduce(Text key,
				Iterable<HashMapStringIntWritable> stripes, Context context)
				throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			SUM_STRIPES.clear();
			for (HashMapStringIntWritable stripe : stripes) {
				for (Map.Entry<String, IntWritable> entry : stripe.getMap().entrySet()) {
					SUM_STRIPES.increment(entry.getKey(), entry.getValue().get());
				}
			}
			int total = 0;
			for (IntWritable count : SUM_STRIPES.getMap().values()) {
				total += count.get();
			}
			// 输出总数记录，格式为 (word, "") -> total
			BIGRAM.set(key.toString(), "");
			FREQ.set((float) total);
			context.write(BIGRAM, FREQ);
			// 输出每个 bigram 的相对频率记录
			for (Map.Entry<String, IntWritable> entry : SUM_STRIPES.getMap().entrySet()) {
				String right = entry.getKey();
				int count = entry.getValue().get();
				float frequency = (float) count / total;
				BIGRAM.set(key.toString(), right);
				FREQ.set(frequency);
				context.write(BIGRAM, FREQ);
			}
		}
	}

	/*
	 * TODO: Write your combiner to aggregate all stripes with the same key
	 */
	private static class MyCombiner
			extends
			Reducer<Text, HashMapStringIntWritable, Text, HashMapStringIntWritable> {
		// Reuse objects.
		private final static HashMapStringIntWritable SUM_STRIPES = new HashMapStringIntWritable();

		@Override
		public void reduce(Text key,
				Iterable<HashMapStringIntWritable> stripes, Context context)
				throws IOException, InterruptedException {
			/*
			 * TODO: Your implementation goes here.
			 */
			SUM_STRIPES.clear();
			for (HashMapStringIntWritable stripe : stripes) {
				for (Map.Entry<String, IntWritable> entry : stripe.getMap().entrySet()) {
					SUM_STRIPES.increment(entry.getKey(), entry.getValue().get());
				}
			}
			// 为避免对象复用问题，新建 aggregated stripe 传出
			HashMapStringIntWritable aggregatedStripe = new HashMapStringIntWritable();
			for (Map.Entry<String, IntWritable> entry : SUM_STRIPES.getMap().entrySet()) {
				aggregatedStripe.increment(entry.getKey(), entry.getValue().get());
			}
			context.write(key, aggregatedStripe);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public BigramFrequencyStripes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String outputPath = cmdline.getOptionValue(OUTPUT);
		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + BigramFrequencyStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Create and configure a MapReduce job
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName(BigramFrequencyStripes.class.getSimpleName());
		job.setJarByClass(BigramFrequencyStripes.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HashMapStringIntWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(FloatWritable.class);

		/*
		 * A MapReduce program consists of four components: a mapper, a reducer,
		 * an optional combiner, and an optional partitioner.
		 */
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BigramFrequencyStripes(), args);
	}
}
