package hadoop_projects.project_one;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EntityAnalysisMRJob extends Configured implements Tool {

	private static final String projectRootPath = System.getProperty("user.dir");
	public static final boolean runOnCluster = true;

	private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";
	private static final String END_CLUSTER_FLAG = "END_CLUSTER_FLAG";

	private static final String rawData = "ComercialBanks10k.csv";
	private static final String mappedData = "output";
	private static final String mappedDataForAnalysis = "/output";

	private static Path outputFile;
	private static Path inputFile;
	private static Path mappedDataPath;
	private static Configuration conf;

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new EntityAnalysisMRJob(), args);
		System.out.println("res: " + res);
		System.out.println("root dir: " + projectRootPath);
		if (res == 0 & runOnCluster) {
			runMigrate();
		}
		System.exit(res);
	}

	public int run(String[] arg0) throws Exception {
		conf = getConf();

		outputFile = new Path(projectRootPath, mappedData);
		inputFile = new Path(projectRootPath, rawData);

		Job job = Job.getInstance(conf);

		job.setJarByClass(EntityAnalysisMRJob.class);
		job.setMapperClass(EntityMapper.class);
		job.setReducerClass(EntityReducerClusterSeeds.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		return (job.waitForCompletion(true) ? 0 : 1);

	}

	public static int runMigrate() throws Exception {

		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path(projectRootPath);
		mappedDataPath = new Path(tmpPath.toString() + mappedDataForAnalysis);

		System.out.println(String.format(" mapped data path %s", mappedDataPath.toString()));
		fs.copyToLocalFile(false, inputFile, mappedDataPath);
		return 0;

	}

	public static class EntityMapper extends Mapper<Object, Text, Text, Text> {

		private final static Text valueLine = new Text();
		private Text keyWord = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] line = value.toString().split("\\t");
			if (line[0] == "ID") {
				return;
			}

			valueLine.set(value);
			keyWord.set(line[1]);

			context.write(keyWord, valueLine);
		}

	}

	public static class EntityReducer extends Reducer<Text, Text, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int total = 0;
			for (Text val : values) {
				total++;
			}
			if (total > 1) {
				StringBuilder logger = new StringBuilder();
				logger.append("Found duplicate values for ").append(key).append("\n");
				for (Text val : values) {
					logger.append(val).append("\n");

				}
				context.write(new Text(logger.toString()), new IntWritable(total));
			} else {
				context.write(key, new IntWritable(total));
			}

		}

	}

	public static class EntityReducerClusterSeeds extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int total = 0;
			StringBuilder logger = new StringBuilder();
			logger.append(START_CLUSTER_FLAG).append("\n");
			for (Text val : values) {
				total++;
				logger.append(key).append("\t").append(val).append("\n");
			}
			if (total > 1) {
				context.write(new Text(logger.toString()), new Text(END_CLUSTER_FLAG));
			}
		}

	}
}
