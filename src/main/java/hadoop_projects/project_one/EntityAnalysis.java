package hadoop_projects.project_one;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class EntityAnalysis extends Configured implements Tool{

	private static final String projectRootPath = System.getProperty("user.dir");
	private static final String rawData="ComercialBanks10k.csv";
	private static final String mappedData="output"+System.currentTimeMillis();
	
	private static Path outputFile;
	private static Path inputFile;
	
	public static void main(String[] args) throws Exception{
		
		int res=ToolRunner.run(new Configuration(), new EntityAnalysis(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf=getConf();
		
		outputFile =new Path(projectRootPath,mappedData);
		inputFile=new Path(projectRootPath,rawData);
		
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(EntityAnalysis.class);
		job.setMapperClass(EntityMapper.class);
		job.setReducerClass(EntityReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,inputFile);
		FileOutputFormat.setOutputPath(job,outputFile);
		
		
		return (job.waitForCompletion(true)?0:1);
	}
	
	public static class EntityMapper extends Mapper<Object,Text,Text,Text>{

		private final static Text valueLine=new Text();
		private Text keyWord=new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] line=value.toString().split("\\t");
			if (line[0]=="ID"){
				return;
			}
			
			valueLine.set(value);
			keyWord.set(line[1]);
			
			context.write(keyWord,valueLine);
		}			
		
	}
	
	public static class EntityReducer extends Reducer<Text,Text,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int total=0;
			for (Text val:values){
				total++;
			}
			if(total>1){
				StringBuilder logger=new StringBuilder();
				logger.append("Found duplicate values for ").append(key).append("\n");
				for(Text val:values){
					logger.append(val).append("\n");
					
				}
				context.write(new Text(logger.toString()), new IntWritable(total));
			}
			else{
				context.write(key, new IntWritable(total));
			}			
			
		}		
		
	}
}

