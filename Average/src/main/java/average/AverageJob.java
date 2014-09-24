package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageJob extends Configured implements Tool {

	public enum Counters {MAP, COMBINE, REDUCE}
	
	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// records are by county
			String [] words = value.toString().split(",");
			outputKey.set(words[1].trim()); // get the state for the key
			outputValue.set(words[9].trim()+",1"); // the median income for year 2000 for this county
			context.write(outputKey, outputValue);
			context.getCounter(Counters.MAP).increment(1);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

		}


	}
	
	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// a combiner to partially calculate average, need to keep a count
			long sum = 0;
			int count = 0;
			for (Text val : values){
				String [] s = val.toString().split(",");
				sum += Long.parseLong(s[0]);
				count += Integer.parseInt(s[1]);
			}
			// key and value out need to be same type as coming in
			outputValue.set(sum + "," + count);
			context.write(key, outputValue);
			context.getCounter(Counters.COMBINE).increment(1);
		}		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

		}
	}

	public static class AveragePartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// states startign with same letter go to same reducer, states are the key
			if(numPartitions == 1){
				return 0;
			}
			String letter = key.toString().substring(0,1);
			return letter.hashCode() % numPartitions;

		}
		
	}
	
	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		
		private DoubleWritable average = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			double sum = 0;
			int count = 0;
			for (Text pair : values){
				String [] s = pair.toString().split(",");
				sum += Double.parseDouble(s[0]);
				count += Integer.parseInt(s[1]);
			}
			average.set(sum/count);
			context.write(key, average);
			context.getCounter(Counters.REDUCE).increment(1);
			
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "AverageJob");
		job.setJarByClass(AverageJob.class);

		Path out = new Path("outputNoPart");
		FileInputFormat.setInputPaths(job, "counties");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);

		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageCombiner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setPartitionerClass(AveragePartitioner.class);
		job.setNumReduceTasks(5);
		
		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new AverageJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
