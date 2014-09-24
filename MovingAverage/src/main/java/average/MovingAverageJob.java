package average;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovingAverageJob extends Configured implements Tool {
	public static class MovingAverageReducer extends Reducer<Stock, DoubleWritable, Text, DoubleWritable> {
		private Text outputKey = new Text();
		private DoubleWritable outputValue = new DoubleWritable();
		private static final int WINDOW_SIZE = 50;
		private LinkedList<StockWindow> windows = new LinkedList<StockWindow>();
		
		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context) throws IOException,
		InterruptedException {
			for (DoubleWritable value : values){
				StockWindow window = new StockWindow(key.toString());
				windows.add(window);
				for (StockWindow w : windows){
					w.addPrice(value.get(), key.getSymbol());
				}
				if(windows.size() >= WINDOW_SIZE){
					StockWindow w = windows.removeFirst();
					outputKey.set(w.toString());
					outputValue.set(w.getAverage());
					context.write(outputKey, outputValue);
				}
			}
			windows.clear();

		}
	}


	public static class StockPartitioner extends Partitioner<Stock, DoubleWritable> {
		@Override
		public int getPartition(Stock key, DoubleWritable value, int numPartitions) {
			int partition = (key.getSymbol().charAt(0) - 'A') % numPartitions;
			return partition;
		}
	}

	public static class StockGroupComparator extends WritableComparator {
		protected StockGroupComparator() {
			super(Stock.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Stock lhs = (Stock) a;
			Stock rhs = (Stock) b;

			return lhs.getSymbol().compareTo(rhs.getSymbol());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MovingAverageJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(MovingAverageJob.class);

		Path out = new Path("movingaverages");
		FileInputFormat.setInputPaths(job, "closingprices");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(MovingAverageReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setPartitionerClass(StockPartitioner.class);
		job.setGroupingComparatorClass(StockGroupComparator.class);

		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), new MovingAverageJob(), args);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
