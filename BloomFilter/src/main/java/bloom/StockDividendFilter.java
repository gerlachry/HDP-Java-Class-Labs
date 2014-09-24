package bloom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class StockDividendFilter extends Configured implements Tool {
	private static final String FILTER_FILE = "hdfs://sandbox:8020/user/train/filters/dividendfilter";

	public static class BloomMapper extends Mapper<LongWritable, Text, NullWritable, BloomFilter> {
		private String stockSymbol;
		private NullWritable outputKey = NullWritable.get();

		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString(),'\\',',');
			if(words[1].equals(stockSymbol)) {
				Stock stock = new Stock(words[1], words[2]);
				Key stockKey = new Key(stock.toString().getBytes());
				outputValue.add(stockKey);
			}
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

		}	
	}
	
	public static class BloomReducer extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable> {
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

		}

		@Override
		protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context)
				throws IOException, InterruptedException {			

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}
	}
	
	
	public static class StockFilterMapper extends Mapper<LongWritable, Text, Stock, DoubleWritable> {
		private BloomFilter dividends;
		private String stockSymbol;
		private DoubleWritable outputValue = new DoubleWritable();
		Stock outputKey = new Stock();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path filter_file = new Path(FILTER_FILE);
			stockSymbol = context.getConfiguration().get("stockSymbol");

			//Initialize the dividends field

		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString(),'\\',',');
			if(words[1].equals(stockSymbol)) {
				outputKey.setSymbol(words[1]);
				outputKey.setDate(words[2]);
				//Instantiate a Bloom Key using outputKey, then check for membership in the Bloom filter

			}
		}
	}

	public static class StockFilterReducer extends Reducer<Stock, DoubleWritable, Text, DoubleWritable> {
		private Text outputKey = new Text();
		private String stockSymbol = "";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");
		}

		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			for(DoubleWritable value : values) {
				//Check for false positives before outputting a record
				if(key.getSymbol().equals(stockSymbol)) {
					outputKey.set(key.toString());
					context.write(outputKey, value);
				}
			}
		}

	}


	@Override
	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "CreateBloomFilter");
		job1.setJarByClass(getClass());
		Configuration conf = job1.getConfiguration();
		conf.set("stockSymbol", args[0]);

		FileInputFormat.setInputPaths(job1, new Path("dividends"));
		
		job1.setMapperClass(BloomMapper.class);
		job1.setReducerClass(BloomReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(NullOutputFormat.class);
		job1.setMapOutputKeyClass(NullWritable.class);
		job1.setMapOutputValueClass(BloomFilter.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setNumReduceTasks(1);
		
		boolean job1success = job1.waitForCompletion(true);
		if(!job1success) {
			System.out.println("The CreateBloomFilter job failed!");
			return -1;
		}

		Job job2 = Job.getInstance(conf, "FilterStocksJob");
		job2.setJarByClass(getClass());
		conf = job2.getConfiguration();

		Path out = new Path("bloomoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job2, new Path("stocks"));
		FileOutputFormat.setOutputPath(job2, out);
		
		job2.setMapperClass(StockFilterMapper.class);
		job2.setReducerClass(StockFilterReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(Stock.class);
		job2.setMapOutputValueClass(DoubleWritable.class);	
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		boolean job2success = job2.waitForCompletion(true);
		if(!job2success) {
			System.out.println("The FilterStocksJob failed!");
			return -1;
		}		
		return 1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new StockDividendFilter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
