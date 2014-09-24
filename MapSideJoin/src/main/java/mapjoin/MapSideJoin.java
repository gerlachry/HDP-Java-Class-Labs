package mapjoin;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {

	public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Stock, StockPrices> {
		private String stockSymbol;
		private HashMap<Stock, Double> stocks = new HashMap<Stock, Double>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// current stock symbol to join on
			stockSymbol = context.getConfiguration().get("stockSymbol");
			// load the file into memory to be used in the join
			Configuration conf = context.getConfiguration();
			Path [] files = context.getLocalCacheFiles();
			for(Path file : files) { 
				if(file.getName().equals("NYSE_dividends_A.txt")) {
					// based on data looking like:
					//  NYSE,AIT,2009-11-12,0.15
					FileSystem fs = FileSystem.getLocal(conf); 
					FSDataInputStream is = fs.open(file);
					BufferedReader in = new BufferedReader(new InputStreamReader(is));
					String currentLine = "";
					while((currentLine = in.readLine()) != null){
						String [] fields = StringUtils.split(currentLine,",");
						if(fields[1].equals(stockSymbol)){
							// add stock to hashmap
							Stock stock = new Stock(fields[1], fields[2]);
							Double dividend = Double.parseDouble(fields[3]);
							stocks.put(stock, dividend);
						}
					}
					in.close();
				} 
			}

			
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// data looking like: 
			// NYSE,AIT,2010-02-08,21.81,21.81,21.28,21.31,103800,21.31
			String [] fields = StringUtils.split(value.toString(),",");
			// only process line if is our current stockSymbol
			if(fields[1].equals(stockSymbol)){
				Stock stock = new Stock(fields[1],fields[2]);
				if(stocks.containsKey(stock)){
					StockPrices stockPrices = new StockPrices(stocks.get(stock), Double.parseDouble(fields[6]));
					context.write(stock, stockPrices);
				}
			}
		}
		
	}


	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MapSideJoinJob");
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		conf.set("stockSymbol", args[0]);
		// updates TextOutputFormat default tab delimited to comma
		conf.set(TextOutputFormat.SEPERATOR, ",");
		Path out = new Path("joinoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("stocks"));
		FileOutputFormat.setOutputPath(job, out);
		
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(StockPrices.class);
		// add dividends file as local resource to use in mapper
		job.addCacheFile(new URI("dividends/NYSE_dividends_A.csv"));
		
		job.setNumReduceTasks(0);
		

		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new MapSideJoin(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
