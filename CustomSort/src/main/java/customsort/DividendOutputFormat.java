package customsort;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DividendOutputFormat extends FileOutputFormat<NullWritable, DividendChange> {

	public static class DividendRecordWriter extends RecordWriter<NullWritable, DividendChange> {

		public final String SEPARATOR = ",";
		private DataOutputStream out;
		
		public DividendRecordWriter(DataOutputStream out) {
			this.out = out;
		}
		
		@Override
		public void write(NullWritable key, DividendChange value)
				throws IOException, InterruptedException {
			StringBuilder result = new StringBuilder();		
			result.append(value.getSymbol()).append(SEPARATOR).append(value.getDate()).append(SEPARATOR).append(value.getChange());
			result.append("\n");
			out.write(result.toString().getBytes());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			out.close();
		}
		
	}

	@Override
	public RecordWriter<NullWritable, DividendChange> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		int partition = context.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(context);
		Path file = new Path(outputDir.getName() + Path.SEPARATOR
		+ context.getJobName()
		+ "_" + partition); FileSystem fs =
		file.getFileSystem(context.getConfiguration()); 
		FSDataOutputStream fileOut = fs.create(file);
		return new DividendRecordWriter(fileOut);
	}


}
