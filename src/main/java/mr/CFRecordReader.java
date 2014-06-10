package mr;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

public class CFRecordReader extends RecordReader<BytesWritable, Text> {

	private final SequenceFileRecordReader<BytesWritable, Text> lineRecordReader;

	public CFRecordReader(CombineFileSplit split, TaskAttemptContext context,
			Integer index) throws IOException, InterruptedException {
		// byte[] bytes = { new Byte("\n") };

		CombineFileSplit cSplit = (CombineFileSplit) split;
		FileSplit fileSplit = new FileSplit(cSplit.getPath(index),
				cSplit.getOffset(index), cSplit.getLength(index),
				cSplit.getLocations());
		lineRecordReader = new SequenceFileRecordReader<BytesWritable, Text>();
		lineRecordReader.initialize(fileSplit, context);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// Won't be called, use custom Constructor
		// `CFRecordReader(CombineFileSplit split, TaskAttemptContext context,
		// Integer index)`
		// instead
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}

	@Override
	public float getProgress() throws IOException {
		return lineRecordReader.getProgress();
	}

	@Override
	public BytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		return (BytesWritable) lineRecordReader.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return (Text) lineRecordReader.getCurrentValue();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lineRecordReader.nextKeyValue();
	}
}
