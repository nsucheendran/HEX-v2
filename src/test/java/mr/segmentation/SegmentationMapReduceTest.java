/*
 * @author achadha
 */

package mr.segmentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import mockit.Mock;
import mockit.MockUp;
import mr.Constants;
import mr.dto.TextMultiple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class SegmentationMapReduceTest {
	private static final BytesWritable bw = new BytesWritable(new byte[0], 0);
	private MapDriver<BytesWritable, Text, TextMultiple, TextMultiple> mapDriver;
	private ReduceDriver<TextMultiple, TextMultiple, TextMultiple, TextMultiple> combineDriver;
	private ReduceDriver<TextMultiple, TextMultiple, Text, NullWritable> reduceDriver;
	private MapReduceDriver<BytesWritable, Text, TextMultiple, TextMultiple, Text, NullWritable> mapReduceDriver;
	private SegmentationJobConfigurator jobConfigurator;
	private static final String segFileContent = "1\ttest1\tlf1,lf2\n2\ttest2\tlf1,lf3";

	@Before
	public void setup() throws IOException {
		SegmentationMapper mapper = new SegmentationMapper();
		SegmentationReducer reducer = new SegmentationReducer();
		SegmentationCombiner combiner = new SegmentationCombiner();

		mapDriver = new MapDriver<BytesWritable, Text, TextMultiple, TextMultiple>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<TextMultiple, TextMultiple, Text, NullWritable>();
		reduceDriver.setReducer(reducer);
		combineDriver = new ReduceDriver<TextMultiple, TextMultiple, TextMultiple, TextMultiple>();
		combineDriver.setReducer(combiner);

		mapReduceDriver = new MapReduceDriver<BytesWritable, Text, TextMultiple, TextMultiple, Text, NullWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		jobConfigurator = new SegmentationJobConfigurator();
		new MockUp<UUID>() {
			@Mock
			public String toString() {
				return "mockuuid";
			}
		};
	}

	@Test
	public void mapper() throws IOException {
		configureJob();

		// rf1\trf2\trf3\trf4

		Text text = new Text();

		StringBuilder sb = new StringBuilder();
		mapDriver.withInput(bw,
				text(text, sb, Constants.COL_DELIM, "id1", "val", "val1", "7"));
		sb.setLength(0);
		mapDriver.withInput(bw,
				text(text, sb, Constants.COL_DELIM, "id1", "val", "val4", "8"));
		sb.setLength(0);
		mapDriver.withInput(bw,
				text(text, sb, Constants.COL_DELIM, "id2", "val", "val1", "9"));

		mapDriver.withOutput(new TextMultiple("1", "test1", "id1", "val",
				"Unknown"), new TextMultiple("7"));
		mapDriver.withOutput(new TextMultiple("2", "test2", "id1", "Unknown",
				"val1"), new TextMultiple("7"));
		mapDriver.withOutput(new TextMultiple("1", "test1", "id1", "val",
				"Unknown"), new TextMultiple("8"));
		mapDriver.withOutput(new TextMultiple("2", "test2", "id1", "Unknown",
				"val4"), new TextMultiple("8"));
		mapDriver.withOutput(new TextMultiple("1", "test1", "id2", "val",
				"Unknown"), new TextMultiple("9"));
		mapDriver.withOutput(new TextMultiple("2", "test2", "id2", "Unknown",
				"val1"), new TextMultiple("9"));

		mapDriver.runTest();
	}

	@SuppressWarnings("deprecation")
	private Job configureJob() throws IOException {
		jobConfigurator.numReduceTasks(100)
				.groupKeys(new HashMap<String, String>() {
					private static final long serialVersionUID = 1L;

					{
						put("lf1", "Unknown");
						put("lf2", "Unknown");
						put("lf3", "Unknown");
					}
				}).metrics(new HashSet<String>() {
					private static final long serialVersionUID = 1L;

					{
						add("lf4");
					}
				});

		jobConfigurator.colMap(Arrays.asList("lf1", "lf2", "lf3", "lf4"),
				Arrays.asList("lf1", "lf2", "lf3", "lf4"), new BufferedReader(
						new StringReader(segFileContent)));

		Job job = jobConfigurator.initJob(mapDriver.getConfiguration(),
				"mapTest", "edwdev");
		jobConfigurator.configureJob(job);
		mapDriver.setConfiguration(job.getConfiguration());
		return job;
	}

	@Test
	@SuppressWarnings("deprecation")
	public void reducer() throws IOException {

		Job job = configureJob();

		Path outPath = new Path(new File(".").getAbsolutePath()
				+ "/target/output");
		FileSystem fs = outPath.getFileSystem(job.getConfiguration());
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		reduceDriver.setConfiguration(job.getConfiguration());
		Text text = new Text();
		StringBuilder sb = new StringBuilder();
		reduceDriver.withInput(
				new TextMultiple("key1", "key2", "key3", "key4", "key5"),
				Arrays.asList(new TextMultiple("1", "240", "2", "242", "2",
						"30", "23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "240", "2", "242", "2", "30",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("-1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("-1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("1", "140", "3", "142", "3", "20",
								"23", "23", "23", "23", "23", "23", "23"),
						new TextMultiple("-1", "-140", "3", "-142", "3", "-20",
								"23", "23", "23", "23", "23", "23", "23")))
				.withOutput(
						text(text, sb, "\t", "mockuuid", "key1", "key2", "4",
								"1320", "28", "1336", "28", "180", "230",
								"230.0", "230", "230.0", "230", "230.0", "230",
								"key3", "key4", "key5"), NullWritable.get());
		reduceDriver.runTest();

	}

	@Test
	@SuppressWarnings("deprecation")
	public void combiner() throws IOException {

		Job job = configureJob();

		Path outPath = new Path(new File(".").getAbsolutePath()
				+ "/target/output");
		FileSystem fs = outPath.getFileSystem(job.getConfiguration());
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		combineDriver.setConfiguration(job.getConfiguration());
		combineDriver
				.withInput(
						new TextMultiple("key1", "key2", "key3", "key4", "key5"),
						Arrays.asList(new TextMultiple("1", "240", "2", "242",
								"2", "30", "23", "23", "23", "23", "23", "23",
								"23"), new TextMultiple("1", "240", "2", "242",
								"2", "30", "23", "23", "23", "23", "23", "23",
								"23"), new TextMultiple("1", "140", "3", "142",
								"3", "20", "23", "23", "23", "23", "23", "23",
								"23"), new TextMultiple("1", "140", "3", "142",
								"3", "20", "23", "23", "23", "23", "23", "23",
								"23"), new TextMultiple("-1", "140", "3",
								"142", "3", "20", "23", "23", "23", "23", "23",
								"23", "23"), new TextMultiple("1", "140", "3",
								"142", "3", "20", "23", "23", "23", "23", "23",
								"23", "23"), new TextMultiple("1", "140", "3",
								"142", "3", "20", "23", "23", "23", "23", "23",
								"23", "23"), new TextMultiple("-1", "140", "3",
								"142", "3", "20", "23", "23", "23", "23", "23",
								"23", "23"), new TextMultiple("1", "140", "3",
								"142", "3", "20", "23", "23", "23", "23", "23",
								"23", "23"), new TextMultiple("-1", "-140",
								"3", "-142", "3", "-20", "23", "23", "23",
								"23", "23", "23", "23")))
				.withOutput(
						new TextMultiple("key1", "key2", "key3", "key4", "key5"),
						new TextMultiple("4", "1320", "28", "1336", "28",
								"180", "230", "230.0", "230", "230.0", "230",
								"230.0", "230"));
		combineDriver.runTest();

	}

	private Text text(Text text, StringBuilder sb, String sep, String... vals) {

		for (String val : vals) {
			sb.append(val).append(sep);
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - sep.length());
		}
		text.set(sb.toString());
		return text;
	}
}
