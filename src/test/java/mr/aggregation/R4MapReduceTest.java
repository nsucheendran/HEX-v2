package mr.aggregation;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import mr.Constants;
import mr.JobConfigurator;
import mr.dto.TextMultiple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

public class R4MapReduceTest {

    private MapDriver<BytesWritable, Text, TextMultiple, TextMultiple> mapDriver;
    private ReduceDriver<TextMultiple, TextMultiple, NullWritable, TextMultiple> reduceDriver;
    private MapReduceDriver<BytesWritable, Text, TextMultiple, TextMultiple, NullWritable, TextMultiple> mapReduceDriver;
    private JobConfigurator jobConfigurator;

    @Before
    public void setup() throws IOException {
        R4Mapper mapper = new R4Mapper();
        R4Reducer reducer = new R4Reducer();
        mapDriver = new MapDriver<BytesWritable, Text, TextMultiple, TextMultiple>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<TextMultiple, TextMultiple, NullWritable, TextMultiple>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<BytesWritable, Text, TextMultiple, TextMultiple, NullWritable, TextMultiple>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        jobConfigurator = new JobConfigurator();
    }

    @Test
    public void mapperAllJoins() throws IOException {
        jobConfigurator.numReduceTasks(100);
        jobConfigurator.equiJoinKeys(new HashMap<String, String>() {
            {
                put("lf1", "rf1");
            }
        });
        jobConfigurator.lteJoinKeys(new HashMap<String, String>() {
            {
                put("lf3", "rf3");
            }
        });
        jobConfigurator.gteJoinKeys(new HashMap<String, String>() {
            {
                put("lf4", "rf4");
            }
        });

        jobConfigurator.groupKeys(new HashSet<String>() {
            {
                add("lf2");
            }
        });
        jobConfigurator.rhsKeys(new HashSet<String>() {
            {
                add("rf2");
            }
        });
        jobConfigurator.lhsFields(Arrays.asList("lf1", "lf2", "lf3", "lf4")).rhsFields(Arrays.asList("rf1", "rf2", "rf3", "rf4"));

        Job job = jobConfigurator.initJob(mapDriver.getConfiguration(), "mapTest", "edwdev");
        jobConfigurator.configureJob(job);
        StringBuilder data = new StringBuilder();
        // rf1\trf2\trf3\trf4
        jobConfigurator.stripe("id1" + Constants.COL_DELIM + "val2" + Constants.COL_DELIM + "val3" + Constants.COL_DELIM + "val4", data);
        job.getConfiguration().set("data", data.toString());
        mapDriver.setConfiguration(job.getConfiguration());

        BytesWritable ignored = new BytesWritable(new byte[0]);
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM
                + "val5"));
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM
                + "val3"));
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val4" + Constants.COL_DELIM
                + "val3"));
        mapDriver.withInput(ignored, new Text("id2" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1"));

        mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val1", "val5"));
        mapDriver.runTest();
    }

    @Test
    public void mapperEquiJoin() throws IOException {
        jobConfigurator.numReduceTasks(100);
        jobConfigurator.equiJoinKeys(new HashMap<String, String>() {
            {
                put("lf1", "rf1");
            }
        });
        jobConfigurator.groupKeys(new HashSet<String>() {
            {
                add("lf2");
            }
        });
        jobConfigurator.rhsKeys(new HashSet<String>() {
            {
                add("rf2");
            }
        });
        Map<String, String> emptyMap = new HashMap<String, String>(0);
        jobConfigurator.lteJoinKeys(emptyMap).gteJoinKeys(emptyMap);

        jobConfigurator.lhsFields(Arrays.asList("lf1", "lf2", "lf3", "lf4")).rhsFields(Arrays.asList("rf1", "rf2", "rf3", "rf4"));

        Job job = jobConfigurator.initJob(mapDriver.getConfiguration(), "mapTest", "edwdev");
        jobConfigurator.configureJob(job);
        StringBuilder data = new StringBuilder();
        // rf1\trf2\trf3\trf4
        jobConfigurator.stripe("id1" + Constants.COL_DELIM + "val2" + Constants.COL_DELIM + "val3" + Constants.COL_DELIM + "val4", data);
        job.getConfiguration().set("data", data.toString());
        mapDriver.setConfiguration(job.getConfiguration());

        BytesWritable ignored = new BytesWritable(new byte[0]);
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM
                + "val5"));
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM
                + "val3"));
        mapDriver.withInput(ignored, new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val4" + Constants.COL_DELIM
                + "val3"));
        mapDriver.withInput(ignored, new Text("id2" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1"));

        mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val1", "val5"));
        mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val1", "val3"));
        mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val4", "val3"));
        mapDriver.runTest();
    }

    @Test
    public void reducer() throws IOException {

        Job job = jobConfigurator.numReduceTasks(100).initJob(mapDriver.getConfiguration(), "reduceTest", "edwdev");
        jobConfigurator.rhsKeys(new HashSet<String>() {
            {
                add("rf1");
            }
        }).lhsFields(Arrays.asList("lf1")).rhsFields(Arrays.asList("rf1"));
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobConfigurator.configureJob(job);
        MultipleOutputs.setCountersEnabled(job, true);
        MultipleOutputs.addNamedOutput(job, "outroot", SequenceFileOutputFormat.class, BytesWritable.class, Text.class);
        Path outPath = new Path(new File(".").getAbsolutePath() + "/target/output");
        FileSystem fs = outPath.getFileSystem(job.getConfiguration());
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        // FileOutputFormat.setCompressOutput(job, true);
        // FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);
        reduceDriver.setConfiguration(job.getConfiguration());

        /*
         * guid => 0 itin_number => 1 trans_date => 2 num_transactions => 3 bkg_gbv => 4 bkg_room_nights => 5 omniture_gbv => 6
         * omniture_room_nights => 7 gross_profit => 8
         */
        reduceDriver.withInput(new TextMultiple("key1", "key2", "key3", "key4", "key5"), Arrays.asList(new TextMultiple("guid1", "itin1",
                "2013-01-01", "1", "240", "2", "242", "2", "30"), new TextMultiple("guid1", "itin1", "2013-01-02", "1", "240", "2", "242",
                "2", "30"), new TextMultiple("guid2", "itin2", "2013-01-02", "1", "140", "3", "142", "3", "20"), new TextMultiple("guid2",
                "itin2", "2013-01-03", "-1", "140", "3", "142", "3", "20"), new TextMultiple("guid2", "itin3", "2013-01-04", "-1", "140",
                "3", "142", "3", "20"), new TextMultiple("guid2", "itin4", "2013-01-04", "1", "140", "3", "142", "3", "20"),
                new TextMultiple("guid2", "itin5", "2013-01-05", "1", "140", "3", "142", "3", "20"), new TextMultiple("guid3", "itin6",
                        "2013-01-02", "-1", "140", "3", "142", "3", "20"), new TextMultiple("guid4", "itin7", "2013-01-02", "1", "140",
                        "3", "142", "3", "20"), new TextMultiple("guid4", "itin7", "2013-01-05", "-1", "-140", "3", "-142", "3", "-20")));

        reduceDriver.withInput(new TextMultiple("key1", "key2", "key3_1", "key4_1", "key5"),
                Arrays.asList(new TextMultiple("guid1", "itin81", "2013-01-01", "1", "240", "2", "242", "2", "30")));

        reduceDriver.runTest();
        validateReduceOutput(outPath.toString(), job);
    }

    private void validateReduceOutput(String outPath, Job job) throws IOException {
        Path path = new Path(outPath);
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
        while (files.hasNext()) {
            LocatedFileStatus lfs = files.next();
            Path filePath = lfs.getPath();
            
            SequenceFile.Reader repReader = new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(filePath));

            try {
                BytesWritable ignored = (BytesWritable) ReflectionUtils.newInstance(repReader.getKeyClass(), job.getConfiguration());

                Text value = (Text) ReflectionUtils.newInstance(repReader.getValueClass(), job.getConfiguration());

                while (repReader.next(ignored, value)) {
                    String[] vals = value.toString().split(Constants.COL_DELIM);
                    validatePath(filePath.toString(), outPath, vals);
                }
            } finally {
                IOUtils.closeStream(repReader);
            }
        }

    }

    private Map<Pattern, Set<String[]>> validPaths = new HashMap<Pattern, Set<String[]>>() {
        {
            put(Pattern.compile(Pattern.quote("experiment_code=key4/version_number=key5/variant_code=key3")), new HashSet<String[]>() {
                {
                    add(new String[] { "4", "3", "3", "2", "1", "4", "2", "1320.0", "28", "1336.0", "28", "180.0", "1", "key1", "key2" });
                }
            });
            put(Pattern.compile(Pattern.quote("experiment_code=key4_1/version_number=key5/variant_code=key3_1")), new HashSet<String[]>() {
                {
                    add(new String[] { "1", "1", "0", "1", "0", "0", "1", "240.0", "2", "242.0", "2", "30.0", "0", "key1", "key2" });
                }

            });
        }
    };

    private void validatePath(String path, String basePath, String[] vals) {
        for (Pattern p : validPaths.keySet()) {
            String[] parts = p.split(path);

            if (parts.length == 2) {
                Set<String[]> data = validPaths.get(p);
                for (String[] datum : data) {
                    if (Arrays.equals(datum, vals)) {
                        return;
                    }
                }
                fail("valid data not found at path: " + path);
            }
        }
        fail("invalid path");
    }
}

