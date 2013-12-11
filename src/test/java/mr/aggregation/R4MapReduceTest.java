package mr.aggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import mr.Constants;
import mr.JobConfigurator;
import mr.dto.TextMultiple;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
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
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM + "val5"));
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM + "val3"));
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val4" + Constants.COL_DELIM + "val3"));
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
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM + "val5"));
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1" + Constants.COL_DELIM + "val3"));
    mapDriver.withInput(ignored,
        new Text("id1" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val4" + Constants.COL_DELIM + "val3"));
    mapDriver.withInput(ignored, new Text("id2" + Constants.COL_DELIM + "val" + Constants.COL_DELIM + "val1"));

    mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val1", "val5"));
    mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val1", "val3"));
    mapDriver.withOutput(new TextMultiple("val", "val2"), new TextMultiple("id1", "val4", "val3"));
    mapDriver.runTest();
  }
  
  @Test
  public void reducer() throws IOException {
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
    reduceDriver.setConfiguration(job.getConfiguration());

    
    // reduceDriver.runTest();
  }

}
