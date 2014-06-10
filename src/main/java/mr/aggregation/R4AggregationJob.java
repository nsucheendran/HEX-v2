package mr.aggregation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mr.CFInputFormat;
import mr.Constants;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/*
 * MapReduce job that emulates the hiveql at
 * <link>github.com/ExpediaEDW/edw-HEXHadoopETL/src/main/scripts/hql/FACT/insertTable_ETL_HCOM_HEX_FACT_ALT.hql</link>
 * (and runs faster trading MR steps for reducer memory)
 * 
 * @author nsood
 * @author achadha
 */
public final class R4AggregationJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(R4AggregationJob.class);
  private static final String jobName = "hdp_hww_hex_etl_fact_aggregation";

  // private Pattern partitionDirPattern; // = Pattern.compile("(.*)(" +
  // jobName + ")(\\/)(.*)(\\/)(^\\/)*");
  // private Pattern partitionBkupDirPattern; // = Pattern.compile("(.*)(" +
  // jobName + "bkup)(\\/)(.*)(\\/)(^\\/)*");

  public static void main(final String[] args) throws Exception {
    GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), new Options(), args);
    int res = ToolRunner.run(parser.getConfiguration(), new R4AggregationJob(), args);
    System.exit(res);
  }

  public int run(final String[] arg0) throws IOException, TException, InterruptedException, ClassNotFoundException {
    Configuration mainConf = super.getConf();
    int numReduceTasks = Integer.parseInt(mainConf.get("reducers", "100"));
    String queueName = mainConf.get("queueName", "edwdev");
    String sourceDbName = mainConf.get("sourceDbName", "etldata");
    String targetDbName = mainConf.get("targetDbName", "hwwdev");
    String sourceTableName = mainConf.get("sourceTableName", "etl_hcom_hex_fact_staging");
    String targetTableName = mainConf.get("targetTableName", "etl_hcom_hex_fact_non_partitioned");
    String reportTableName = mainConf.get("reportTableName", "hex_reporting_requirements");

    JobConfigurator configurator = new JobConfigurator();
    Job job = configurator.initJob(mainConf, jobName, queueName);

    List<String> lhsfields, rhsfields;
    HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
    try {
      setInputPathsFromTable(sourceDbName, sourceTableName, job, cl);

      List<FieldSchema> fieldschemas = cl.getFields(sourceDbName, sourceTableName);
      lhsfields = new ArrayList<String>(fieldschemas.size());
      for (FieldSchema field : fieldschemas) {
        lhsfields.add(field.getName());
      }
      List<FieldSchema> rhsfieldschemas = cl.getFields(targetDbName, reportTableName);
      rhsfields = new ArrayList<String>(rhsfieldschemas.size());
      for (FieldSchema field : rhsfieldschemas) {
        rhsfields.add(field.getName());
      }
    } finally {
    }
    configurator.lhsFields(lhsfields).rhsFields(rhsfields).numReduceTasks(numReduceTasks);
    configurator.configureJob(job);
    job.getConfiguration().set("data", getReportDataAsString(reportTableName, targetDbName, configurator, job, cl));
    // job.getConfiguration().set
    // Set JVM reuse to speed up reducer
    // conf.setNumTasksToExecutePerJvm(-1);
    Table table = cl.getTable(targetDbName, targetTableName);
    StorageDescriptor tableSd = table.getSd();
    Path outputPath = new Path(tableSd.getLocation());

    FileSystem fileSystem = null;
    boolean success = false;
    try {
      fileSystem = outputPath.getFileSystem(job.getConfiguration());
      success = fileSystem.delete(outputPath, true);
      if (success) {
        // MultipleOutputs.setCountersEnabled(job, true);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        // MultipleOutputs.addNamedOutput(job, "outroot",
        // SequenceFileOutputFormat.class, BytesWritable.class,
        // Text.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        // FileOutputFormat.setCompressOutput(job, true);
        // FileOutputFormat.setOutputCompressorClass(job,
        // org.apache.hadoop.io.compress.SnappyCodec.class);

        success = job.waitForCompletion(true);
        log.info("output written to: " + outputPath.toString());
      } else {
        log.info("Not able to delete output path: " + outputPath + ". Exiting!!!");
      }
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      cl.close();
    }
    return success ? 0 : -1;
  }

  private void setInputPathsFromTable(String sourceDbName, String sourceTableName, Job job, HiveMetaStoreClient cl)
    throws TException, IOException {
    Table table = cl.getTable(sourceDbName, sourceTableName);
    Path tblPath = new Path(table.getSd().getLocation());
    FileSystem fileSystem = tblPath.getFileSystem(job.getConfiguration());

    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(tblPath, true);
    Set<String> inputPathsAdded = new HashSet<String>();
    StringBuilder inputPathsBuilder = new StringBuilder();
    boolean first = true;
    while (files.hasNext()) {
      LocatedFileStatus lfs = files.next();
      String[] pathSplits = lfs.getPath().toString().split("\\" + Path.SEPARATOR);
      StringBuilder pathMinusFileName = new StringBuilder();
      for (int j = 0; j < pathSplits.length - 1; j++) {
        pathMinusFileName.append(pathSplits[j] + Path.SEPARATOR);
      }
      if (!inputPathsAdded.contains(pathMinusFileName.toString())) {
        if (!first) {
          inputPathsBuilder.append(",");
        }
        inputPathsBuilder.append(pathMinusFileName).append("*");
        log.info("Adding input path to process: " + pathMinusFileName);
        first = false;
        inputPathsAdded.add(pathMinusFileName.toString());
      }
    }
    fileSystem.close();
    CFInputFormat.setInputPaths(job, inputPathsBuilder.toString());
  }

  private String getReportDataAsString(String reportTableName, String reportDbName, JobConfigurator configurator,
      Job job, HiveMetaStoreClient cl) throws IOException, TException {
    StringBuilder data = new StringBuilder();
    Table table = cl.getTable(reportDbName, reportTableName);
    Path tblPath = new Path(table.getSd().getLocation());
    FileSystem fileSystem = tblPath.getFileSystem(job.getConfiguration());

    BufferedReader br = null;
    try {
      RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(tblPath, true);
      while (files.hasNext()) {
        br = new BufferedReader(new InputStreamReader(fileSystem.open(files.next().getPath())));
        String line;
        while ((line = br.readLine()) != null) {
          configurator.stripe(line, data, Constants.REPORT_TABLE_COL_DELIM);
          data.append("\n");
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
      if (fileSystem != null) {
        fileSystem.close();
      }
    }
    // log.info("Reporting Data: " + data.toString());
    return data.toString();
  }
}
