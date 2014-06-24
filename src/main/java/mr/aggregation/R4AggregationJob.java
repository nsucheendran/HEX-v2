package mr.aggregation;

import static com.expedia.edw.hww.common.hadoop.metrics.CountersToMapFunction.countersToMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.edw.hww.common.hadoop.spring.DriverEntryPoint;
import com.expedia.edw.hww.common.logging.ManifestAttributes;
import com.expedia.edw.hww.common.metrics.StatsWriter;
import com.expedia.edw.hww.hex.etl.Constants;

/*
 * MapReduce job that emulates the hiveql at
 * <link>github.com/ExpediaEDW/edw-HEXHadoopETL/src/main/scripts/hql/FACT/insertTable_ETL_HCOM_HEX_FACT_ALT.hql</link>
 * (and runs faster trading MR steps for reducer memory)
 * 
 * @author nsood
 * @author achadha
 */
@Component
public final class R4AggregationJob implements DriverEntryPoint {
  private static final Logger log = Logger.getLogger(R4AggregationJob.class);
  private static final String jobName = "hdp_hww_hex_etl_fact_aggregation";

  private final List<String> args;
  private final Configuration configuration;
  private final StatsWriter statsWriter;
  private final ManifestAttributes manifestAttributes;
  private final int numReduceTasks;
  private final String queueName;
  private final String sourceDbName;
  private final String targetDbName;
  private final String sourceTableName;
  private final String targetTableName;
  private final String reportTableName;

  @Autowired
  R4AggregationJob(@Value("#{args}") List<String> args, Configuration configuration, StatsWriter statsWriter,
      ManifestAttributes manifestAttributes, @Value("${aggregation.reducers}") int numReduceTasks,
      @Value("${aggregation.queue.name}") String queueName,
      @Value("${aggregation.source.db.name}") String sourceDbName,
      @Value("${aggregation.target.db.name}") String targetDbName,
      @Value("${aggregation.source.table.name}") String sourceTableName,
      @Value("${aggregation.target.table.name}") String targetTableName,
      @Value("${aggregation.report.table.name}") String reportTableName) {
    this.args = args;
    this.configuration = configuration;
    this.statsWriter = statsWriter;
    this.manifestAttributes = manifestAttributes;
    this.numReduceTasks = numReduceTasks;
    this.queueName = queueName;
    this.sourceDbName = sourceDbName;
    this.targetDbName = targetDbName;
    this.sourceTableName = sourceTableName;
    this.targetTableName = targetTableName;
    this.reportTableName = reportTableName;
  }

  @Override
  public void run() throws IOException, TException, InterruptedException, ClassNotFoundException {
    JobConfigurator configurator = new JobConfigurator();
    Job job = configurator.initJob(configuration, jobName, queueName);

    List<String> lhsfields, rhsfields;
    HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
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

    configurator.lhsFields(lhsfields).rhsFields(rhsfields).numReduceTasks(numReduceTasks);
    configurator.configureJob(job);
    job.getConfiguration().set("data", getReportDataAsString(reportTableName, targetDbName, configurator, job, cl));
    Table table = cl.getTable(targetDbName, targetTableName);
    StorageDescriptor tableSd = table.getSd();
    Path outputPath = new Path(tableSd.getLocation());

    FileSystem fileSystem = null;
    boolean success = false;
    try {
      fileSystem = outputPath.getFileSystem(job.getConfiguration());
      success = fileSystem.delete(outputPath, true);
      if (success) {
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        FileOutputFormat.setOutputPath(job, outputPath);

        success = job.waitForCompletion(true);
        log.info("output written to: " + outputPath.toString());

        log.info("Enabling StatsWriter");
        statsWriter.writeStats(countersToMap(manifestAttributes).apply(job.getCounters()));
      } else {
        log.info("Not able to delete output path: " + outputPath + ". Exiting!!!");
      }
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      cl.close();
    }
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
    FileInputFormat.setInputPaths(job, inputPathsBuilder.toString());
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
    return data.toString();
  }
}
