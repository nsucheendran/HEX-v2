/*
 * @author achadha
 */

package mr.segmentation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mr.CFInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;

import com.expedia.edw.hww.common.hadoop.spring.DriverEntryPoint;
import com.expedia.edw.hww.common.logging.ManifestAttributes;
import com.expedia.edw.hww.common.metrics.StatsWriter;

/*
 * Multi-group-by Map-Reduce for segmenting AGG (denormalized star-schema table) into a multi-grain
 * table that's like a pure-molap cube (sorta, not as flexible of course)
 * 
 * @author achadha
 */
public final class SegmentationJob implements DriverEntryPoint {
  private static final Logger log = Logger.getLogger(SegmentationJob.class);
  private static final String jobName = "hdp_hww_hex_etl_fact_aggregation";

  List<String> args;
  @Autowired
  Configuration configuration;
  StatsWriter statsWriter;
  ManifestAttributes manifestAttributes;
  int numReduceTasks;
  String queueName;
  String sourceDbName;
  String targetDbName;
  String sourceTableName;
  String targetTableName;
  String segmentationInputFilePath;
  private FileSystem fileSystem;

  @Override
  public void run() throws IOException, NoSuchObjectException, TException, InterruptedException, ClassNotFoundException {
    Configuration mainConf = configuration;
    SegmentationJobConfigurator configurator = new SegmentationJobConfigurator();
    Job job = configurator.initJob(mainConf, jobName, queueName);

    List<String> sourceFields, targetFields;
    HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
    Path outputPath = null;
    try {
      Table targetTable = cl.getTable(targetDbName, targetTableName);
      StorageDescriptor tableSd = targetTable.getSd();
      outputPath = new Path(tableSd.getLocation());

      setInputPathsFromTable(sourceDbName, sourceTableName, job, cl);

      sourceFields = getFieldNames(sourceDbName, sourceTableName, cl);
      targetFields = getFieldNames(targetDbName, targetTableName, cl);
    } finally {
      cl.close();
    }

    BufferedReader segSpecReader = new BufferedReader(new InputStreamReader(new FileInputStream(
        segmentationInputFilePath)));
    try {
      configurator.colMap(sourceFields, targetFields, segSpecReader).numReduceTasks(numReduceTasks);
    } finally {
      segSpecReader.close();
    }

    configurator.configureJob(job);

    FileSystem fileSystem = null;
    boolean success = false;
    try {
      fileSystem = outputPath.getFileSystem(job.getConfiguration());
      success = fileSystem.delete(outputPath, true);
      if (success) {
        FileOutputFormat.setOutputPath(job, outputPath);
        success = job.waitForCompletion(true);
        log.info("output written to: " + outputPath.toString());
      } else {
        log.info("Not able to delete output path: " + outputPath + ". Exiting!");
      }
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
    }
  }

  private List<String> getFieldNames(String dbName, String tableName, HiveMetaStoreClient cl) throws TException {
    List<FieldSchema> fieldschemas = cl.getFields(dbName, tableName);
    List<String> fieldNames = new ArrayList<String>(fieldschemas.size());
    for (FieldSchema field : fieldschemas) {
      fieldNames.add(field.getName());
    }
    return fieldNames;
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
}
