package mr.aggregation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mr.Constants;
import mr.exceptions.UnableToMoveDataException;

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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Sets;

/*
 * MapReduce job that emulates the hiveql at
 * <link>github.com/ExpediaEDW/edw-HEXHadoopETL/src/main/scripts/hql/FACT/insertTable_ETL_HCOM_HEX_FACT.hql</link>
 * (and runs faster trading MR steps for reducer memory)
 * 
 * @author nsood
 * @author achadha
 */
public final class R4AggregationJob extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(R4AggregationJob.class);
    private static final String jobName = "hdp_hww_hex_etl_fact_aggregation";
    private static final String logsDirName = "_logs";
    private Pattern partitionDirPattern; // = Pattern.compile("(.*)(" + jobName + ")(\\/)(.*)(\\/)(^\\/)*");
    private Pattern partitionBkupDirPattern; // = Pattern.compile("(.*)(" + jobName + "bkup)(\\/)(.*)(\\/)(^\\/)*");

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
        String targetTableName = mainConf.get("targetTableName", "etl_hcom_hex_fact");
        String reportTableName = mainConf.get("reportTableName",
                "hex_reporting_requirements");

        JobConfigurator configurator = new JobConfigurator();
        Job job = configurator.initJob(mainConf, jobName, queueName);

        String tmpOutputPath = "/tmp/" + jobName;
        partitionDirPattern = Pattern.compile("(.*)(" + jobName
                + ")(\\/)(.*)(\\/)(^\\/)*");
        partitionBkupDirPattern = Pattern.compile("(.*)(" + jobName
                + "bkup)(\\/)(.*)(\\/)(^\\/)*");
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
            cl.close();
        }
        configurator.lhsFields(lhsfields).rhsFields(rhsfields).numReduceTasks(numReduceTasks);
        configurator.configureJob(job);
        job.getConfiguration().set(
                "data",
                getReportDataAsString(reportTableName, targetDbName, configurator,
                        job, cl));
        // job.getConfiguration().set
        // Set JVM reuse to speed up reducer
        // conf.setNumTasksToExecutePerJvm(-1);
        Path tempPath = new Path(tmpOutputPath);
        FileSystem fileSystem = null;
        boolean success = false;
        try {
            fileSystem = tempPath.getFileSystem(job.getConfiguration());
            if (success = fileSystem.delete(tempPath, true)) {
                // MultipleOutputs.setCountersEnabled(job, true);
                job.getConfiguration().setBoolean("mapred.compress.map.output",
                        true);
                job.getConfiguration().set(
                        "mapred.map.output.compression.codec",
                        "org.apache.hadoop.io.compress.SnappyCodec");
                //MultipleOutputs.addNamedOutput(job, "outroot",
                  //      SequenceFileOutputFormat.class, BytesWritable.class,
                    //    Text.class);
                FileOutputFormat.setOutputPath(job, tempPath);

                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job,
                        org.apache.hadoop.io.compress.SnappyCodec.class);

                success = job.waitForCompletion(true);
                log.info("output written to: " + tempPath.toString());

                createPartitions(sourceDbName, targetTableName, job,
                        tmpOutputPath);
            } else {
                log.info("Not able to delete temp path: " + tempPath
                        + ". Exiting!!!");
            }
        } finally {
            fileSystem.close();
        }
        return success ? 0 : -1;

    }

    private void setInputPathsFromTable(String sourceDbName, String sourceTableName, Job job, HiveMetaStoreClient cl) throws TException,
            IOException {
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

    private void createPartitions(String sourceDbName, String targetTableName, Job job, String tmpOutputPath) throws MetaException {
        HiveMetaStoreClient cl;
        Set<String> newPartitionsAdded = Sets.newHashSet();
        cl = new HiveMetaStoreClient(new HiveConf());
        String tableLocation = null;
        try {
            Table table = cl.getTable(sourceDbName, targetTableName);
            Map<String, String> params = new HashMap<String, String>();
            StorageDescriptor tableSd = table.getSd();
            tableLocation = tableSd.getLocation();
            Set<String> partStrings = getPartitions(tmpOutputPath, tableLocation, job);

            for (String partString : partStrings) {
                StorageDescriptor partSd = new StorageDescriptor(tableSd);
                partSd.setLocation(tableLocation + Path.SEPARATOR + partString);
                List<String> values = getValues(partString);

                if (!values.isEmpty()) {
                    Partition part = new Partition(values, sourceDbName, targetTableName,
                            (int) (System.currentTimeMillis() & 0x00000000FFFFFFFFL), 0, partSd, params);
                    try {
                        cl.dropPartition(sourceDbName, targetTableName, values, false);
                    } catch (NoSuchObjectException ex) {
                        log.debug("New Partition:" + partString);
                        newPartitionsAdded.add(partString);
                    }
                    cl.add_partition(part);
                }
            }
        } catch (Exception ex) {
            restoreDataFromBkup(tmpOutputPath + "bkup", tableLocation, job, newPartitionsAdded);
            throw new UnableToMoveDataException(ex);
        } finally {
            cl.close();
        }
    }

    private String getReportDataAsString(String reportTableName, String reportDbName,
            JobConfigurator configurator, Job job, HiveMetaStoreClient cl)
        throws IOException,
            TException {
        StringBuilder data = new StringBuilder();
        Table table = cl.getTable(reportDbName, reportTableName);
        Path tblPath = new Path(table.getSd().getLocation());
        FileSystem fileSystem = tblPath.getFileSystem(job.getConfiguration());

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(tblPath,
                true);
        BufferedReader br = null;
        try {
            while (files.hasNext()) {
                br = new BufferedReader(new InputStreamReader(
                        fileSystem.open(files.next().getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    configurator.stripe(line, data,
                            Constants.REPORT_TABLE_COL_DELIM);
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
        log.info("Reporting Data: " + data.toString());
        return data.toString();
    }

    private List<String> getValues(final String partString) throws UnsupportedEncodingException {
        String[] pairs = partString.split("/");
        List<String> values = new ArrayList<String>(3);
        if (partString.contains("=")) {
            for (String pair : pairs) {
                String val = pair.split("=")[1];
                val = URLDecoder.decode(val, "UTF-8");
                values.add(val);
            }
        }
        return values;
    }

    private Set<String> getPartitions(String sourcePath, String targetPath, Job job) throws IOException, UnableToMoveDataException {
        String backupPath = sourcePath + "bkup";
        Path srcPath = new Path(sourcePath);
        Path bkpPath = new Path(backupPath);
        FileSystem outFileSystem = null;
        Set<String> partitionNames = Sets.newHashSet();
        try {
            outFileSystem = srcPath.getFileSystem(job.getConfiguration());
            // delete & recreate bkup path, if exists
            outFileSystem.delete(bkpPath, true);
            outFileSystem.mkdirs(bkpPath);
            // recursively fetch all files in job output location
            RemoteIterator<LocatedFileStatus> files = outFileSystem.listFiles(srcPath, true);
            boolean success = true;
            while (files.hasNext()) {
                LocatedFileStatus fs = files.next();
                String path = fs.getPath().toString();
                if (!path.contains(logsDirName)) {
                    Matcher m = partitionDirPattern.matcher(path);
                    boolean found = m.find();
                    if (found) {
                        // extract just the partition path
                        String partition = m.group(4);
                        // if the partition has not been moved already
                        if (partitionNames.add(partition)) {
                            String[] partitions = partition.split("\\/");
                            StringBuilder partitionMinusChild = new StringBuilder();
                            for (int i = 0; i < partitions.length - 1; i++) {
                                partitionMinusChild.append(partitions[i] + Path.SEPARATOR);
                            }
                            String partitionMinusChildStr = partitionMinusChild.toString();
                            Path tablePartitionPath = new Path(targetPath + Path.SEPARATOR + partition);
                            Path tablePartitionMinusChildPath = new Path(targetPath + Path.SEPARATOR + partitionMinusChildStr);
                            Path bkupPartionPath = new Path(backupPath + Path.SEPARATOR + partition);
                            
                            Path bkupPartionMinusChildPath = new Path(backupPath + Path.SEPARATOR + partitionMinusChildStr);

                            // default true, as if table partition doesn't exist, we don't need any bkup
                            success = true;
                            // take existing data bkup, if exists
                            boolean tablePartitionExists = outFileSystem.exists(tablePartitionPath);
                            if (tablePartitionExists) {
                                if (!outFileSystem.exists(bkupPartionMinusChildPath)) {
                                    success = outFileSystem.mkdirs(bkupPartionMinusChildPath);
                                    if (!success) {
                                        throw new UnableToMoveDataException("Unable to create backup directory: "
                                                + bkupPartionMinusChildPath);
                                    }
                                }
                                success = outFileSystem.rename(tablePartitionPath, bkupPartionMinusChildPath);
                                if (!success) {
                                    throw new UnableToMoveDataException("Unable to take table partition data backup. " + "Renaming "
                                            + tablePartitionPath + " to " + bkupPartionMinusChildPath);
                                }
                            } else if (!outFileSystem.exists(tablePartitionMinusChildPath)) {
                                success = outFileSystem.mkdirs(tablePartitionMinusChildPath);
                                if (!success) {
                                    throw new UnableToMoveDataException("Unable to make table partition directory: "
                                            + tablePartitionMinusChildPath);
                                }
                            }
                            // move temp data to table
                            String tmpPartitionPathLoc = new StringBuilder(m.group(1)).append(m.group(2)).append(m.group(3))
                                    .append(m.group(4)).toString();

                            success = outFileSystem.rename(new Path(tmpPartitionPathLoc), tablePartitionMinusChildPath);
                            if (!success) {
                                throw new UnableToMoveDataException("Unable to move data to table partition directory: "
                                        + tablePartitionMinusChildPath + " from tmp directory: " + tmpPartitionPathLoc);
                            }
                            if (success && tablePartitionExists) {
                                outFileSystem.delete(bkupPartionPath, true);
                            }
                        }
                    } else {
                        log.debug(">not matching>>>" + fs.getPath() + "<<<<" + found);
                    }
                }
            }
        } finally {
            if (outFileSystem != null) {
                outFileSystem.close();
            }
        }
        return partitionNames;
    }

    private void restoreDataFromBkup(String sourcePath, String targetPath, Job job, Set<String> newPartitionsAdded) {
        Path bkpPath = new Path(sourcePath);
        FileSystem outFileSystem = null;
        Set<String> partitionNames = Sets.newHashSet();
        try {
            outFileSystem = bkpPath.getFileSystem(job.getConfiguration());
            // recursively fetch all files in job output location
            RemoteIterator<LocatedFileStatus> files = outFileSystem.listFiles(bkpPath, true);
            while (files.hasNext()) {
                LocatedFileStatus fs = files.next();
                String path = fs.getPath().toString();
                Matcher m = partitionBkupDirPattern.matcher(path);
                boolean found = m.find();
                if (found) {
                    // extract just the partition path
                    String partition = m.group(4);
                    // if the partition has not been moved already
                    if (partitionNames.add(partition)) {
                        String[] partitions = partition.split("\\/");
                        StringBuilder partitionMinusChild = new StringBuilder();
                        for (int i = 0; i < partitions.length - 1; i++) {
                            partitionMinusChild.append(partitions[i] + Path.SEPARATOR);
                        }
                        String partitionMinusChildStr = partitionMinusChild.toString();
                        Path tablePartitionPath = new Path(targetPath + Path.SEPARATOR + partition);
                        Path tablePartitionMinusChildPath = new Path(targetPath + Path.SEPARATOR + partitionMinusChildStr);
                        Path bkupPartionPath = new Path(sourcePath + Path.SEPARATOR + partition);
                        
                        // take existing data bkup, if exists
                        if (outFileSystem.exists(tablePartitionPath)) {
                            outFileSystem.delete(tablePartitionPath, true);
                        }
                        outFileSystem.rename(bkupPartionPath, tablePartitionMinusChildPath);
                        outFileSystem.delete(bkupPartionPath, true);
                    }
                } else {
                    log.debug(">not matching>>>" + fs.getPath() + "<<<<" + found);
                }
            }
            // delete all new partitions
            for (String part : newPartitionsAdded) {
                Path tablePartitionPath = new Path(targetPath + Path.SEPARATOR + part);
                outFileSystem.delete(tablePartitionPath, true);
            }
        } catch (IOException e) {
            throw new UnableToMoveDataException("Unable to restore data from bkup. ", e);
        } finally {
            try {
                if (outFileSystem != null) {
                    outFileSystem.close();
                }
            } catch (IOException e) {
                throw new UnableToMoveDataException("Unable to close filesystem obj while restoring data from bkup. ", e);
            }
        }
    }
}
