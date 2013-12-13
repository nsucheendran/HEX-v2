package mr.aggregation;

import java.io.IOException;
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

import mr.JobConfigurator;
import mr.exceptions.UnableToMoveDataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
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
    private static final Pattern partitionDirPattern = Pattern.compile("(.*)(" + jobName + ")(\\/)(.*)(\\/)(^\\/)*");
    private static final Pattern partitionBkupDirPattern = Pattern.compile("(.*)(" + jobName + "bkup)(\\/)(.*)(\\/)(^\\/)*");

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new R4AggregationJob(), args);
        System.exit(res);
    }


    public int run(final String[] arg0) throws IOException, TException, InterruptedException, ClassNotFoundException {
        String queueName = "edwdev";
        String dbName = "etldata";
        String repDbName = "hwwdev";
        String tableName = "etl_hcom_hex_fact_staging";
        String outputTableName = "etl_hcom_hex_fact";

        String reportFilePath = "/user/hive/warehouse/hwwdev.db/hex_reporting_requirements/000000_0";
        String reportTableName = "hex_reporting_requirements";
        String tmpOutputPath = "/tmp/" + jobName;

        int numReduceTasks = 100;

        JobConfigurator configurator = new JobConfigurator();
        Job job = configurator.initJob(super.getConf(), jobName, queueName);

        List<String> lhsfields, rhsfields;
        HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
        try {

            Table table = cl.getTable(dbName, tableName);
            Path tblPath = new Path(table.getSd().getLocation());
            FileSystem fileSystem = tblPath.getFileSystem(job.getConfiguration());
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(
                    tblPath, true);
            Set<String> inputPathsAdded = new HashSet<String>();
            StringBuilder inputPathsBuilder = new StringBuilder();
            boolean first = true;
            while (files.hasNext()) {
                LocatedFileStatus lfs = files.next();
                String[] pathSplits = lfs.getPath().toString()
                        .split("\\" + Path.SEPARATOR);
                StringBuilder pathMinusFileName = new StringBuilder();
                for (int j = 0; j < pathSplits.length - 1; j++) {
                    pathMinusFileName.append(pathSplits[j] + Path.SEPARATOR);
                }
                if (!inputPathsAdded.contains(pathMinusFileName.toString())) {
                    if (!first) {
                        inputPathsBuilder.append(",");
                    }
                    inputPathsBuilder.append(pathMinusFileName).append("*");
                    log.info("Adding input path to process: "
                            + pathMinusFileName);
                    first = false;
                    inputPathsAdded.add(pathMinusFileName.toString());
                }
            }
            fileSystem.close();
            FileInputFormat.setInputPaths(job, inputPathsBuilder.toString());

            List<FieldSchema> fieldschemas = cl.getFields(dbName, tableName);
            lhsfields = new ArrayList<String>(fieldschemas.size());
            for (FieldSchema field : fieldschemas) {
                lhsfields.add(field.getName());
            }
            List<FieldSchema> rhsfieldschemas = cl.getFields(repDbName,
                    reportTableName);
            rhsfields = new ArrayList<String>(rhsfieldschemas.size());
            for (FieldSchema field : rhsfieldschemas) {
                rhsfields.add(field.getName());
            }
        } finally {
            cl.close();
        }
        configurator.lhsFields(lhsfields).rhsFields(rhsfields).numReduceTasks(numReduceTasks);
        configurator.configureJob(job);
        job.getConfiguration().set("data",
                getDataAsString(reportFilePath, configurator, job));

        Path tempPath = new Path(tmpOutputPath);
        FileSystem fileSystem = tempPath.getFileSystem(job.getConfiguration());
        fileSystem.delete(tempPath, true);
        MultipleOutputs.setCountersEnabled(job, true);
        MultipleOutputs.addNamedOutput(job, "outroot", SequenceFileOutputFormat.class, BytesWritable.class, Text.class);
        FileOutputFormat.setOutputPath(job, tempPath);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);

        boolean success = job.waitForCompletion(true);
        log.info("output written to: " + tempPath.toString());

        Set<String> newPartitionsAdded = Sets.newHashSet();
        cl = new HiveMetaStoreClient(new HiveConf());
        String tableLocation = null;
        try {
            Table table = cl.getTable(dbName, outputTableName);
            Map<String, String> params = new HashMap<String, String>();
            StorageDescriptor tableSd = table.getSd();
            tableLocation = tableSd.getLocation();
            Set<String> partStrings = getPartitions(tmpOutputPath, tableLocation, job);

            for (String partString : partStrings) {
                StorageDescriptor partSd = new StorageDescriptor(tableSd);
                partSd.setLocation(tableLocation + Path.SEPARATOR + partString);
                List<String> values = getValues(partString);

                if (!values.isEmpty()) {
                    Partition part = new Partition(values, dbName, outputTableName,
                            (int) (System.currentTimeMillis() & 0x00000000FFFFFFFFL), 0, partSd, params);
                    try {
                        cl.dropPartition(dbName, outputTableName, values, false);
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
        return success ? 0 : -1;

    }

    private String getDataAsString(String reportFilePath, JobConfigurator configurator, Job job) throws IOException {
        StringBuilder data = new StringBuilder();
        SequenceFile.Reader repReader = new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(new Path(reportFilePath)));

        try {
            BytesWritable ignored = (BytesWritable) ReflectionUtils.newInstance(repReader.getKeyClass(), job.getConfiguration());

            Text value = (Text) ReflectionUtils.newInstance(repReader.getValueClass(), job.getConfiguration());

            while (repReader.next(ignored, value)) {
                configurator.stripe(new String(value.getBytes(), "utf-8"), data);
                data.append("\n");
            }
        } finally {
            IOUtils.closeStream(repReader);
        }
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

    private Set<String> getPartitions(String tmpOutputPath, String tableOutputPath, Job job) throws IOException, UnableToMoveDataException {
        String bkupTmpOutput = tmpOutputPath + "bkup";
        Path tmpOutPath = new Path(tmpOutputPath);
        Path bkupTmpOutPath = new Path(bkupTmpOutput);
        FileSystem outFileSystem = null;
        Set<String> partitionNames = Sets.newHashSet();
        try {
            outFileSystem = tmpOutPath.getFileSystem(job.getConfiguration());
            // delete & recreate bkup path, if exists
            outFileSystem.delete(bkupTmpOutPath, true);
            outFileSystem.mkdirs(bkupTmpOutPath);
            // recursively fetch all files in job output location
            RemoteIterator<LocatedFileStatus> files = outFileSystem.listFiles(tmpOutPath, true);
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
                            // System.out.println("Partition: " + partition);
                            String[] partitions = partition.split("\\/");
                            StringBuilder partitionMinusChild = new StringBuilder();
                            for (int i = 0; i < partitions.length - 1; i++) {
                                partitionMinusChild.append(partitions[i] + Path.SEPARATOR);
                            }
                            String partitionMinusChildStr = partitionMinusChild.toString();
                            // System.out.println(">>>>>>partitionMinusChildStr>>>>>>>>>>" + partitionMinusChildStr + "<<<<<<<<<<<<<<");
                            Path tablePartitionPath = new Path(tableOutputPath + Path.SEPARATOR + partition);
                            Path tablePartitionMinusChildPath = new Path(tableOutputPath + Path.SEPARATOR + partitionMinusChildStr);
                            // System.out.println(">>>>>>tablePartitionMinusChildPath>>>>>>>>>>" + tablePartitionMinusChildPath +
                            // "<<<<<<<<<<<<<<");
                            Path bkupPartionPath = new Path(bkupTmpOutput + Path.SEPARATOR + partition);
                            Path bkupPartionMinusChildPath = new Path(bkupTmpOutput + Path.SEPARATOR + partitionMinusChildStr);

                            boolean tablePartitionExists = outFileSystem.exists(tablePartitionPath);
                            // default true, as if table partition doesn't exist, we don't need any bkup
                            success = true;
                            // take existing data bkup, if exists
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
                            // System.out.println(">>>>tmp to table-> Rename " + tmpPartitionPathLoc + " to " +
                            // tablePartitionMinusChildPath);
                            success = outFileSystem.rename(new Path(tmpPartitionPathLoc), tablePartitionMinusChildPath);
                            if (!success) {
                                throw new UnableToMoveDataException("Unable to move data to table partition directory: "
                                        + tablePartitionMinusChildPath + " from tmp directory: " + tmpPartitionPathLoc);
                            }
                            if (success && tablePartitionExists) {
                                // System.out.println(">>>>>Delete " + bkupPartionPath);
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

    private void restoreDataFromBkup(String bkupTmpOutput, String tableOutputPath, Job job, Set<String> newPartitionsAdded) {
        Path bkupTmpOutPath = new Path(bkupTmpOutput);
        FileSystem outFileSystem = null;
        Set<String> partitionNames = Sets.newHashSet();
        try {
            outFileSystem = bkupTmpOutPath.getFileSystem(job.getConfiguration());
            // recursively fetch all files in job output location
            RemoteIterator<LocatedFileStatus> files = outFileSystem.listFiles(bkupTmpOutPath, true);
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
                        // System.out.println("Partition: " + partition);
                        String[] partitions = partition.split("\\/");
                        StringBuilder partitionMinusChild = new StringBuilder();
                        for (int i = 0; i < partitions.length - 1; i++) {
                            partitionMinusChild.append(partitions[i] + Path.SEPARATOR);
                        }
                        String partitionMinusChildStr = partitionMinusChild.toString();
                        // System.out.println(">>>>>>partitionMinusChildStr>>>>>>>>>>" + partitionMinusChildStr + "<<<<<<<<<<<<<<");
                        Path tablePartitionPath = new Path(tableOutputPath + Path.SEPARATOR + partition);
                        Path tablePartitionMinusChildPath = new Path(tableOutputPath + Path.SEPARATOR + partitionMinusChildStr);
                        // System.out.println(">>>>>>tablePartitionMinusChildPath>>>>>>>>>>" + tablePartitionMinusChildPath +
                        // "<<<<<<<<<<<<<<");
                        Path bkupPartionPath = new Path(bkupTmpOutput + Path.SEPARATOR + partition);
                        // Path bkupPartionMinusChildPath = new Path(bkupTmpOutput + Path.SEPARATOR + partitionMinusChildStr);
                        // System.out.println(">>>>>>bkupPartionMinusChildPath>>>>>>>>>>" + bkupPartionMinusChildPath + "<<<<<<<<<<<<<<");
                        // take existing data bkup, if exists
                        if (outFileSystem.exists(tablePartitionPath)) {
                            outFileSystem.delete(tablePartitionPath, true);
                        }
                        outFileSystem.rename(bkupPartionPath, tablePartitionMinusChildPath);
                        // System.out.println(">>>>>Delete " + bkupPartionPath);
                        outFileSystem.delete(bkupPartionPath, true);
                    }
                } else {
                    log.debug(">not matching>>>" + fs.getPath() + "<<<<" + found);
                }
            }
            // delete all new partitions
            for (String part : newPartitionsAdded) {
                Path tablePartitionPath = new Path(tableOutputPath + Path.SEPARATOR + part);
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
