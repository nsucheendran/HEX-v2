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
 * Multi-group-by Map-Reduce for segmenting AGG (denormalized star-schema table) into a multi-grain
 * table that's like a pure-molap cube (sorta, not as flexible of course)
 * 
 * @author achadha
 */
public final class SegmentationJob extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(SegmentationJob.class);
    private static final String jobName = "hdp_hww_hex_etl_fact_segmentation";

    // private Pattern partitionDirPattern; // = Pattern.compile("(.*)(" + jobName + ")(\\/)(.*)(\\/)(^\\/)*");
    // private Pattern partitionBkupDirPattern; // = Pattern.compile("(.*)(" + jobName + "bkup)(\\/)(.*)(\\/)(^\\/)*");

    public static void main(final String[] args) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), new Options(), args);
        int res = ToolRunner.run(parser.getConfiguration(), new SegmentationJob(), args);
        System.exit(res);
    }

    public int run(final String[] arg0) throws IOException, TException, InterruptedException, ClassNotFoundException {
        Configuration mainConf = super.getConf();
        int numReduceTasks = Integer.parseInt(mainConf.get("reducers", "100"));
        String queueName = mainConf.get("queueName", "edwdev");
        String sourceDbName = mainConf.get("sourceDbName", "dm");
        String targetDbName = mainConf.get("targetDbName", "hwwdev");
        String reportDbName = mainConf.get("reportDbName", "etldata");

        String sourceTableName = mainConf.get("sourceTableName", "rpt_hexdm_agg_unparted");
        String targetTableName = mainConf.get("targetTableName", "rpt_hexdm_seg_unparted");
        String reportTableName = mainConf.get("reportTableName", "hex_reporting_requirements");
        String segFilePath = mainConf.get("segFile", "/autofs/edwfileserver/sherlock_in/HEX/HEXV2UAT/segmentations.txt");

        SegmentationJobConfigurator configurator = new SegmentationJobConfigurator();
        Job job = configurator.initJob(mainConf, jobName, queueName);

        List<String> sourceFields, targetFields, rhsFields;
        HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
        Path outputPath = null;
        try {
            Table targetTable = cl.getTable(targetDbName, targetTableName);
            StorageDescriptor tableSd = targetTable.getSd();
            outputPath = new Path(tableSd.getLocation());
            
            setInputPathsFromTable(sourceDbName, sourceTableName, job, cl);

            List<FieldSchema> fieldschemas = cl.getFields(sourceDbName, sourceTableName);
            sourceFields = new ArrayList<String>(fieldschemas.size());
            for (FieldSchema field : fieldschemas) {
                sourceFields.add(field.getName());
            }
            
            fieldschemas = cl.getFields(targetDbName, targetTableName);
            targetFields = new ArrayList<String>(fieldschemas.size());
            for (FieldSchema field : fieldschemas) {
                targetFields.add(field.getName());
            }

            fieldschemas = cl.getFields(reportDbName, reportTableName);
            rhsFields = new ArrayList<String>(fieldschemas.size());
            for (FieldSchema field : fieldschemas) {
                rhsFields.add(field.getName());
            }
        } finally {
            cl.close();
        }
        BufferedReader segSpecReader = new BufferedReader(new InputStreamReader(new FileInputStream(segFilePath)));
        configurator.colMap(sourceFields, targetFields, segSpecReader, rhsFields).numReduceTasks(numReduceTasks);

        configurator.configureJob(job);
        job.getConfiguration().set("data", getReportDataAsString(reportTableName, reportDbName, configurator, job, cl));

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

    private String getReportDataAsString(String reportTableName, String reportDbName, SegmentationJobConfigurator configurator, Job job,
            HiveMetaStoreClient cl) throws IOException, TException {
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
