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

import mr.dto.TextMultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Sets;

public class R4AggregationJob extends Configured implements Tool {
    private String jobName = "hdp_hww_hex_etl_fact_aggregation";
    // hdfs://nameservice1/tmp/hdp_hww_hex_etl_fact_aggregation1386152554484/experiment_code=H848/variant_code=S598.6382/version_number=1/result-r-00099
    private static Pattern partitionDirPattern = Pattern.compile("(.*)(hdp_hww_hex_etl_fact_aggregation)(\\/)(.*)(\\/)(^\\/)*");
    // hdfs://nameservice1/tmp/hdp_hww_hex_etl_fact_aggregation/experiment_code=H848/variant_code=S598.%25/version_number=1/result-r-00000
    private final Map<String, String> equiJoinKeys = new HashMap<String, String>() {
        {
            // lhs keys => rhs keys
            put("variant_code", "variant_code");
            put("experiment_code", "experiment_code");
            put("version_number", "version_number");
        }
    };
    
    private final Map<String, String> lteJoinKeys = new HashMap<String, String>() {
        {
            put("local_date", "report_end_date");
            put("trans_date", "trans_date");
        }
    };

    private final Map<String, String> gteJoinKeys = new HashMap<String, String>() {
        {
            put("local_date", "report_start_date");
            put("trans_date", "report_start_date");
        }
    };

    private final Set<String> rhsKeys = new HashSet<String>() {
        {
            add("experiment_name");
            add("variant_name");
            add("status");
            add("experiment_test_id");
        }
    };

    private final Set<String> groupKeys = new HashSet<String>() {
        {
            add("cid");
            add("local_date");
            add("new_visitor_ind");
            add("page_assigned_entry_page_name");
            add("site_sectn_name");
            add("user_cntext_name");
            add("browser_height");
            add("browser_width");
            add("brwsr_id");
            add("mobile_ind");
            add("destination_id");
            add("property_destination_id");
            add("platform_type");
            add("days_until_stay");
            add("length_of_stay");
            add("number_of_rooms");
            add("number_of_adults");
            add("number_of_children");
            add("children_in_search");
            add("operating_system_id");
            add("all_mktg_seo_30_day");
            add("all_mktg_seo_30_day_direct");
            add("operating_system");
            add("all_mktg_seo");
            add("all_mktg_seo_direct");
            add("entry_page_name");
            add("supplier_property_id");
            add("experiment_name");
            add("variant_name");
            add("status");
            add("experiment_test_id");

            add("variant_code");
            add("experiment_code");
            add("version_number");
        }
    };

    private R4AggregationJob() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new R4AggregationJob(), args);
        System.exit(res);
    }

    private static class IntPair {
        final int one;
        final int two;

        IntPair(int one, int two) {
            this.one = one;
            this.two = two;
        }
    }


    public final int run(String[] arg0) throws Exception {
        String queueName = "edwdev";
        String dbName = "hwwdev";
        String tableName = "etl_hcom_hex_fact_staging_new";
        String jobName = "hdp_hww_hex_etl_fact_aggregation";
        String outputTableName = "hex_fact_adi";

        // String tableOutputPath = "/user/hive/warehouse/hwwdev.db/hex_fact_adi";
//        String tmpOutputPath = "/tmp/";
        String reportFilePath = "/user/hive/warehouse/hwwdev.db/hex_reporting_requirements/000000_0";
        String reportTableName = "hex_reporting_requirements";
        String outputPath = "/tmp/hdp_hww_hex_etl_fact_aggregation/hex_fact_adi";

        int numReduceTasks = 100;

        JobConf conf = new JobConf(super.getConf());

        conf.setQueueName(queueName);
        // conf.setOutputFormat(HiveSequenceFileOutputFormat.class);
        Job job = new Job(conf, jobName);
        job.setJarByClass(R4AggregationJob.class);

        job.setMapperClass(R4Mapper.class);
        job.setReducerClass(R4Reducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TextMultiple.class);
        job.setMapOutputKeyClass(TextMultiple.class);
        job.setMapOutputValueClass(TextMultiple.class);
        job.setNumReduceTasks(numReduceTasks);

        HiveMetaStoreClient cl = new HiveMetaStoreClient(new HiveConf());
        try {

            Table table = cl.getTable(dbName, tableName);
            // System.out.println("input format: " + table.getSd().getOutputFormat());
            // System.out.println("output format: " + table.getSd().getInputFormat());
            Path tblPath = new Path(table.getSd().getLocation());
            FileSystem fileSystem = tblPath.getFileSystem(job.getConfiguration());
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(tblPath, true);

            while (files.hasNext()) {
                FileInputFormat.addInputPath(job, files.next().getPath());
            }
            fileSystem.close();

            List<FieldSchema> fields = cl.getFields(dbName, tableName);
            int i = 0;
            int kj = 0;
            int vj = 0;
            // Map<String, Integer> fieldMap = new LinkedHashMap<String, Integer>(fields.size());
            StringBuilder keySb = new StringBuilder();
            StringBuilder valSb = new StringBuilder();

            Map<String, Integer> equiLhsPosMap = new HashMap<String, Integer>();
            Map<String, Integer> lteLhsPosMap = new HashMap<String, Integer>();
            Map<String, Integer> gteLhsPosMap = new HashMap<String, Integer>();
            for (FieldSchema field : fields) {
                if (groupKeys.contains(field.getName())) {
                    if (kj++ > 0)
                        keySb.append(",");
                    keySb.append(i);
                    System.out.println(field.getName() + " => key[" + (kj - 1) + "]");
                } else {
                    if (vj++ > 0)
                        valSb.append(",");
                    valSb.append(i);
                    System.out.println(field.getName() + " => val[" + (vj - 1) + "]");
                }
                if (equiJoinKeys.containsKey(field.getName())) {
                    equiLhsPosMap.put(field.getName(), i);
                }
                if (lteJoinKeys.containsKey(field.getName())) {
                    lteLhsPosMap.put(field.getName(), i);
                }
                if (gteJoinKeys.containsKey(field.getName())) {
                    gteLhsPosMap.put(field.getName(), i);
                }

                i++;
            }

            List<FieldSchema> rhsfields = cl.getFields(dbName, reportTableName);
            StringBuilder rhsKeySb = new StringBuilder();
            i = 0;
            int tableSize = 0;
            Map<String, IntPair> rhsPosMap = new HashMap<String, IntPair>();
            for (FieldSchema field : rhsfields) {
                if (equiJoinKeys.values().contains(field.getName()) || lteJoinKeys.values().contains(field.getName())
                        || gteJoinKeys.values().contains(field.getName()) || rhsKeys.contains(field.getName())) {
                    rhsPosMap.put(field.getName(), new IntPair(i, tableSize++));
                }
                ++i;
            }

            int rk = 0;
            StringBuilder equiJoinPosMap = new StringBuilder();
            for (String fieldName : equiJoinKeys.keySet()) {
                if (rk++ > 0)
                    equiJoinPosMap.append(",");
                String rField = equiJoinKeys.get(fieldName);
                equiJoinPosMap.append(equiLhsPosMap.get(fieldName)).append("=").append(rhsPosMap.get(rField).two);
            }
            rk = 0;
            StringBuilder lteJoinPosMap = new StringBuilder();
            for (String fieldName : lteJoinKeys.keySet()) {
                if (rk++ > 0)
                    lteJoinPosMap.append(",");
                String rField = lteJoinKeys.get(fieldName);
                lteJoinPosMap.append(lteLhsPosMap.get(fieldName)).append("=").append(rhsPosMap.get(rField).two);
            }
            rk = 0;
            StringBuilder gteJoinPosMap = new StringBuilder();
            for (String fieldName : gteJoinKeys.keySet()) {
                if (rk++ > 0)
                    gteJoinPosMap.append(",");
                String rField = gteJoinKeys.get(fieldName);
                gteJoinPosMap.append(gteLhsPosMap.get(fieldName)).append("=").append(rhsPosMap.get(rField).two);
            }
            rk = 0;
            for (String fieldName : rhsKeys) {
                if (rk++ > 0)
                    rhsKeySb.append(",");
                rhsKeySb.append(rhsPosMap.get(fieldName).two);
            }

            job.getConfiguration().set("lhsKeys", keySb.toString());
            job.getConfiguration().set("lhsVals", valSb.toString());

            job.getConfiguration().set("rhsKeys", rhsKeySb.toString());
            job.getConfiguration().set("rhsVals", "");

            job.getConfiguration().set("eqjoin", equiJoinPosMap.toString());
            System.out.println("eqjoin: " + equiJoinPosMap);

            job.getConfiguration().set("ltejoin", lteJoinPosMap.toString());
            System.out.println("ltejoin: " + lteJoinPosMap);

            job.getConfiguration().set("gtejoin", gteJoinPosMap.toString());
            System.out.println("gtejoin: " + gteJoinPosMap);

            SequenceFile.Reader repReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(reportFilePath)));
            BytesWritable key = (BytesWritable) ReflectionUtils.newInstance(repReader.getKeyClass(), conf);

            Text value = (Text) ReflectionUtils.newInstance(repReader.getValueClass(), conf);
            StringBuilder data = new StringBuilder();

            while (repReader.next(key, value)) {
                String[] values = new String(value.getBytes()).split(new String(new char[] { 1 }));
                String[] vals = new String[tableSize];
                for (IntPair p : rhsPosMap.values()) {
                    vals[p.two] = values[p.one];
                }
                int x = 0;
                for (String val : vals) {
                    if (x++ > 0)
                        data.append("\t");
                    data.append(val);
                }
                data.append("\n");
            }

            IOUtils.closeStream(repReader);
            job.getConfiguration().set("data", data.toString());

        } finally {
            cl.close();
        }

        Path outPath = new Path(outputPath);
        FileSystem fileSystem = outPath.getFileSystem(job.getConfiguration());
        fileSystem.delete(outPath, true);
        MultipleOutputs.setCountersEnabled(job, true);
        MultipleOutputs.addNamedOutput(job, "outroot", SequenceFileOutputFormat.class, BytesWritable.class, Text.class);
        FileOutputFormat.setOutputPath(job, outPath);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);
        
        boolean success = job.waitForCompletion(true);
        System.out.println("output written to: " + outPath.toString());
        
        cl = new HiveMetaStoreClient(new HiveConf());
        try {
            Table table = cl.getTable(dbName, outputTableName);
            Map<String, String> params = new HashMap<String, String>();
            StorageDescriptor tableSd = table.getSd();
            // List<FieldSchema> partitionKeys = table.getPartitionKeys();
            String tableLocation = tableSd.getLocation();
            Set<String> partStrings = getPartitions(outputPath, tableLocation, job);

            for (String partString : partStrings) {
                StorageDescriptor partSd = new StorageDescriptor(tableSd);
                partSd.setLocation(tableLocation + Path.SEPARATOR + partString);
                List<String> values = getValues(partString);
                // cl.dropPartition(dbName, outputTableName, values);
                if (!values.isEmpty()) {
                    Partition part = new Partition(values, dbName, outputTableName,
                            (int) (System.currentTimeMillis() & 0x00000000FFFFFFFFL), 0, partSd, params);

                    cl.add_partition(part);
                }
            }
        } finally {
            cl.close();
        }

        return success ? 0 : -1;
    }


    private List<String> getValues(String partString) throws UnsupportedEncodingException {
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

    private Set<String> getPartitions(String tempLocation, String outputPath) {
        return new HashSet<String>() {
            {
                add("experiment_code=H848/variant_code=S598.6381/version_number=1");
                add("experiment_code=H848/variant_code=S598.6382/version_number=1");
                add("experiment_code=H848/variant_code=S598.%25/version_number=1");

            }
        };
    }
    

 // TODO: add failover mechanism to reload data from backup
    private Set<String> getPartitions(String tmpOutputPath, String tableOutputPath, Job job) throws IOException {
        String bkupTmpOutput = tmpOutputPath + "_bkup";
        Path tmpOutPath = new Path(tmpOutputPath);
        Path bkupTmpOutPath = new Path(bkupTmpOutput);

        FileSystem outFileSystem = tmpOutPath.getFileSystem(job.getConfiguration());
        // delete & recreate bkup path, if exists
        outFileSystem.delete(bkupTmpOutPath, true);
        outFileSystem.mkdirs(bkupTmpOutPath);
        // recursively fetch all files in job output location
        RemoteIterator<LocatedFileStatus> files = outFileSystem.listFiles(tmpOutPath, true);

        Set<String> partitionNames = Sets.newHashSet();
        while (files.hasNext()) {
            LocatedFileStatus fs = files.next();
            String path = fs.getPath().toString();
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
                    // System.out.println(">>>>>>tablePartitionMinusChildPath>>>>>>>>>>" + tablePartitionMinusChildPath + "<<<<<<<<<<<<<<");
                    Path bkupPartionPath = new Path(bkupTmpOutput + Path.SEPARATOR + partition);
                    Path bkupPartionMinusChildPath = new Path(bkupTmpOutput + Path.SEPARATOR + partitionMinusChildStr);
                    // System.out.println(">>>>>>bkupPartionMinusChildPath>>>>>>>>>>" + bkupPartionMinusChildPath + "<<<<<<<<<<<<<<");
                    boolean tablePartitionExists = false;
                    // default true, as if table partition doesn't exist, we don't need any bkup
                    boolean partitionBkupSuccessful = true;
                    // take existing data bkup, if exists
                    if (tablePartitionExists = outFileSystem.exists(tablePartitionPath)) {
                        if (!outFileSystem.exists(bkupPartionPath)) {
                            outFileSystem.mkdirs(bkupPartionMinusChildPath);
                        } else {
                            outFileSystem.delete(bkupPartionPath, true);
                        }
                        if (partitionBkupSuccessful = outFileSystem.rename(tablePartitionPath, bkupPartionMinusChildPath)) {
                            outFileSystem.delete(tablePartitionPath, true);
                        }
                        // System.out.println(">>>>table to bkup-> Rename " + tablePartitionPath + " to " + bkupPartionMinusChildPath);
                    } else {
                        outFileSystem.mkdirs(tablePartitionMinusChildPath);
                    }
                    // move temp data to table
                    String tmpPartitionPathLoc = new StringBuilder(m.group(1)).append(m.group(2)).append(m.group(3)).append(m.group(4))
                            .toString();
                    // System.out.println(">>>>tmp to table-> Rename " + tmpPartitionPathLoc + " to " + tablePartitionMinusChildPath);
                    outFileSystem.rename(new Path(tmpPartitionPathLoc), tablePartitionMinusChildPath);
                    if (tablePartitionExists) {
                        // System.out.println(">>>>>Delete " + bkupPartionPath);
                        outFileSystem.delete(bkupPartionPath, true);
                    }
                }
            } else {
                System.out.println(">not matching>>>" + fs.getPath() + "<<<<" + found);
            }
        }
        outFileSystem.close();
        return partitionNames;
    }
}
