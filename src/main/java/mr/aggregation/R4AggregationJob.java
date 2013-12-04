package mr.aggregation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class R4AggregationJob extends Configured implements Tool {
    private final Map<String, String> joinKeys = new HashMap<String, String>() {
        {
            put("variant_code", "variant_code");
            put("experiment_code", "experiment_code");
            put("version_number", "version_number");
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
            add("operating_system");
            add("all_mktg_seo");
            add("all_mktg_seo_direct");
            add("entry_page_name");
            add("experiment_name");
            add("variant_name");
            add("status");
            add("experiment_test_id");
            add("supplier_property_id");

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

    @Override
    public final int run(String[] arg0) throws Exception {
        String queueName = "edwdev";
        String dbName = "hwwdev";
        String tableName = "etl_hcom_hex_fact_staging_new";
        String jobName = "hdp_hww_hex_etl_fact_aggregation";
        String outputPath = "/user/hive/warehouse/hwwdev.db/hex_fact_adi";
        String reportFilePath = "/user/hive/warehouse/hwwdev.db/hex_reporting_requirements/000000_0";
        String reportTableName = "hex_reporting_requirements";
        int numReduceTasks = 100;

        JobConf conf = new JobConf(super.getConf());

        conf.setQueueName(queueName);
        // conf.setOutputFormat(HiveSequenceFileOutputFormat.class);
        Job job = new Job(conf, jobName);
        job.setJarByClass(R4AggregationJob.class);

        job.setMapperClass(R4Mapper.class);
        job.setReducerClass(R4Reducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
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

            Map<String, Integer> lhsPosMap = new HashMap<String, Integer>();
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
                if (joinKeys.containsKey(field.getName())) {
                    lhsPosMap.put(field.getName(), i);
                }
                i++;
            }

            List<FieldSchema> rhsfields = cl.getFields(dbName, reportTableName);
            StringBuilder rhsKeySb = new StringBuilder();
            i = 0;
            int tableSize = 0;
            Map<String, IntPair> rhsPosMap = new HashMap<String, IntPair>();
            for (FieldSchema field : rhsfields) {
                if (joinKeys.values().contains(field.getName()) || rhsKeys.contains(field.getName())) {
                    rhsPosMap.put(field.getName(), new IntPair(i, tableSize++));
                }
                ++i;
            }

            int rk = 0;
            StringBuilder joinPosMap = new StringBuilder();
            for (String fieldName : joinKeys.keySet()) {
                if(rk++>0)
                    joinPosMap.append(",");
                joinPosMap.append(lhsPosMap.get(fieldName)).append("=").append(rhsPosMap.get(fieldName).two);
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

            job.getConfiguration().set("join", joinPosMap.toString());
            System.out.println("join: " + joinPosMap);

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
        MultipleOutputs.addNamedOutput(job, "outroot", SequenceFileOutputFormat.class, NullWritable.class, TextMultiple.class);
        FileOutputFormat.setOutputPath(job, outPath);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);

        boolean success = job.waitForCompletion(true);
        System.out.println("output written to: " + outPath.toString());

        return success ? 0 : -1;
    }
}
