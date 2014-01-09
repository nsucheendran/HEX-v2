/*
 * @author achadha
 */

package mr.segmentation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mr.Constants;
import mr.dto.TextMultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class SegmentationJobConfigurator {
    private Map<String, String> equiJoinKeys = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            // lhs keys => rhs keys
            put("variant_code", "variant_code");
            put("experiment_code", "experiment_code");
            put("version_number", "version_number");
        }
    };

    private Map<String, String> lteJoinKeys = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            put("local_date", "report_end_date");
            put("trans_date", "trans_date");
        }
    };

    private Map<String, String> gteJoinKeys = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            put("local_date", "report_start_date");
            put("trans_date", "report_start_date");
        }
    };

    private Set<String> rhsKeys = new HashSet<String>();

    private Map<String, String> groupKeys = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            put("experiment_name", "Unknown");
            put("variant_name", "Unknown");
            put("report_start_date", "Unknown");
            put("report_end_date", "Unknown");
            put("status", "Unknown");
            put("report_transaction_end_date", "Unknown");
            put("test_manager", "Unknown");
            put("product_manager", "Unknown");
            put("pod", "Unknown");
            put("experiment_test_id", "Unknown");

            put("local_date", "Unknown");
            put("new_visitor_ind", "Unknown");
            put("page_assigned_entry_page_name", "Unknown");
            put("site_sectn_name", "Unknown");
            put("user_cntext_name", "Unknown");
            put("browser_height", "Unknown");
            put("browser_width", "Unknown");
            put("mobile_ind", "Unknown");
            put("platform_type", "Unknown");
            put("days_until_stay", "-9998");
            put("length_of_stay", "-9998");
            put("number_of_rooms", "-9998");
            put("number_of_adults_children", "Unknown");
            put("children_in_search_flag", "Unknown");
            put("entry_page_name", "Unknown");

            put("operating_system_name", "Unknown");

            put("brwsr_name", "Unknown");
            put("brwsr_typ_name", "Unknown");

            put("property_typ_name", "Unknown");
            put("property_parnt_chain_name", "Unknown");
            put("property_brand_name", "Unknown");
            put("property_super_regn_name", "Unknown");
            put("property_regn_id", "-9998");
            put("property_regn_name", "Unknown");
            put("property_mkt_id", "-9998");
            put("property_mkt_name", "Unknown");
            put("property_sub_mkt_id", "-9998");
            put("property_sub_mkt_name", "Unknown");
            put("property_cntry_name", "Unknown");
            put("property_state_provnc_name", "Unknown");
            put("property_city_name", "Unknown");
            put("expe_half_star_rtg", "Unknown");
            put("property_parnt_chain_acct_typ_name", "Unknown");
            put("property_paymnt_choice_enabl_ind", "Unknown");
            put("property_cntrct_model_name", "Unknown");

            put("posa_super_region", "Unknown");
            put("posa_region", "Unknown");
            put("posa_country", "Unknown");

            put("mktg_chnnl_name", "Unknown");
            put("mktg_sub_chnnl_name", "Unknown");

            put("mktg_chnnl_name_direct", "Unknown");
            put("mktg_sub_chnnl_name_direct", "Unknown");

            put("hcom_srch_dest_typ_name", "Unknown");
            put("hcom_srch_dest_name", "Unknown");
            put("hcom_srch_dest_cntry_name", "Unknown");
            put("hcom_srch_dest_id", "Unknown");

            put("psg_mkt_name", "Unknown");
            put("psg_mkt_regn_name", "Unknown");
            put("psg_mkt_super_regn_name", "Unknown");

            put("dom_intl_flag", "Unknown");

            put("experiment_code", "Unknown");
            put("version_number", "-1");
            put("variant_code", "Unknown");

        }
    };

    private Set<String> metrics = new HashSet<String>() {
        private static final long serialVersionUID = 1L;

        {
            add("num_unique_viewers");
            add("num_unique_purchasers");
            add("num_unique_cancellers");
            add("num_active_purchasers");
            add("num_inactive_purchasers");
            add("total_cancellations");
            add("net_orders");
            add("net_bkg_gbv");
            add("net_bkg_room_nights");
            add("net_omniture_gbv");
            add("net_omniture_room_nights");
            add("net_gross_profit");
            add("num_repeat_purchasers");
        }
    };

    private static class IntPair {
        private final int first;
        private final int second;

        IntPair(final int one, final int two) {
            this.first = one;
            this.second = two;
        }
    }

    private Map<String, IntPair> rhsPosMap;
    private int numReduceTasks = 100;
    private String colMapStr;
    private String metricsStr;
    private String segSpecStr;
    private String equiJoinPosMap;
    private String lteJoinPosMap;
    private String gteJoinPosMap;

    
    public Job initJob(Configuration config, String jobName, String queueName) throws IOException {
        JobConf conf = new JobConf(config);
        conf.setQueueName(queueName);
        Job job = new Job(conf, jobName);
        job.setJarByClass(SegmentationJob.class);

        job.setMapperClass(SegmentationMapper.class);
        job.setReducerClass(SegmentationReducer.class);
        job.setCombinerClass(SegmentationCombiner.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(TextMultiple.class);
        job.setMapOutputValueClass(TextMultiple.class);
        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
        job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        return job;
    }

    public SegmentationJobConfigurator equiJoinKeys(Map<String, String> equiJoinKeys) {
        this.equiJoinKeys = equiJoinKeys;
        return this;
    }

    public SegmentationJobConfigurator lteJoinKeys(Map<String, String> lteJoinKeys) {
        this.lteJoinKeys = lteJoinKeys;
        return this;
    }

    public SegmentationJobConfigurator gteJoinKeys(Map<String, String> gteJoinKeys) {
        this.gteJoinKeys = gteJoinKeys;
        return this;
    }

    public SegmentationJobConfigurator rhsKeys(Set<String> rhsKeys) {
        this.rhsKeys = rhsKeys;
        return this;
    }

    public SegmentationJobConfigurator groupKeys(Map<String, String> groupKeys) {
        this.groupKeys = groupKeys;
        return this;
    }
    
    public SegmentationJobConfigurator metrics(Set<String> metrics) {
        this.metrics = metrics;
        return this;
    }
    
    // prepare source-target column mappings and segmentation specification for sending to mappers
    public SegmentationJobConfigurator colMap(List<String> sourceFields, List<String> targetFields, BufferedReader segSpecReader,
            List<String> rhsFields) throws IOException {
        rhsFields(rhsFields);
        List<ColumnMapping> mappings = new ArrayList<ColumnMapping>(groupKeys.size());
        StringBuilder metricsSB = new StringBuilder();
        Map<String, Integer> equiLhsPosMap = new HashMap<String, Integer>();
        Map<String, Integer> lteLhsPosMap = new HashMap<String, Integer>();
        Map<String, Integer> gteLhsPosMap = new HashMap<String, Integer>();
        int i = 0;
        
        for (String targetField : targetFields) {
            if (sourceFields.contains(targetField)) {
                if (groupKeys.containsKey(targetField)) {
                    mappings.add(new ColumnMapping(sourceFields.indexOf(targetField), groupKeys.get(targetField)));
                } else if (metrics.contains(targetField)) {
                    if (i++ > 0) {
                        metricsSB.append(',');
                    }
                    metricsSB.append(sourceFields.indexOf(targetField));
                }

                if (equiJoinKeys.containsKey(targetField)) {
                    equiLhsPosMap.put(targetField, sourceFields.indexOf(targetField));
                }
                if (lteJoinKeys.containsKey(targetField)) {
                    lteLhsPosMap.put(targetField, sourceFields.indexOf(targetField));
                }
                if (gteJoinKeys.containsKey(targetField)) {
                    gteLhsPosMap.put(targetField, sourceFields.indexOf(targetField));
                }
            }
        }

        this.equiJoinPosMap = configString(equiLhsPosMap, equiJoinKeys).toString();
        this.lteJoinPosMap = configString(lteLhsPosMap, lteJoinKeys).toString();
        this.gteJoinPosMap = configString(gteLhsPosMap, gteJoinKeys).toString();

        this.metricsStr = metricsSB.toString();
        StringBuilder colMapSB = new StringBuilder();
        for (ColumnMapping mapping : mappings) {
            colMapSB.append(mapping.toString()).append('\n');
        }
        this.colMapStr = colMapSB.toString();
        StringBuilder segSpecSB = new StringBuilder();
        String line = segSpecReader.readLine();
        while (line != null && !"".equals(line.trim())) {
            String[] vals = line.split("\t");
            segSpecSB.append(vals[0]).append('\t').append(vals[1]).append('\t');
            String[] segFields = vals[2].split(",");
            int c = 0;
            for (String segField : segFields) {
                if (sourceFields.contains(segField) && groupKeys.containsKey(segField)) {
                    if (c++ > 0) {
                        segSpecSB.append(',');
                    }
                    segSpecSB.append(sourceFields.indexOf(segField));
                }
            }
            segSpecSB.append('\n');
            line = segSpecReader.readLine();
        }
        this.segSpecStr = segSpecSB.toString();
        return this;
    }

    private SegmentationJobConfigurator rhsFields(List<String> rhsFields) {
        int i = 0;
        int tableSize = 0;
        rhsPosMap = new HashMap<String, IntPair>();
        for (String field : rhsFields) {
            if (equiJoinKeys.values().contains(field) || lteJoinKeys.values().contains(field) || gteJoinKeys.values().contains(field)
                    || rhsKeys.contains(field)) {
                rhsPosMap.put(field, new IntPair(i, tableSize++));
            }
            ++i;
        }

        return this;
    }

    public SegmentationJobConfigurator numReduceTasks(int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
        return this;
    }

    public void configureJob(Job job) {
        job.setNumReduceTasks(numReduceTasks);

        int rk = 0;
        StringBuilder rhsKeySb = new StringBuilder();
        for (String fieldName : rhsKeys) {
            if (rk++ > 0) {
                rhsKeySb.append(",");
            }
            rhsKeySb.append(rhsPosMap.get(fieldName).second);
        }

        job.getConfiguration().set("colMap", colMapStr);
        job.getConfiguration().set("segSpecs", segSpecStr);

        job.getConfiguration().set("lhsVals", metricsStr);
        job.getConfiguration().set("rhsKeys", rhsKeySb.toString());
        job.getConfiguration().set("rhsVals", "");
        
        job.getConfiguration().set("eqjoin", equiJoinPosMap.toString());
        job.getConfiguration().set("ltejoin", lteJoinPosMap.toString());
        job.getConfiguration().set("gtejoin", gteJoinPosMap.toString());
    }

    public void stripe(String row, StringBuilder data) {
        this.stripe(row, data, Constants.COL_DELIM);
    }

    // stripe-out the columns from the rhs table that are needed for the join or select clauses
    public void stripe(String row, StringBuilder data, String colDelim) {
        String[] values = row.split(colDelim);
        String[] vals = new String[rhsPosMap.size()];
        // columns to be striped/collected may not be contiguously positioned in the table
        for (IntPair p : rhsPosMap.values()) {
            vals[p.second] = values[p.first];
        }
        int x = 0;
        for (String val : vals) {
            if (x++ > 0) {
                data.append("\t");
            }
            data.append(val);
        }
    }

    // build string representation of join criteria
    private StringBuilder configString(Map<String, Integer> posMap, Map<String, String> joinKeys) {
        int rk = 0;
        StringBuilder posMapSB = new StringBuilder();
        for (Map.Entry<String, String> entry : joinKeys.entrySet()) {
            String fieldName = entry.getKey();
            if (posMap.containsKey(fieldName)) {
                if (rk++ > 0) {
                    posMapSB.append(",");
                }
                String rField = entry.getValue();
                posMapSB.append(posMap.get(fieldName)).append("=").append(rhsPosMap.get(rField).second);
            }
        }
        return posMapSB;
    }

}
