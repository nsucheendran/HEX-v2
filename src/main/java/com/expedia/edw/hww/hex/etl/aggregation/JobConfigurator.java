package com.expedia.edw.hww.hex.etl.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.expedia.edw.hww.hex.etl.CFInputFormat;
import com.expedia.edw.hww.hex.etl.Constants;
import com.expedia.edw.hww.hex.etl.dto.TextMultiple;

public final class JobConfigurator {
  private Map<String, String> equiJoinKeys = new HashMap<String, String>() {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    {
      // lhs keys => rhs keys
      put("variant_code", "variant_code");
      put("experiment_code", "experiment_code");
      put("version_number", "version_number");
    }
  };

  private Map<String, String> lteJoinKeys = new HashMap<String, String>() {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    {
      put("local_date", "report_end_date");
      put("trans_date", "trans_date");
    }
  };

  private Map<String, String> gteJoinKeys = new HashMap<String, String>() {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    {
      put("local_date", "report_start_date");
      put("trans_date", "report_start_date");
    }
  };

  private Set<String> rhsKeys = new HashSet<String>() {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    {
      add("experiment_name");
      add("variant_name");
      add("status");
      add("experiment_test_id");
    }
  };

  private Set<String> groupKeys = new HashSet<String>() {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
      add("supplier_id");
      add("lodg_property_key");
      add("experiment_name");
      add("variant_name");
      add("status");
      add("experiment_test_id");

      add("variant_code");
      add("experiment_code");
      add("version_number");
    }
  };

  private static class IntPair {
    private final int one;
    private final int two;

    IntPair(final int one, final int two) {
      this.one = one;
      this.two = two;
    }
  }

  private Map<String, IntPair> rhsPosMap;

  private List<String> lhsfields;

  private int numReduceTasks = 100;

  public JobConfigurator() {

  }

  public Job initJob(Configuration config, String jobName, String queueName) throws IOException {
    JobConf conf = new JobConf(config);
    conf.setQueueName(queueName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(R4AggregationJob.class);

    job.setMapperClass(R4Mapper.class);
    job.setReducerClass(R4Reducer.class);
    // job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setInputFormatClass(CFInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(TextMultiple.class);
    job.setMapOutputValueClass(TextMultiple.class);
    // job.getConfiguration().setLong("mapred.min.split.size", 536870912L);
    return job;
  }

  public JobConfigurator equiJoinKeys(Map<String, String> equiJoinKeys) {
    this.equiJoinKeys = equiJoinKeys;
    return this;
  }

  public JobConfigurator lteJoinKeys(Map<String, String> lteJoinKeys) {
    this.lteJoinKeys = lteJoinKeys;
    return this;
  }

  public JobConfigurator gteJoinKeys(Map<String, String> gteJoinKeys) {
    this.gteJoinKeys = gteJoinKeys;
    return this;
  }

  public JobConfigurator groupKeys(Set<String> groupKeys) {
    this.groupKeys = groupKeys;
    return this;
  }

  public JobConfigurator rhsKeys(Set<String> rhsKeys) {
    this.rhsKeys = rhsKeys;
    return this;
  }

  public JobConfigurator lhsFields(List<String> lhsFields) {
    this.lhsfields = lhsFields;
    return this;
  }

  public JobConfigurator rhsFields(List<String> rhsFields) {
    int i = 0;
    int tableSize = 0;
    rhsPosMap = new HashMap<String, IntPair>();
    for (String field : rhsFields) {
      if (equiJoinKeys.values().contains(field) || lteJoinKeys.values().contains(field)
          || gteJoinKeys.values().contains(field) || rhsKeys.contains(field)) {
        rhsPosMap.put(field, new IntPair(i, tableSize++));
      }
      ++i;
    }
    return this;
  }

  public JobConfigurator numReduceTasks(int numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
    return this;
  }

  public void configureJob(Job job) {
    job.setNumReduceTasks(numReduceTasks);

    int i = 0;
    int kj = 0;
    int vj = 0;

    StringBuilder keySb = new StringBuilder();
    StringBuilder valSb = new StringBuilder();

    Map<String, Integer> equiLhsPosMap = new HashMap<String, Integer>();
    Map<String, Integer> lteLhsPosMap = new HashMap<String, Integer>();
    Map<String, Integer> gteLhsPosMap = new HashMap<String, Integer>();
    for (String field : lhsfields) {
      if (groupKeys.contains(field)) {
        if (kj++ > 0) {
          keySb.append(",");
        }
        keySb.append(i);
        // System.out.println(field + " => key[" + (kj - 1) + "]");
      } else {
        if (vj++ > 0) {
          valSb.append(",");
        }
        valSb.append(i);
        // System.out.println(field + " => val[" + (vj - 1) + "]");
      }
      if (equiJoinKeys.containsKey(field)) {
        equiLhsPosMap.put(field, i);
      }
      if (lteJoinKeys.containsKey(field)) {
        lteLhsPosMap.put(field, i);
      }
      if (gteJoinKeys.containsKey(field)) {
        gteLhsPosMap.put(field, i);
      }

      i++;
    }

    StringBuilder equiJoinPosMap = configString(equiLhsPosMap, equiJoinKeys);
    StringBuilder lteJoinPosMap = configString(lteLhsPosMap, lteJoinKeys);
    StringBuilder gteJoinPosMap = configString(gteLhsPosMap, gteJoinKeys);

    int rk = 0;
    StringBuilder rhsKeySb = new StringBuilder();
    for (String fieldName : rhsKeys) {
      if (rk++ > 0) {
        rhsKeySb.append(",");
      }
      rhsKeySb.append(rhsPosMap.get(fieldName).two);
    }

    job.getConfiguration().set("lhsKeys", keySb.toString());
    job.getConfiguration().set("lhsVals", valSb.toString());
    job.getConfiguration().set("rhsKeys", rhsKeySb.toString());
    job.getConfiguration().set("rhsVals", "");
    job.getConfiguration().set("eqjoin", equiJoinPosMap.toString());
    job.getConfiguration().set("ltejoin", lteJoinPosMap.toString());
    job.getConfiguration().set("gtejoin", gteJoinPosMap.toString());

    // System.out.println("eqjoin: " + equiJoinPosMap);
    // System.out.println("ltejoin: " + lteJoinPosMap);
    // System.out.println("gtejoin: " + gteJoinPosMap);

  }

  public void stripe(String row, StringBuilder data) {
    this.stripe(row, data, Constants.COL_DELIM);
  }

  // stripe-out the columns from the rhs table that are needed for the join or
  // select clauses
  public void stripe(String row, StringBuilder data, String colDelim) {
    String[] values = row.split(colDelim);
    String[] vals = new String[rhsPosMap.size()];
    // columns to be striped/collected may not be contiguously positioned in
    // the table
    for (IntPair p : rhsPosMap.values()) {
      vals[p.two] = values[p.one];
    }
    int x = 0;
    for (String val : vals) {
      if (x++ > 0) {
        data.append("\t");
      }
      data.append(val);
    }
  }

  private StringBuilder configString(Map<String, Integer> equiLhsPosMap, Map<String, String> joinKeys) {
    int rk = 0;
    StringBuilder equiJoinPosMap = new StringBuilder();
    for (Map.Entry<String, String> entry : joinKeys.entrySet()) {
      String fieldName = entry.getKey();
      if (equiLhsPosMap.containsKey(fieldName)) {
        if (rk++ > 0) {
          equiJoinPosMap.append(",");
        }
        String rField = entry.getValue();
        equiJoinPosMap.append(equiLhsPosMap.get(fieldName)).append("=").append(rhsPosMap.get(rField).two);
      }
    }
    return equiJoinPosMap;
  }

}
