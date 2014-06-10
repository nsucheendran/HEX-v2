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

import mr.dto.TextMultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class SegmentationJobConfigurator {

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

	private int numReduceTasks = 100;
	private String colMapStr;
	private String metricsStr;
	private String segSpecStr;

	public Job initJob(Configuration config, String jobName, String queueName)
			throws IOException {
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
		job.getConfiguration()
				.setBoolean("mapreduce.map.output.compress", true);
		job.getConfiguration().set("mapreduce.map.output.compress.codec",
				"org.apache.hadoop.io.compress.SnappyCodec");
		return job;
	}

	public SegmentationJobConfigurator groupKeys(Map<String, String> groupKeys) {
		this.groupKeys = groupKeys;
		return this;
	}

	public SegmentationJobConfigurator metrics(Set<String> metrics) {
		this.metrics = metrics;
		return this;
	}

	// prepare source-target column mappings and segmentation specification for
	// sending to mappers
	public SegmentationJobConfigurator colMap(List<String> sourceFields,
			List<String> targetFields, BufferedReader segSpecReader)
			throws IOException {
		List<ColumnMapping> mappings = new ArrayList<ColumnMapping>(
				groupKeys.size());
		StringBuilder metricsSB = new StringBuilder();
		int i = 0;

		for (String targetField : targetFields) {
			if (sourceFields.contains(targetField)) {
				if (groupKeys.containsKey(targetField)) {
					mappings.add(new ColumnMapping(sourceFields
							.indexOf(targetField), groupKeys.get(targetField)));
				} else if (metrics.contains(targetField)) {
					if (i++ > 0) {
						metricsSB.append(',');
					}
					metricsSB.append(sourceFields.indexOf(targetField));
				}

			}
		}

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
				segField = segField.trim().toLowerCase();
				if (sourceFields.contains(segField)
						&& groupKeys.containsKey(segField)) {
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

	public SegmentationJobConfigurator numReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
		return this;
	}

	public void configureJob(Job job) {
		job.setNumReduceTasks(numReduceTasks);

		job.getConfiguration().set("colMap", colMapStr);
		job.getConfiguration().set("segSpecs", segSpecStr);

		job.getConfiguration().set("lhsVals", metricsStr);

	}

}
