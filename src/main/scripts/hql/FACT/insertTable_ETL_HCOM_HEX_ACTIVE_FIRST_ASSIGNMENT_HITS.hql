set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set hive.exec.compress.output=true;
set mapred.max.split.size=256000000;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.job.queue.name=${hiveconf:job.queue};
set hive.auto.convert.join=true;
set mapred.job.reduce.total.mem.bytes=99000000;

use ${hiveconf:hex.db};

-- populate 1st assignment hits for the active reporting requirements in the reporting range
insert overwrite table ${hiveconf:hex.db}.${hiveconf:hex.table}
select active_hits.guid,
       active_hits.cid,
       active_hits.variant_code,
       active_hits.experiment_code,
       active_hits.version_number,
       active_hits.report_start_date,
       active_hits.report_end_date,
       active_hits.last_updated_dt,
       active_hits.insert_dt,
       active_hits.trans_date,
       active_hits.local_date,
       active_hits.local_hour,
       active_hits.gmt,
       active_hits.new_visitor_ind,
       active_hits.page_assigned_entry_page_name,
       active_hits.site_sectn_name,
       active_hits.user_cntext_name,
       active_hits.browser_height,
       active_hits.browser_width,
       active_hits.brwsr_id,
       active_hits.mobile_ind,
       active_hits.destination_id,
       active_hits.property_destination_id,
       active_hits.platform_type,
       active_hits.days_until_stay,
       active_hits.length_of_stay,
       active_hits.number_of_rooms,
       active_hits.number_of_adults,
       active_hits.number_of_children,
       active_hits.children_in_search,
       active_hits.operating_system_id,
       active_hits.all_mktg_seo_30_day,
       active_hits.all_mktg_seo_30_day_direct,
       active_hits.entry_page_name,
       active_hits.supplier_property_id,
       active_hits.supplier_id,
       case when active_hits.supplier_id = -9998 or active_hits.supplier_property_id = -9998 then -1
            when lpk.lodg_property_key is null or lpk.lodg_property_key = '' then -2
            else lpk.lodg_property_key end as lodg_property_key
from (
select /*+ MAPJOIN(rep) */ guid,
       cid,
       rep.variant_code,
       experiment_code,
       version_number,
       report_start_date,
       report_end_date,
       last_updated_dt,
       insert_dt, 
       trans_date,
       local_date,
       local_hour,
       gmt,
       new_visitor_ind,
       page_assigned_entry_page_name, 
       site_sectn_name,
       user_cntext_name,
       browser_height,
       browser_width,
       brwsr_id,
       mobile_ind,
       destination_id,
       property_destination_id,
       platform_type, 
       days_until_stay,
       length_of_stay,
       number_of_rooms,
       number_of_adults,
       number_of_children,
       children_in_search,
       operating_system_id,
       all_mktg_seo_30_day,
       all_mktg_seo_30_day_direct,
       entry_page_name,
       supplier_property_id,
       supplier_id
from (
           select guid,
                  cid,
                  local_date,
                  local_hour,
                  gmt,
                  experiment_variant_code,
                  new_visitor_ind,
                  page_assigned_entry_page_name, 
                  site_sectn_name,
                  user_cntext_name,
                  browser_height,
                  browser_width,
                  brwsr_id, 
                  mobile_ind,
                  destination_id,
                  property_destination_id,
                  platform_type, 
                  days_until_stay,
                  length_of_stay,
                  number_of_rooms,
                  number_of_adults,
                  number_of_children,
                  children_in_search,
                  operating_system_id,
                  all_mktg_seo_30_day,
                  all_mktg_seo_30_day_direct,
                  entry_page_name,
                  supplier_property_id,
                  supplier_id
              from etldata.etl_hcom_hex_first_assignment_hit
              where year_month>='${hiveconf:min_report_date_yrmonth}'
                 and year_month<='${hiveconf:max_omniture_record_yr_month}'
                 and local_date<='${hiveconf:max_omniture_record_date}'
                 and local_date>='${hiveconf:min_report_date}'
     ) first_hits
     inner join
     (
           select variant_code,
                  experiment_code,
                  version_number,
                  report_start_date,
                  report_end_date,
                  last_updated_dt,
                  insert_dt,
                  trans_date
           from ${hiveconf:hex.db}.${hiveconf:hex.rep.table}
        ) rep
      on first_hits.experiment_variant_code=rep.variant_code
      where first_hits.local_date>=rep.report_start_date
      and first_hits.local_date<=rep.report_end_date
      )	active_hits
	left outer join 
	( 
		select lodg_property_key,
			supplier_id,
			supplier_property_id
			from ${hiveconf:hex.db}.${hiveconf:hex.sup.map.table}
			where current = 1 
	) lpk
	on active_hits.supplier_id=lpk.supplier_id
	and active_hits.supplier_property_id=lpk.supplier_property_id
;
