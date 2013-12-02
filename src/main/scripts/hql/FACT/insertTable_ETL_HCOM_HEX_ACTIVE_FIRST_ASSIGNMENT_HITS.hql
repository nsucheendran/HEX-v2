
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
set mapred.job.queue.name=${job.queue};
set hive.auto.convert.join=true;
set mapred.job.reduce.total.mem.bytes=99000000;


-- set min_report_date='${hiveconf:min_report_date}';
-- set min_report_date_yrmonth='${hiveconf:min_report_date_yrmonth}';
-- max_omniture_record_date=min(max_report_date, r1_bookmark_date)
-- set max_omniture_record_date='${hiveconf:max_omniture_record_date}';
-- set max_omniture_record_yr_month='${hiveconf:max_omniture_record_yr_month}';


use ${hiveconf:hex.db};

-- populate 1st assignment hits for the active reporting requirements in the reporting range
insert overwrite table ${hiveconf:hex.db}.${hiveconf:hex.table} partition(year_month)
select /*+ MAPJOIN(rep) */ cid,
       rep.variant_code,
       experiment_code,
       version_number,
       guid,
       report_start_date,
       report_end_date,
       last_updated_dt, 
       trans_date,
       gmt,
       local_date,
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
       substr(local_date, 1, 7) as year_month
from (
           select guid,
                  cid,
                  local_date,
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
                  supplier_property_id
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
                  trans_date
           from ${hiveconf:hex.db}.${hiveconf:hex.rep.table}
      ) rep
      on first_hits.experiment_variant_code=rep.variant_code
      where first_hits.local_date>=rep.report_start_date
            and first_hits.local_date<=rep.report_end_date;

