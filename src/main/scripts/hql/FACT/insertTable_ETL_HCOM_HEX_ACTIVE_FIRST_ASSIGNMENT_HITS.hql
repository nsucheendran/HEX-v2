
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
       supplier_property_id
from (
           select guid,
                  cid,
                  local_date,
                  local_hour,
                  gmt,
                  experiment_variant_code,
                  case when new_visitor_ind = 1 then 'new'
                       when new_visitor_ind = 0 then 'return'
                       else 'Not Applicable'
                  end as new_visitor_ind,
                  page_assigned_entry_page_name, 
                  site_sectn_name,
                  user_cntext_name,
                  Case When browser_height > 0 And browser_height < 500 Then '< 500'
                       When browser_height >= 500 And browser_height < 600 Then '>=500'
                       When browser_height >= 600 And browser_height < 700 Then '>=600'
                       When browser_height >= 700 Then '>=700'
                       Else 'Not Applicable'
                  End as browser_height,
                  Case When browser_width > 0 And browser_width < 900 Then '< 900'
                       When browser_width >= 900 And browser_width < 1200 Then '>=900'
                       When browser_width >= 1200 Then '>=1200'
                       Else 'Not Applicable'
                  End as browser_width,
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
                  insert_dt,
                  trans_date
           from ${hiveconf:hex.db}.${hiveconf:hex.rep.table}
      ) rep
      on first_hits.experiment_variant_code=rep.variant_code
      where first_hits.local_date>=rep.report_start_date
            and first_hits.local_date<=rep.report_end_date;

