

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set mapred.job.queue.name=${hiveconf:job.queue};
set hive.exec.compress.output=true;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.min.split.size=${hiveconf:split.size};
set mapred.max.split.size=${hiveconf:split.size};
set hive.exec.max.created.files=10000000;

use ${hiveconf:hex.db};

insert overwrite table ${hiveconf:hex.db}.${hiveconf:hex.table} partition(experiment_code, version_number, variant_code)
    select num_unique_viewers, 
           num_unique_purchasers, 
           num_unique_cancellers, 
           num_active_purchasers, 
           num_inactive_purchasers, 
           total_cancellations, 
           net_orders, 
           net_bkg_gbv, 
           net_bkg_room_nights, 
           net_omniture_gbv, 
           net_omniture_room_nights, 
           net_gross_profit, 
           num_repeat_purchasers, 
           cid, 
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
           experiment_name, 
           variant_name, 
           status, 
           experiment_test_id, 
           experiment_code, 
           version_number, 
           variant_code 
      from ETL_HCOM_HEX_FACT_UNPARTED;
      