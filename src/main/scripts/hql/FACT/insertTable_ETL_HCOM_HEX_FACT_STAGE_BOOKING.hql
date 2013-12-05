
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

insert into table ${hiveconf:hex.db}.${hiveconf:hex.table} partition(year_month, source)
          select guid,
                 cid,
                 itin_number,
                 local_date,
                 trans_date,
                 variant_code as variant_code,
                 experiment_code,
                 version_number,
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
                 case when purchase_flag=false then 0-num_transactions
                      when purchase_flag=true then num_transactions
                      else cast(0 as bigint)
                 end as num_transactions,
                 bkg_gbv,
                 bkg_room_nights,
                 omniture_gbv,
                 omniture_room_nights,
                 gross_profit,
                 substr(local_date, 1, 7) as year_month,
                 case when source is null then 'omniture' else source end as source
            from (          select count(1) as num_transactions,
                                   hits_by_report.variant_code,
                                   experiment_code,
                                   version_number,
                                   hits_by_report.guid,
                                   cid,
                                   hits_by_report.local_date as local_date, 
                                   trans.purchase_flag,
                                   trans.trans_date as trans_date,
                                   sum(bkg_gbv) as bkg_gbv,
                                   sum(bkg_room_nights) as bkg_room_nights,
                                   sum(omniture_gbv) as omniture_gbv,
                                   sum(omniture_room_nights) as omniture_room_nights,
                                   sum(gross_profit) as gross_profit, 
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
                                   source,
                                   trans.itin_number
                              from (          select guid,
                                                     purchase_flag,
                                                     local_date as trans_date,
                                                     local_hour as trans_hour,
                                                     gmt,
                                                     bkg_gbv,
                                                     bkg_room_nights,
                                                     omniture_gbv,
                                                     omniture_room_nights,
                                                     gross_profit,
                                                     source,
                                                     itin_number 
                                                from ETLDATA.ETL_HCOM_HEX_TRANSACTIONS
                                               where year_month>='${hiveconf:min_report_date_yrmonth}'
                                                 and year_month<='${hiveconf:max_trans_record_date_yr_month}'
                                                 and local_date>='${hiveconf:min_report_date}'
                                                 and ((source='omniture' and local_date<='${hiveconf:max_omniture_record_date}')
                                                      or 
                                                      (source='booking' and local_date<='${hiveconf:max_booking_record_date}')
                                                     )
                                   ) trans
                              inner join ${hiveconf:hex.db}.${hiveconf:hex.active.hits.table} hits_by_report
                                    on hits_by_report.guid=trans.guid
                                    where (    trans.trans_date<=hits_by_report.trans_date
                                               and hits_by_report.gmt<=trans.gmt
                                               and (    
                                                      (    hits_by_report.last_updated_dt>'${hiveconf:min_src_bookmark}'
                                                            and trans.trans_date>=hits_by_report.report_start_date
                                                      )
                                                      or 
                                                      (    
                                                         (   hits_by_report.last_updated_dt is null
                                                             or hits_by_report.last_updated_dt<='${hiveconf:min_src_bookmark}'
                                                         ) 
                                                         and 
                                                         (    
                                                             (    trans.source='booking'
                                                                  and trans.trans_date>'${hiveconf:src_bookmark_bkg}'
                                                             ) 
                                                             or 
                                                             (    trans.source='omniture' 
                                                                     and trans.trans_date>'${hiveconf:src_bookmark_omni}'
                                                                     and trans.trans_hour>${hiveconf:src_bookmark_omni_hr}
                                                             )
                                                         )
                                                      )
                                                   )
                                          )
                                    group by hits_by_report.variant_code,
                                             experiment_code,
                                             version_number,
                                             hits_by_report.guid,
                                             cid,
                                             trans.purchase_flag,
                                             hits_by_report.local_date,
                                             trans.trans_date,
                                             source,
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
                                             trans.itin_number
                 ) rep_hit_trans;

