Omniture_bookmark_date='2013-10-08.57.00.000000' (R1 & R2 common bookmark)
Booking_bookmark_date='2012-10-07' (R3 bookmark)
-- omniture's last transaction considered timestamp
src_bookmark_omni='2012-10-31' (after run, update to {Omniture_bookmark_date})
-- bkg_xref's last transaction considered timestamp
src_bookmark_bkg='2012-10-31' (after run, update to min{Omniture_bookmark_date, Booking_bookmark_date})
min_src_bookmark=min(src_bookmark_omni,src_bookmark_bkg)
min_report_date='2012-11-01' (min report_start_date of currently active reporting requirements)
min_report_date_yrmonth=year-month(min_report_date)
max_report_date='2013-10-08' (max report_end_date of currently active reporting requirements)
max_trans_record_date=max(max_omniture_record_date, max_booking_record_date)
max_trans_record_date_yr_month=year-month(max_trans_record_date)
max_omniture_record_date=min(max_report_date, Omniture_bookmark_date)
max_booking_record_date=min(max_report_date, Booking_bookmark_date)
max_omniture_record_yr_month=year-month(max_omniture_record_date)
max_booking_record_yr_month=year-month(max_booking_record_date)

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
set mapred.job.queue.name=${job.queue}

set min_report_date='2013-07-10';
set min_report_date_yrmonth='2013-07';
set max_report_date='2013-08-27';
set max_report_date_yrmonth='2013-08';
set max_omniture_record_date='2013-08-27';
set max_booking_record_date='2013-08-27';
set max_omniture_record_yr_month='2013-08';
set max_booking_record_yr_month='2013-08';
set max_trans_record_date='2013-08-27';
set max_trans_record_date_yr_month='2013-08'; 
set min_src_bookmark='2012-10-31'; 
set src_bookmark_omni='2013-10-13';
set src_bookmark_bkg='2013-10-13';

use ${hiveconf:hex.db};

insert into table ${hiveconf:hex.table} partition(year_month, source)
          select case when purchase_flag=false then 0-num_transactions
                      when purchase_flag=true then num_transactions
                      else cast(0 as bigint)
                 end as num_transactions,
                 bkg_gbv,
                 bkg_room_nights,
                 gross_profit,
                 guid,
                 cid,
                 itin_number,
                 local_date,
                 trans_date,
                 rep_variant_code as variant_code,
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
                 substr(local_date, 1, 7) as year_month,
                 case when source is null then 'omniture' else source end as source
            from (          select count(1) as num_transactions,
                                   hits_by_report.rep_variant_code,
                                   experiment_code,
                                   version_number,
                                   trans.guid,
                                   cid,
                                   hits_by_report.local_date as local_date, 
                                   trans.purchase_flag,
                                   trans.trans_date as trans_date,
                                   sum(bkg_gbv) as bkg_gbv,
                                   sum(bkg_room_nights) as bkg_room_nights,
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
                                                     gmt,
                                                     bkg_gbv,
                                                     bkg_room_nights,
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
                              join (          select /*+ MAPJOIN(rep) */ cid,
                                                     rep_variant_code,
                                                     experiment_code,
                                                     version_number,
                                                     guid,
                                                     local_date,
                                                     gmt,
                                                     report_start_date,
                                                     report_end_date,
                                                     last_updated_dt, 
                                                     case when trans_date is null or trans_date='' then report_end_date else trans_date end as trans_date,
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
                                                from (          select guid,
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
                                          inner join (          select variant_code,
                                                                       experiment_code,
                                                                       version_number,
                                                                       report_start_date,
                                                                       case when report_end_date is null or report_end_date='' then '9999-99-99' else report_end_date end as report_end_date,
                                                                       last_updated_dt,
                                                                       trans_date
                                                                  from hwwdev.HEX_REPORTING_REQUIREMENTS
                                                                 where last_updated_dt>='${hiveconf:min_src_bookmark}'
                                                                    or (    report_start_date<='${hiveconf:min_src_bookmark}'
                                                                        and report_end_date>='${hiveconf:min_src_bookmark}')
                                                     ) rep
                                                  on first_hits.experiment_variant_code=rep.variant_code
                                                  where first_hits.local_date>=rep.report_start_date
                                                    and first_hits.local_date<=rep.report_end_date
                                   ) hits_by_report
                                on hits_by_report.guid=trans.guid
                             where (    trans.trans_date<=hits_by_report.trans_date
                                    and hits_by_report.gmt<=trans.gmt
                                    and (    (    hits_by_report.last_updated_dt>='${hiveconf:min_src_bookmark}'
                                              and trans.trans_date>=hits_by_report.report_start_date
                                             )
                                          or (    (    hits_by_report.last_updated_dt=''
                                                    or hits_by_report.last_updated_dt is null
                                                  ) 
                                              and (    (    trans.source='booking'
                                                        and trans.trans_date>'${hiveconf:src_bookmark_bkg}'
                                                       ) 
                                                    or (    trans.source='omniture' 
                                                        and trans.trans_date>'${hiveconf:src_bookmark_omni}'
                                                       )
                                                  )
                                             )
                                        )
                                   )
                          group by hits_by_report.rep_variant_code,
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
