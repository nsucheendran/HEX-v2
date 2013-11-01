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

-- populate 1st assignment hits for the active reporting requirements in the reporting range
insert overwrite table ${hiveconf:rep.first.hits.hex.table} partition(year_month)
select cid,
       variant_code,
       experiment_code,
       version_number,
       guid,
       report_start_date,
       report_end_date,
       last_updated_dt, 
       trans_date,
       cast(min_hit_data[0] as bigint) as gmt,
       min_hit_data[1] as local_date,
       min_hit_data[2] as new_visitor_ind,
       min_hit_data[3] as page_assigned_entry_page_name, 
       min_hit_data[4] as site_sectn_name,
       min_hit_data[5] as user_cntext_name,
       min_hit_data[6] as browser_height,
       min_hit_data[7] as browser_width,
       min_hit_data[8] as brwsr_id,
       min_hit_data[9] as mobile_ind,
       min_hit_data[10] as destination_id,
       min_hit_data[11] as property_destination_id,
       min_hit_data[12] as platform_type, 
       min_hit_data[13] as days_until_stay,
       min_hit_data[14] as length_of_stay,
       min_hit_data[15] as number_of_rooms,
       min_hit_data[16] as number_of_adults,
       min_hit_data[17] as number_of_children,
       min_hit_data[18] as children_in_search,
       min_hit_data[19] as operating_system_id,
       min_hit_data[20] as all_mktg_seo_30_day,
       min_hit_data[21] as all_mktg_seo_30_day_direct,
       min_hit_data[22] as entry_page_name,
       min_hit_data[23] as supplier_property_id,
       substr(min_hit_data[1], 1, 7) as year_month
from (
           select /*+ MAPJOIN(rep) */ cid,
                  variant_code,
                  experiment_code,
                  version_number,
                  guid,
                  report_start_date, 
                  report_end_date,
                  last_updated_dt, 
                  trans_date,
                  split(firstValueNSort(concat_ws("~~~", 
                                                        cast(gmt as string),
                                                        local_date,
                                                        cast(new_visitor_ind as string),
                                                        page_assigned_entry_page_name, 
                                                        site_sectn_name,
                                                        user_cntext_name,
                                                        cast(browser_height as string),
                                                        cast(browser_width as string),
                                                        cast(brwsr_id as string),
                                                        mobile_ind,
                                                        cast(destination_id as string),
                                                        cast(property_destination_id as string),
                                                        platform_type, 
                                                        cast(days_until_stay as string),
                                                        cast(length_of_stay as string),
                                                        cast(number_of_rooms as string),
                                                        cast(number_of_adults as string),
                                                        cast(number_of_children as string),
                                                        cast(children_in_search as string),
                                                        cast(operating_system_id as string),
                                                        all_mktg_seo_30_day,
                                                        all_mktg_seo_30_day_direct,
                                                        entry_page_name,
                                                        cast(supplier_property_id as string)
                                                 ), gmt
                                        ),"~~~"
                       ) as min_hit_data 
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
                              reporting_variant_code,
                              version_number,
                              report_start_date,
                              report_end_date,
                              last_updated_dt,
                              trans_date
                       from hwwdev.HEX_REPORTING_REQUIREMENTS
                ) rep
           on first_hits.experiment_variant_code=rep.reporting_variant_code
           where first_hits.local_date>=rep.report_start_date
                 and first_hits.local_date<=rep.report_end_date
           group by guid, cid, variant_code,experiment_code,
                    version_number, report_start_date, report_end_date, 
                    last_updated_dt, trans_date  
     ) hits_by_report;  

-- pick first assignment hits incrementally
insert into table ${hiveconf:hex.table} partition(year_month, source)
select guid,
       cid,
       null as itin_number,
       local_date,
       null as trans_date,
       variant_code,
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
       operating_system,
       all_mktg_seo,
       all_mktg_seo_direct,
       entry_page_name,
       supplier_property_id,
       0 as num_transactions,
       0 as bkg_gbv,
       0 as bkg_room_nights,
       0 as omniture_gbv,
       0 as omniture_room_nights,
       0 as gross_profit,
       substr(local_date, 1, 7) as year_month,
       'omniture' as source 
from ${hiveconf:rep.first.hits.hex.table} 
where (    
           last_updated_dt>='${hiveconf:src_bookmark_omni}'
      )
      or 
      (    
           (   last_updated_dt is null
               or last_updated_dt<='${hiveconf:min_src_bookmark}'
           ) 
           and 
           (    
               local_date>'${hiveconf:src_bookmark_omni}'
           )
      );
      

insert into table ${hiveconf:hex.table} partition(year_month, source)
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
                 operating_system,
                 all_mktg_seo,
                 all_mktg_seo_direct,
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
                                   operating_system,
                                   all_mktg_seo,
                                   all_mktg_seo_direct,
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
                              join ETL_HCOM_HEX_REP_FIRST_HITS hits_by_report
                                    on hits_by_report.guid=trans.guid
                                    where (    trans.trans_date<=hits_by_report.trans_date
                                               and hits_by_report.gmt<=trans.gmt
                                               and (    
                                                      (    hits_by_report.last_updated_dt>='${hiveconf:min_src_bookmark}'
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
                                             operating_system,
                                             all_mktg_seo,
                                             all_mktg_seo_direct,
                                             entry_page_name,
                                             supplier_property_id,
                                             trans.itin_number
                 ) rep_hit_trans;
