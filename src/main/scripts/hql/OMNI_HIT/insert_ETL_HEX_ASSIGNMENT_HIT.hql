CREATE TEMPORARY FUNCTION firstValueNSort AS 'udaf.GenericUDAFFirstValueNValueSort';

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set mapred.job.queue.name=${hiveconf:job.queue};
set hive.exec.compress.output=true;
set mapred.max.split.size=256000000;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

use ${hiveconf:hex.fah.db};

insert ${hiveconf:into.overwrite} table ${hiveconf:hex.fah.table} PARTITION(year_month)
          select all_hits.guid,
                 all_hits.cid,
                 all_hits.test_variant_code as experiment_variant_code,
                 all_hits.min_hit_data[0] as local_date,
                 all_hits.min_hit_data[1] as gmt,
                 all_hits.min_hit_data[2] as gmt_timestamp,
                 all_hits.min_hit_data[3] as hit_data_id,
                 all_hits.min_hit_data[4] as new_visitor_ind,
                 all_hits.min_hit_data[5] as page_assigned_entry_page_name,
                 all_hits.min_hit_data[6] as site_sectn_name,
                 all_hits.min_hit_data[7] as user_cntext_name,
                 all_hits.min_hit_data[8] as browser_height,
                 all_hits.min_hit_data[9] as browser_width,
                 all_hits.min_hit_data[10] as brwsr_id,
                 all_hits.min_hit_data[11] as mobile_ind,
                 all_hits.min_hit_data[12] as destination_id,
                 all_hits.min_hit_data[13] as property_destination_id,
                 all_hits.min_hit_data[14] as Platform_Type,
                 all_hits.min_hit_data[15] as DAYS_UNTIL_STAY,
                 all_hits.min_hit_data[16] as LENGTH_OF_STAY,
                 all_hits.min_hit_data[17] as NUMBER_OF_ROOMS,
                 all_hits.min_hit_data[18] as NUMBER_OF_ADULTS,
                 all_hits.min_hit_data[19] as NUMBER_OF_CHILDREN,
                 all_hits.min_hit_data[20] as CHILDREN_IN_SEARCH,
                 all_hits.min_hit_data[21] as operating_system,
                 all_hits.min_hit_data[22] as all_mktg_seo_30_day,
                 all_hits.min_hit_data[23] as all_mktg_seo_30_day_direct,
                 all_hits.min_hit_data[24] as entry_page_name,
                 all_hits.min_hit_data[25] as supplier_property_id,
                 substr(all_hits.min_hit_data[0], 1, 7) as year_month
            from (      select split(firstValueNSort(concat_ws("~~~", 
                                                               local_date, 
                                                               case when gmt is null then 'Unknown' else cast(gmt as string) end, 
                                                               case when gmt_timestamp is null then 'Unknown' else cast(gmt_timestamp as string) end, 
                                                               case when hit_data_id is null then 'Unknown' else cast(hit_data_id as string) end, 
                                                               case when new_visitor_ind is null then 'Unknown' else cast(new_visitor_ind as string) end, 
                                                               case when (page_assigned_entry_page_name = '' or page_assigned_entry_page_name is null) then 'Unknown' else page_assigned_entry_page_name end, 
                                                               case when (site_sectn_name = '' or site_sectn_name is null) then 'Unknown' else site_sectn_name end,
                                                               case when (user_cntext_name  = '' or user_cntext_name is null) then 'Unknown' else user_cntext_name end,
                                                               case when browser_height is null then 'Unknown' else cast(browser_height as string) end,
                                                               case when browser_width is null then 'Unknown' else cast(browser_width as string) end,
                                                               case when brwsr_id is null then 'Unknown' else cast(brwsr_id as string) end,
                                                               case when c302 is null then 'Non Mobile' else 'Mobile' end, 
                                                               case when destination_id is null then 'Unknown' else cast(destination_id as string) end, 
                                                               case when property_destination_id is null then 'Unknown' else cast(property_destination_id as string) end, 
                                                               case when (c277 = '' or c277 is null) then 'Unknown' else c277 end, 
                                                               case when DAYS_UNTIL_STAY is null then 'Unknown' else cast(DAYS_UNTIL_STAY as string) end, 
                                                               case when LENGTH_OF_STAY is null then 'Unknown' else cast(LENGTH_OF_STAY as string) end, 
                                                               case when NUMBER_OF_ROOMS is null then 'Unknown' else cast(NUMBER_OF_ROOMS as string) end, 
                                                               case when NUMBER_OF_ADULTS is null then 'Unknown' else cast(NUMBER_OF_ADULTS as string) end, 
                                                               case when NUMBER_OF_CHILDREN is null then 'Unknown' else cast(NUMBER_OF_CHILDREN as string) end, 
                                                               case when CHILDREN_IN_SEARCH is null then 'Unknown' else cast(CHILDREN_IN_SEARCH as string) end, 
                                                               case when (c93 = '' or c93 is null) then 'Unknown' else c93 end,
                                                               case when (c120 = '' or c120 is null) then 'Unknown' else upper(c120) end,
                                                               case when (c210 = '' or c210 is null) then 'Unknown' else upper(c210) end,
                                                               case when (c104 = '' or c104 is null) then 'Unknown' else c104 end,
                                                               case when (supplier_property_id = '' or supplier_property_id is null) then 'Unknown' else supplier_property_id end
                                                              ), 
                                                     gmt,
                                                     visit_page_number
                                                    ),
                                     "~~~") as min_hit_data,
                               cid,
                               test_variant_code,
                               case when (c44 = '' or c44 is null) then 'Unknown' else c44 end as guid
                          from etl.etl_hcom_hit_data LATERAL VIEW explode(split(concat_ws(',',c154,c281),',')) tt as test_variant_code
                         where test_variant_code <> '' and test_variant_code NOT like '%.UID.%'
                           and ((local_date = '${hiveconf:start.date}' and local_hour >= '${hiveconf:start.hour}') or
                                (local_date = '${hiveconf:end.date}' and local_hour <= '${hiveconf:end.hour}') or
                                (local_date > '${hiveconf:start.date}' and local_date < '${hiveconf:end.date}')
                               )
                           and is_ip_excluded = false AND is_user_agent_excluded = false and is_excluded_hit = false
                           and (length(trim(c154)) > 0 or length(trim(c281)) > 0) 
                      group by cid, test_variant_code, c44) all_hits  
 left outer join ${hiveconf:hex.fah.table} first_hits 
              on (all_hits.guid = first_hits.guid
             and all_hits.test_variant_code = first_hits.experiment_variant_code
             and all_hits.cid = first_hits.cid
             and first_hits.year_month >= ${hiveconf:start.ym})
           where first_hits.guid is null;
