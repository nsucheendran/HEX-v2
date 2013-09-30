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
                 all_hits.min_hit_data[22] as all_mktg_seo,
                 all_hits.min_hit_data[23] as all_mktg_seo_direct,
                 all_hits.min_hit_data[24] as entry_page_name,
                 all_hits.min_hit_data[25] as supplier_property_id,
                 substr(all_hits.min_hit_data[0], 1, 7) as year_month
            from (      select split(firstValueNSort(concat_ws("~~~", 
                                                               local_date, 
                                                               cast(gmt as string), 
                                                               cast(gmt_timestamp as string), 
                                                               cast(hit_data_id as string), 
                                                               cast(new_visitor_ind as string), 
                                                               page_assigned_entry_page_name, 
                                                               site_sectn_name,
                                                               user_cntext_name,
                                                               cast(browser_height as string),
                                                               cast(browser_width as string),
                                                               cast(brwsr_id as string),
                                                               case when c302 is null then 'Non Mobile' else 'Mobile' end, 
                                                               cast(destination_id as string), 
                                                               cast(property_destination_id as string), 
                                                               c277, 
                                                               cast(DAYS_UNTIL_STAY as string), 
                                                               cast(LENGTH_OF_STAY as string), 
                                                               cast(NUMBER_OF_ROOMS as string), 
                                                               cast(NUMBER_OF_ADULTS as string), 
                                                               cast(NUMBER_OF_CHILDREN as string), 
                                                               cast(CHILDREN_IN_SEARCH as string), 
                                                               c93,
                                                               all_mktg_seo,
                                                               all_mktg_seo_direct,
                                                               c104,
                                                               supplier_property_id
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

