add jar hex-etl-hadoop-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION firstValueNSort AS 'udaf.GenericUDAFFirstValueNValueSort';

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set mapred.job.queue.name=edwdev;
SET hive.exec.compress.output=true;
SET mapred.max.split.size=256000000;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

use platdev;

insert into table platdev.ETL_HCOM_HEX_ASSIGNMENT_HIT PARTITION(year, month)
  select temp.guid,
         temp.cid,
         temp.test_variant_code as experiment_variant_code,
         temp.min_hit_data[0] as local_date,
         temp.min_hit_data[1] as gmt,
         temp.min_hit_data[2] as hit_data_id,
         temp.min_hit_data[3] as new_visitor_ind,
         temp.min_hit_data[4] as page_assigned_entry_page_name,
         temp.min_hit_data[5] as site_sectn_name,
         temp.min_hit_data[6] as user_cntext_name,
         temp.min_hit_data[7] as browser_height,
         temp.min_hit_data[8] as browser_width,
         temp.min_hit_data[9] as brwsr_id,
         temp.min_hit_data[10] as mobile_ind,
         temp.min_hit_data[11] as destination_id,
         temp.min_hit_data[12] as property_destination_id,
         year(temp.min_hit_data[0]) as year,
         month(temp.min_hit_data[0]) as month
    from 
    (select split(firstValueNSort(concat_ws("$$$", local_date, cast(gmt as string), cast(hit_data_id as string), cast(new_visitor_ind as string), page_assigned_entry_page_name,
                     site_sectn_name,user_cntext_name,cast(browser_height as string),cast(browser_width as string),cast(brwsr_id as string),c302,cast(destination_id as string),
                     cast(property_destination_id as string)),  gmt, visit_page_number),"$$$") min_hit_data,
                                    cid,
                                    test_variant_code,
                                    coalesce(c44, 'Unknown') as guid
                               from etl.etl_hcom_hit_data LATERAL VIEW explode(split(concat_ws(',',c154,c281),',')) tt as test_variant_code
                              where test_variant_code <> ''
                                and local_date = '${hiveconf:local.date}' 
                           group by cid, test_variant_code, c44) temp 
                           left outer join
                           platdev.ETL_HCOM_HEX_ASSIGNMENT_HIT test1 
                       on (temp.guid = test1.guid
                       and temp.test_variant_code = test1.experiment_variant_code
                       and temp.cid = test1.cid)
                       where test1.guid is null;