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

insert ${hiveconf:into.overwrite} table ${hiveconf:hex.trans.table} PARTITION(year_month, source)
           select purchase_data[4] as guid, 
                  purchase_data[5] as local_date, 
                  purchase_data[6] as local_hour, 
                  purchase_data[0] as gmt, 
                  purchase_data[1] as gmt_datetm, 
                  purchase_id as itin_number, 
                  case when (purchase_data[2] is null) then '0' else purchase_data[2] end as Omniture_GBV, 
                  0 as BKG_GBV, 
                  case when (purchase_data[3] is null) then '0' else purchase_data[3] end as Omniture_Room_Nights, 
                  0 as BKG_Room_Nights, 
                  0 as Gross_Profit,
                  true as purchase_flag, 
                  substr(purchase_data[5], 1, 7) as year_month,
                  'omniture' as source 
             from (  select split(firstValueNSort(concat_ws("~~~", 
                                                            cast(gmt as string), 
                                                            gmt_timestamp, 
                                                            cast(GBV_USD_OMNITURE as string), 
                                                            cast(Room_Nights as string), 
                                                            c44, 
                                                            local_date,
                                                            cast(local_hour as string)
                                                           ),
                                                  gmt_timestamp, 
                                                  hit_data_id
                                                 ), 
                                  "~~~"
                                 ) as purchase_data,
                            purchase_id
                       from etl.etl_hcom_hit_data 
                      where (   (    ${hiveconf:start.date} < local_date 
                                 and local_date < ${hiveconf:end.date}) 
                             or (    ${hiveconf:start.date} = ${hiveconf:end.date} 
                                 and ${hiveconf:start.date} = local_date 
                                 and ${hiveconf:start.hour} <= local_hour
                                 and local_hour <= ${hiveconf:end.hour})
                             or (    ${hiveconf:start.date} = local_date 
                                 and local_date < ${hiveconf:end.date} 
                                 and local_hour >= ${hiveconf:start.hour})   
                             or (    local_date = ${hiveconf:end.date} 
                                 and ${hiveconf:start.date} < local_date 
                                 and local_hour <= ${hiveconf:end.hour})
                            ) 
                        and c44 is not null and c44<>''
                        and is_order = true
                        and is_duplicate_order = false
                        and is_excluded_hit = false
                        and purchase_id is not null and purchase_id<>''
                   group by purchase_id) temp;