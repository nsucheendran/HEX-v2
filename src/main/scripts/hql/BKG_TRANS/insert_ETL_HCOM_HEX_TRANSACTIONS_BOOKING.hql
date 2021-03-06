CREATE TEMPORARY FUNCTION firstValueNSort AS 'com.expedia.edw.hww.hex.etl.udaf.GenericUDAFFirstValueNValueSort';

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
           select val[2] as guid, 
                  local_date, 
                  cast('23' as smallint) as local_hour,
                  val[1] as gmt, 
                  val[0] as gmt_datetm, 
                  itin_number, 
                  0 as Omniture_GBV, 
                  case when (BKG_GBV is null) then 0.0 else BKG_GBV end as BKG_GBV, 
                  0 as Omniture_Room_Nights, 
                  case when (BKG_Room_Nights is null) then cast(0 as bigint) else BKG_Room_Nights end as BKG_Room_Nights, 
                  case when (Gross_Profit is null) then 0.0 else Gross_Profit end as Gross_Profit,  
                  purchase_flag,
                  substr(local_date, 1, 7) as year_month,
                  'booking' as source
             from (  select FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "yyyy-MM-dd"), "yyyy-MM-dd") as local_date,
                            split(firstValueNSort(concat_ws("~~~", 
                                                            cast(gmt_trans_datetm as string), 
                                                            cast(UNIX_TIMESTAMP(CONCAT(gmt_trans_datetm, " GMT"), "yyyy-MM-dd HH:mm:ss Z") as string), 
                                                            guid
                                                           ),
                                                  gmt_trans_datetm
                                                 ), 
                                  "~~~") as val,
                            itin_number,
                            sum(GROSS_BKG_AMT_USd) as BKG_GBV,
                            sum(RM_NIGHT_CNT) as BKG_Room_Nights, 
                            sum(gross_profit_amt_usd) as Gross_Profit,
                            case when cancel_count=1 then false else null end as purchase_flag
                       from ETLDM_HCOM_BKG_ORDER_XREF_HEX
                      where trans_date between '${hiveconf:start.date}' and '${hiveconf:end.date}'
                        and year_month = '${hiveconf:month}'
                        and guid is not null and guid<>''
                   group by itin_number, cancel_count, trans_date) temp;
