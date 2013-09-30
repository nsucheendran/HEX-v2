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

insert ${hiveconf:into.overwrite} table ${hiveconf:hex.trans.table} PARTITION(year_month)
           select val[2] as guid, 
                  local_date, 
                  val[1] as gmt, 
                  val[0] as gmt_datetm, 
                  itin_number, 
                  0 as Omniture_GBV, 
                  BKG_GBV, 
                  0 as Omniture_Room_Nights, 
                  BKG_Room_Nights, 
                  Gross_Profit, 
                  purchase_flag,
                  substr(local_date, 1, 7) as year_month
             from (  select FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "yyyyMMdd"), "yyyy-MM-dd") as local_date,
                            split(firstValueNSort(concat_ws("~~~", 
                                                            gmt_trans_datetm, 
                                                            cast(UNIX_TIMESTAMP(gmt_trans_datetm, "yyyy-MM-dd-HH.mm.ss") as string), 
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
                       from ${hive.fah.db}.ETLDM_HCOM_BKG_ORDER_XREF_HEX
                      where trans_date between '${hiveconf:start.date}' and '${hiveconf:end.date}'
                        and year_month = '${hiveconf:month}'
                        and guid is not null and guid<>''
                   group by itin_number, cancel_count, trans_date) temp;

