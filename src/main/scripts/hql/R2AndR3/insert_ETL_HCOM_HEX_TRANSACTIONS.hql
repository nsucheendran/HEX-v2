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

-- we've to make sure that these queries are run only once for a day, otherwise duplicate entries will get created   

-- add new purchases on a day, incrementally
insert into table hwwdev.ETL_HCOM_HEX_transactions PARTITION(year_month)
select purchase_data[4] as guid, purchase_data[5] as local_date, purchase_data[0] as gmt, purchase_data[1] as gmt_datetm, purchase_id as itin_number, 
purchase_data[2] as Omniture_GBV, 0 as BKG_GBV, purchase_data[3] as Omniture_Room_Nights, 0 as BKG_Room_Nights, 0 as Gross_Profit,
true as purchase_flag, substr(purchase_data[5], 1, 7) as year_month from
(select split(firstValueNSort(concat_ws("~~~", cast(gmt as string), gmt_timestamp, cast(GBV_USD_OMNITURE as string), 
      cast(Room_Nights as string), c44, local_date),
      gmt_timestamp, hit_data_id), "~~~") as purchase_data,
      purchase_id
      from etl.etl_hcom_hit_data 
      where  
      local_date='2013-07-01'  
      and c44 is not null and c44<>''
      and is_order = true
      and is_duplicate_order = false
      and is_excluded_hit = false
      and purchase_id is not null and purchase_id<>''
      group by purchase_id) temp;            

-- pick aggregated purchase + cancellations for a day
insert into table hwwdev.ETL_HCOM_HEX_transactions PARTITION(year_month)
select val[2] as guid, local_date, val[1] as gmt, val[0] as gmt_datetm, itin_number, 0 as Omniture_GBV, BKG_GBV, 0 as Omniture_Room_Nights, BKG_Room_Nights, Gross_Profit, purchase_flag,
substr(local_date, 1, 7) as year_month 
from
(select FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "yyyyMMdd"), "yyyy-MM-dd") as local_date, 
split(firstValueNSort(concat_ws("~~~", gmt_trans_datetm, cast(UNIX_TIMESTAMP(gmt_trans_datetm, "yyyy-MM-dd-HH.mm.ss") as string), guid),gmt_trans_datetm), "~~~") as val,
itin_number, sum(GROSS_BKG_AMT_USd) as BKG_GBV, 
sum(RM_NIGHT_CNT) as BKG_Room_Nights, sum(gross_profit_amt_usd) as Gross_Profit,
case when cancel_count=1 then false else
null end as purchase_flag
from platdev.ETLDM_HCOM_BKG_ORDER_XREF_final1 
where year=2013 and month='2' and
                trans_date='20130201' 
                and guid is not null and guid<>''
                group by itin_number, cancel_count, trans_date) temp;