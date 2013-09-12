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
insert into table ETL_HCOM_HEX_transactions PARTITION(year, month)
select case when (c44='' or c44 is null) then 'Unknown' 
else c44 end as guid, purchase_data[0] as local_date, purchase_data[1] as gmt, purchase_data[2] as gmt_timestamp, purchase_id as itin_number, 
purchase_data[3] as Omniture_GBV, 0 as BKG_GBV, purchase_data[4] as Omniture_Room_Nights, 0 as BKG_Room_Nights, 0 as Gross_Profit,
true as purchase_flag, year(purchase_data[0]) as year, month(purchase_data[0]) as month from
(select split(firstValueNSort(concat_ws("~~~", local_date, cast(gmt as string), gmt_timestamp, cast(GBV_USD_OMNITURE as string), 
      cast(Room_Nights as string)),
      gmt_timestamp, hit_data_id), "~~~") as purchase_data,
      purchase_id, case when (c44='' or c44 is null) then 'Unknown' 
      else c44 end as guid, cid
      from etl.etl_hcom_hit_data 
      where  
      local_date='2013-07-01' and   
      is_order = true
      and is_duplicate_order = false
      and is_excluded_hit = false
      group by purchase_id, c44, cid) temp;            

-- pick aggregated purchase + cancellations for a day

insert into table ETL_HCOM_HEX_transactions PARTITION(year, month)  
select case when (guid='' or guid is null) then 'Unknown' 
else guid end as guid,
FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "yyyyMMdd"), "yyyy-MM-dd") as local_date, unix_timestamp(trans_date, "yyyyMMdd") as gmt, 
firstValueNSort(gmt_trans_datetm, gmt_trans_datetm) as gmt_datetm, itin_number, 0 as Omniture_GBV, sum(GROSS_BKG_AMT_USd) as BKG_GBV, 
0 as Omniture_Room_Nights, sum(RM_NIGHT_CNT) as BKG_Room_Nights, sum(gross_profit_amt_usd) as Gross_Profit,
case when cancel_count=1 then false else true end as purchase_flag, substr(trans_date,1,4) as year, cast(substr(trans_date, 5, 2) as smallint) as month 
from platdev.ETLDM_HCOM_BKG_ORDER_XREF_final1 
where year=2013 and month='2' and
                trans_date='20130201' 
                group by itin_number, guid, cancel_count, trans_date;