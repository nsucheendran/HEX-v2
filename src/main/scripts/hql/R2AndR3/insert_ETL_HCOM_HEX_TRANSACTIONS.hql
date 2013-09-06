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

-- add new purchases on a day, incrementally
insert into table ETL_HCOM_HEX_transactions PARTITION(year, month)
select guid, purchase_data[0] as local_date, purchase_data[1] as gmt, purchase_data[2] as gmt_timestamp, purchase_id as itni_number, 
purchase_data[3] as gbv, purchase_data[4] as Room_Nights, purchase_data[5] as Gross_Profit, 
true as purchase_flag, year(purchase_data[0]) as year, month(purchase_data[0]) as month
      from ( select split(firstValueNSort(concat_ws("~~~", t1.local_date, cast(t1.gmt as string), t1.gmt_timestamp, cast(t1.GBV_USD_OMNITURE as string), 
      cast(t1.Room_Nights as string), cast(t2.GROSS_BKG_AMT_USD as string)),
      t1.gmt_timestamp, t1.hit_data_id), "~~~") as purchase_data,
      purchase_id, coalesce(c44, 'Unknown') as guid, cid
      from etl.etl_hcom_hit_data t1 left outer join v_etldm_hcom_bkg_order_xref_final t2
      on (t2.gmt_bk_date=t1.local_date and t1.purchase_id=t2.omniture_purchase_id)
      where t1.local_date='2013-07-01' 
      and is_order = true
      and is_duplicate_order = false
      and is_excluded_hit = false
      group by purchase_id, c44, cid
)temp;

-- add new cancellations on a day, incrementally
insert into table ETL_HCOM_HEX_transactions PARTITION(year, month)
select guid, purchase_data[0] as local_date, unix_timestamp(purchase_data[1]) as gmt, purchase_data[1] as gmt_datetm, itin_number, 
purchase_data[2] as gbv, purchase_data[3] as Room_Nights, purchase_data[4] as Gross_Profit, 
false as purchase_flag, year(purchase_data[0]) as year, month(purchase_data[0]) as month from 
(select split(firstValueNSort(concat_ws("~~~", trans_date, cast(gmt_trans_datetm as string),
cast(GROSS_BKG_AMT_USd as string), cast(RM_NIGHT_CNT as string), 
cast(gross_profit_amt_usd as string)), 
gmt_trans_datetm), "~~~") purchase_data, 
itin_number, guid
from platdev.v_etldm_hcom_bkg_order_xref_final 
where cancel_count = 1
                and trans_date>='20130501' and trans_date<='20130531' 
                group by itin_number, guid) temp1;  