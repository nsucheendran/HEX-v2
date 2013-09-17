use hwwdev;
drop table if exists ETL_HCOM_HEX_transactions;

create table ETL_HCOM_HEX_transactions (
  guid string,
  local_date string,
  gmt bigint,
  gmt_datetm string,
  itin_number string, 
  Omniture_GBV double,
  BKG_GBV double,
  Omniture_Room_Nights smallint,
  BKG_Room_Nights smallint,
  Gross_Profit decimal,
  purchase_flag boolean
  ) partitioned by (year_month string);