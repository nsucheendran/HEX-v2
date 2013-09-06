use platdev;
drop table if exists ETL_HCOM_HEX_transactions;

create table ETL_HCOM_HEX_transactions (
  guid string,
  local_date string,
  gmt int,
  gmt_datetm string,
  itin_number string,
  GBV double,
  Room_Nights smallint,
  Gross_Profit decimal,
  purchase_flag boolean
  ) partitioned by (year smallint, month smallint);