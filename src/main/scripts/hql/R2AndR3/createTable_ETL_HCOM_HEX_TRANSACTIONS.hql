use ${hiveconf:hex.fah.db};

DROP TABLE IF EXISTS ${hiveconf:hex.trans.table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hex.trans.table} (
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
) 
PARTITIONED BY (
  year_month string,
  source string)
STORED AS RCFILE
LOCATION "/data/HWW/ETLDATA/${hiveconf:hex.trans.table}";

ALTER TABLE ${hiveconf:hex.trans.table} ENABLE NO_DROP;
