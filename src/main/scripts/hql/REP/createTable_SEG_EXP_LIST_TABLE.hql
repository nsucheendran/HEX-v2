-- contains list of experiment,version_num,variants for which data is loaded in segmented table

use ${hiveconf:hex.db};

drop table if exists ${hiveconf:hex.table};

create table ${hiveconf:hex.table} (
experiment_code string,
experiment_name string,
version_number smallint,
variant_code string,
variant_name string,
status string,
experiment_test_id string,
test_manager string,
product_manager string,
pod string,
report_start_date string,
report_end_date string,
latest_local_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
stored as textfile
LOCATION "/data/HWW/${hiveconf:hex.db}/${hiveconf:hex.table}";

ALTER TABLE ${hiveconf:hex.table} ENABLE NO_DROP;