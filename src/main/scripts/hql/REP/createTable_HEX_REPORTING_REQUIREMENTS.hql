-- contains raw reporting data

use ${hiveconf:hex.db}

drop table if exists HEX_REPORTING_REQUIREMENTS_RAW;

create table HEX_REPORTING_REQUIREMENTS_RAW (
experiment_code string,
experiment_name string,
variant_code string,
variant_name string,
version_number smallint,
report_start_date string,
report_end_date string,
status string,
trans_date string,
test_manager string,
product_manager string, 
pod string,
experiment_test_id string,
last_updated_datetm string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- this csv contains all variant_codes in exploded form
LOAD DATA LOCAL INPATH '${hex.report.file}' OVERWRITE INTO TABLE HEX_REPORTING_REQUIREMENTS_RAW;


drop table if exists ${hex.report.table};

create table if not exists ${hex.report.table} (
experiment_code string,
experiment_name string,
variant_code string,
variant_name string,
version_number smallint,
report_start_date string,
report_end_date string,
status string,
trans_date string,
test_manager string,
product_manager string, 
pod string,
experiment_test_id string,
last_updated_dt string)
stored as sequencefile;