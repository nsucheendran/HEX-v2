set mapred.job.queue.name=edwdev;
SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

insert overwrite table HEX_REPORTING_REQUIREMENTS
select experiment_code,experiment_name, variant_code, variant_name, version_number, 
FROM_UNIXTIME(UNIX_TIMESTAMP(report_start_date, "MM/dd/yyyy"), "yyyy-MM-dd") as report_start_date, 
case when report_end_date<>'' and report_end_date is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(report_end_date, "MM/dd/yyyy"), "yyyy-MM-dd") 
else '9999-99-99' end as report_end_date, status, 
case when trans_date<>'' and trans_date is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "MM/dd/yyyy"), "yyyy-MM-dd") else 
case when report_end_date<>'' and report_end_date is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(report_end_date, "MM/dd/yyyy"), "yyyy-MM-dd") 
else '9999-99-99' end end as trans_date, 
test_manager, product_manager, pod, 
experiment_test_id, 
case when last_updated_datetm<>'' and last_updated_datetm is not null 
then FROM_UNIXTIME(UNIX_TIMESTAMP(last_updated_datetm, "MM/dd/yyyy HH:mm"), "yyyy-MM-dd HH:mm") 
else null end as last_updated_datetm
from platdev.HEX_REPORTING_REQUIREMENTS_temp
where experiment_name<>'EXPERIMNT_NAME'
and experiment_code<>'' and experiment_name<>''
and variant_name<>'' and version_number is not null and report_start_date<>'' and status<>'' and status<>'Deleted';