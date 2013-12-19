SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

use ${hiveconf:hex.db};

insert overwrite table ${hiveconf:hex.db}.${hiveconf:hex.report.table}
select experiment_code,
       min(experiment_name), 
       variant_code, 
       min(variant_name), 
       version_number, 
       FROM_UNIXTIME(UNIX_TIMESTAMP(min(report_start_date), "MM/dd/yyyy"), "yyyy-MM-dd") as report_start_date, 
       case when min(report_end_date)<>'' and min(report_end_date) is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(min(report_end_date), "MM/dd/yyyy"), "yyyy-MM-dd") 
            else '9999-99-99' 
       end as report_end_date, 
       min(status), 
       case when min(trans_date)<>'' and min(trans_date) is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(min(trans_date), "MM/dd/yyyy"), "yyyy-MM-dd") else 
            case when min(report_end_date)<>'' and min(report_end_date) is not null then FROM_UNIXTIME(UNIX_TIMESTAMP(min(report_end_date), "MM/dd/yyyy"), "yyyy-MM-dd") 
                  else '9999-99-99' 
            end 
       end as trans_date, 
       min(test_manager), 
       min(product_manager), 
       min(pod), 
       min(experiment_test_id), 
       case when min(last_updated_datetm)<>'' and min(last_updated_datetm) is not null 
            then FROM_UNIXTIME(UNIX_TIMESTAMP(min(last_updated_datetm), "MM/dd/yyyy HH:mm"), "yyyy-MM-dd HH:mm") 
            else null 
       end as last_updated_dt,
       case when min(insert_datetm)<>'' and min(insert_datetm) is not null 
            then FROM_UNIXTIME(UNIX_TIMESTAMP(min(insert_datetm), "MM/dd/yyyy HH:mm"), "yyyy-MM-dd HH:mm") 
            else null 
       end as insert_dt
       from ${hiveconf:lz.db}.HEX_REPORTING_REQUIREMENTS
       where experiment_name<>'EXPERIMNT_NAME'
             and experiment_code<>'' and experiment_name<>''
             and variant_name<>'' and version_number is not null and report_start_date<>'' and status<>'' and status<>'Deleted'
             and ((case when last_updated_datetm<>'' and last_updated_datetm is not null 
                        then FROM_UNIXTIME(UNIX_TIMESTAMP(last_updated_datetm, "MM/dd/yyyy HH:mm"), "yyyy-MM-dd HH:mm") 
                        else null 
                   end) >='${hiveconf:min_src_bookmark}'
             or ((case when insert_datetm<>'' and insert_datetm is not null  
                       then FROM_UNIXTIME(UNIX_TIMESTAMP(insert_datetm, "MM/dd/yyyy HH:mm"), "yyyy-MM-dd HH:mm") 
                        else null   
                  end) >='${hiveconf:min_src_bookmark}'       
                ) 
             or (FROM_UNIXTIME(UNIX_TIMESTAMP(report_start_date, "MM/dd/yyyy"), "yyyy-MM-dd")<='${hiveconf:min_src_bookmark}'
                  and (
                       (case when report_end_date<>'' and report_end_date is not null 
                            then FROM_UNIXTIME(UNIX_TIMESTAMP(report_end_date, "MM/dd/yyyy"), "yyyy-MM-dd") 
                            else '9999-99-99' 
                       end>='${hiveconf:min_src_bookmark}')
                       or 
                       (case when trans_date<>'' and trans_date is not null 
                       then FROM_UNIXTIME(UNIX_TIMESTAMP(trans_date, "MM/dd/yyyy"), "yyyy-MM-dd")
                       else null
                       end) >='${hiveconf:min_src_bookmark}'
                      )
                )
             )
         group by experiment_code, variant_code, version_number
         having count(*)=1;