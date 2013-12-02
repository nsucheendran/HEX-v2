
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set hive.exec.compress.output=true;
set mapred.max.split.size=256000000;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.job.queue.name=${hiveconf:job.queue};
set hive.auto.convert.join=true;
set mapred.job.reduce.total.mem.bytes=99000000;

-- set src_bookmark_omni='${hiveconf:src_bookmark_omni}';

use ${hiveconf:hex.db};

-- insert first assignment hits incrementally
insert into table ${hiveconf:hex.db}.${hiveconf:hex.table} partition(year_month, source)
select guid,
       cid,
       null as itin_number,
       local_date,
       null as trans_date,
       variant_code,
       experiment_code,
       version_number,
       new_visitor_ind,
       page_assigned_entry_page_name,
       site_sectn_name,
       user_cntext_name,
       browser_height,
       browser_width,
       brwsr_id,
       mobile_ind,
       destination_id,
       property_destination_id,
       platform_type,
       days_until_stay,
       length_of_stay,
       number_of_rooms,
       number_of_adults,
       number_of_children,
       children_in_search,
       operating_system,
       all_mktg_seo,
       all_mktg_seo_direct,
       entry_page_name,
       supplier_property_id,
       0 as num_transactions,
       0 as bkg_gbv,
       0 as bkg_room_nights,
       0 as omniture_gbv,
       0 as omniture_room_nights,
       0 as gross_profit,
       substr(local_date, 1, 7) as year_month,
       'omniture' as source 
from ${hiveconf:hex.db}.${hiveconf:hex.rep.table} 
where (    
           last_updated_dt>'${hiveconf:src_bookmark_omni}'
      )
      or 
      (    
           (   last_updated_dt is null
               or last_updated_dt<='${hiveconf:src_bookmark_omni}'
           ) 
           and 
           (    
               local_date>'${hiveconf:src_bookmark_omni}'
           )
      );

