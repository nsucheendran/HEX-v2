use hwwdev;
drop table if exists hwwdev.ETL_HCOM_HEX_ASSIGNMENT_HIT;

create table if not exists hwwdev.ETL_HCOM_HEX_ASSIGNMENT_HIT
(guid string,
 cid int,
 gmt int,
 local_date string,
 hit_data_id bigint,
 new_visitor_ind smallint,
 page_assigned_entry_page_name string,
 site_sectn_name string,
 user_cntext_name string,
 browser_height smallint,
 browser_width smallint,
 brwsr_id smallint,
 mobile_ind string,
 destination_id int,
 property_destination_id int)
partitioned by (experiment_variant_code string, local_month string)
stored as SEQUENCEFILE;

