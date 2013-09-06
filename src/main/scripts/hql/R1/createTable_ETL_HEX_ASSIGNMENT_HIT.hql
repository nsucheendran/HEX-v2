use platdev;
drop table if exists ETL_HCOM_HEX_ASSIGNMENT_HIT;

create table ETL_HCOM_HEX_ASSIGNMENT_HIT
(guid string,
 cid int,
 experiment_variant_code string,
 local_date string,
 gmt int,
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
 property_destination_id int,
 platform_type string,
 day_until_stay int,
 length_of_stay int,
 number_of_rooms int,
 number_of_adults int,
 number_of_children int,
 children_in_search int
 )
partitioned by (year int, month int)
stored as RCFILE;