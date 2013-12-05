use hwwdev;

drop table if exists hwwdev.hex_fact_adi;

create table hwwdev.dm_hcom_hex_fact(
num_unique_viewers bigint,
num_unique_purchasers bigint,
num_unique_cancellers bigint,
num_active_purchasers bigint,
num_nil_net_order_purchasers bigint,
total_cancellations bigint,
net_orders bigint,
net_bkg_gbv double,
net_bkg_room_nights bigint,
net_omniture_gbv double,
net_omniture_room_nights bigint,
net_gross_profit double,
num_repeat_purchasers bigint,
cid int, 
local_date string,
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
days_until_stay int,
length_of_stay int,
number_of_rooms int,
number_of_adults int,
number_of_children int,
children_in_search int,
operating_system_id smallint,
all_mktg_seo_30_day string,
all_mktg_seo_30_day_direct string,
entry_page_name string,
supplier_property_id int,
experiment_name string,
variant_name string,
status string,
experiment_test_id string
) partitioned by (
variant_code string,
experiment_code string,
version_number smallint
) 
stored as sequencefile
location "/tmp/hdp_hww_hex_etl_fact_aggregation/dm_hcom_hex_fact";