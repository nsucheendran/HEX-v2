use ${hiveconf:hex.db};

DROP TABLE IF EXISTS ${hiveconf:hex.table};

DROP TABLE IF EXISTS ${hiveconf:rep.first.hits.hex.table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:rep.first.hits.hex.table} (
cid int, 
variant_code string,
experiment_code string,
version_number smallint,
guid string,
report_start_date string,
report_end_date string,
last_updated_dt string, 
trans_date string,
gmt bigint,
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
operating_system string,
all_mktg_seo string,
all_mktg_seo_direct string,
entry_page_name string,
supplier_property_id int
) 
partitioned by(year_month string)
stored as sequencefile
LOCATION "/data/HWW/ETLDATA/${hiveconf:rep.first.hits.hex.table}";

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hex.table} (
guid string,
cid int, 
itin_number string,
local_date string,
trans_date string,
variant_code string,
experiment_code string,
version_number smallint,
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
operating_system string,
all_mktg_seo string,
all_mktg_seo_direct string,
entry_page_name string,
supplier_property_id int,
num_transactions int,
bkg_gbv double,
bkg_room_nights smallint,
omniture_gbv double,
omniture_room_nights smallint,
gross_profit decimal
) 
partitioned by(year_month string, source string)
stored as sequencefile
LOCATION "/data/HWW/ETLDATA/${hiveconf:hex.table}";

