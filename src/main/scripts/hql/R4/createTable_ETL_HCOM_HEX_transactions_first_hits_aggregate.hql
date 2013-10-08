use platdev;

create table ETL_HCOM_HEX_transactions_first_hits_aggregate(
num_transactions int,
bkg_gbv double,
bkg_room_nights smallint,
gross_profit decimal,
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
entry_page_name string
) 
partitioned by(year_month string, source string)
stored as sequencefile;