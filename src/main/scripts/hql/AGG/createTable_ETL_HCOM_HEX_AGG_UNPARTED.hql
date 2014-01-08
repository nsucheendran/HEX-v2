use ${hiveconf:hex.db};

DROP TABLE IF EXISTS ${hiveconf:hex.agg.unparted.table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hex.agg.unparted.table} (
local_date string,
new_visitor_ind string, 
page_assigned_entry_page_name string,
site_sectn_name string, 
user_cntext_name string, 
browser_height string, 
browser_width string, 
mobile_ind string, 
platform_type string, 
days_until_stay string, 
length_of_stay string, 
number_of_rooms string, 
number_of_adults_children string,
children_in_search_flag string,
entry_page_name string,                        

experiment_name string,
variant_name string,
report_start_date string,
report_end_date string,
status string,
report_transaction_end_date string,
test_manager string,
product_manager string,
pod string,
experiment_test_id string,

operating_system_name string,

brwsr_name string,
brwsr_typ_name string,

property_typ_name string,
property_parnt_chain_name string,
property_brand_name string,
property_super_regn_name string,
property_regn_id int,
property_regn_name string,
property_mkt_id int,
property_mkt_name string,
property_sub_mkt_id int,
property_sub_mkt_name string,
property_cntry_name string,
property_state_provnc_name string,
property_city_name string,
expe_half_star_rtg string,
property_parnt_chain_acct_typ_name string,
property_paymnt_choice_enabl_ind string,
property_cntrct_model_name string,
                                           
POSa_Super_Region string,
POSa_Region string,
POSa_Country string,

mktg_chnnl_name string,
mktg_sub_chnnl_name string,

mktg_chnnl_name_direct string,
mktg_sub_chnnl_name_direct string,

hcom_srch_dest_typ_name string,
hcom_srch_dest_name string,
hcom_srch_dest_cntry_name string,
hcom_srch_dest_id string,

PSG_mkt_name string,
PSG_mkt_regn_name string,
PSG_mkt_super_regn_name string,

dom_intl_flag string,

num_unique_viewers bigint,
num_unique_purchasers bigint,
num_unique_cancellers bigint,
num_active_purchasers bigint,
num_inactive_purchasers bigint,
total_cancellations bigint,
net_orders bigint,
net_bkg_gbv double,
net_bkg_room_nights bigint,
net_omniture_gbv double,
net_omniture_room_nights bigint,
net_gross_profit double,
num_repeat_purchasers bigint,
experiment_code string,
version_number smallint,
variant_code string
)
stored as sequencefile
LOCATION "/data/HWW/${hiveconf:hex.db}/${hiveconf:hex.agg.unparted.table}";

ALTER TABLE ${hiveconf:hex.agg.unparted.table} ENABLE NO_DROP;