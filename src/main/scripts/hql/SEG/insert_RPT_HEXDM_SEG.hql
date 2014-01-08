
set hive.auto.convert.join=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.job.queue.name=${hiveconf:job.queue};
set mapred.max.split.size=${hiveconf:split.size};
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.reduce.tasks=${hiveconf:seg.num.reduce.tasks};
-- set mapred.job.reduce.total.mem.bytes=99061748;
set hive.exec.max.created.files=10000000;

use ${hiveconf:hex.db};

insert overwrite table ${hiveconf:hex.seg.table} partition(experiment_code, version_number, variant_code)
select 
rpt_hex_seg_uuid,
segment_number,
segment_name,

experiment_name,
variant_name,
report_start_date,
report_end_date,
status,
report_transaction_end_date,
test_manager,
product_manager,
pod,
experiment_test_id,

local_date,
new_visitor_ind, 
page_assigned_entry_page_name,
site_sectn_name, 
user_cntext_name, 
browser_height, 
browser_width, 
mobile_ind, 
platform_type, 
days_until_stay, 
length_of_stay, 
number_of_rooms, 
number_of_adults_children, 
children_in_search_flag,
entry_page_name,

operating_system_name,

brwsr_name,
brwsr_typ_name,

property_typ_name,
property_parnt_chain_name,
property_brand_name,
property_super_regn_name,
property_regn_id,
property_regn_name,
property_mkt_id,
property_mkt_name,
property_sub_mkt_id,
property_sub_mkt_name,
property_cntry_name,
property_state_provnc_name,
property_city_name,
expe_half_star_rtg,
property_parnt_chain_acct_typ_name,
property_paymnt_choice_enabl_ind,
property_cntrct_model_name,
                                           
POSa_Super_Region,
POSa_Region,
POSa_Country,

mktg_chnnl_name,
mktg_sub_chnnl_name,

mktg_chnnl_name_direct,
mktg_sub_chnnl_name_direct,

hcom_srch_dest_typ_name,
hcom_srch_dest_name,
hcom_srch_dest_cntry_name,
hcom_srch_dest_id,

PSG_mkt_name,
PSG_mkt_regn_name,
PSG_mkt_super_regn_name,

dom_intl_flag,

num_unique_viewers,
num_unique_purchasers,
num_unique_cancellers,
num_active_purchasers,
num_inactive_purchasers,
total_cancellations,
net_orders,
net_bkg_gbv,
net_bkg_room_nights,
net_omniture_gbv,
net_omniture_room_nights,
net_gross_profit,
num_repeat_purchasers,
experiment_code,
version_number,
variant_code
from ${hiveconf:hex.seg.unparted.table} 
distribute by experiment_code, version_number, variant_code;
