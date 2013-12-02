set hive.auto.convert.join=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=1024;
set mapred.job.queue.name=${hiveconf:job.queue};
set hive.exec.compress.output=true;
set mapred.max.split.size=256000000;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
use hwwdev;
add jar /tmp/testProd.jar;
CREATE TEMPORARY FUNCTION randomize as 'udf.GenericUDFRandomizeInput';
set mapred.reduce.tasks=400;
set mapred.job.reduce.total.mem.bytes=99061748;

insert overwrite table ${hiveconf:hex.dim.table}
select 
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
number_of_adults, 
number_of_children, 
children_in_search,
entry_page_name,                               

active_metrics.experiment_code,
active_metrics.experiment_name,
active_metrics.variant_code,
active_metrics.variant_name,
active_metrics.version_number,
active_metrics.report_start_date,
active_metrics.report_end_date,
active_metrics.status,
active_metrics.trans_date as report_transaction_end_date,
active_metrics.test_manager,
active_metrics.product_manager,
active_metrics.pod,
active_metrics.experiment_test_id,

os.OPERATING_SYSTEMS as operating_system_name,

browsr.brwsr_name,
browsr.brwsr_typ_name,

lpd.property_typ_name, 
lpd.property_parnt_chain_name, 
lpd.property_brand_name, 
lpd.property_super_regn_name, 
lpd.property_regn_id, 
lpd.property_regn_name,
lpd.property_mkt_id, 
lpd.property_mkt_name, 
lpd.property_sub_mkt_id, 
lpd.property_sub_mkt_name, 
lpd.property_cntry_name, 
lpd.property_state_provnc_name, 
lpd.property_city_name, 
lpd.expe_half_star_rtg,
lpd.property_parnt_chain_acct_typ_name, 
lpd.property_paymnt_choice_enabl_ind, 
lpd.property_cntrct_model_name,
                                           
site.site_super_regn_name as POSa_Super_Region, 
site.site_regn_name as POSa_Region,
site.site_cntry_name as POSa_Country,

mktg.mktg_chnnl_name as mktg_chnnl_name,
mktg.mktg_sub_chnnl_name as mktg_sub_chnnl_name,

mktg_dir.mktg_chnnl_name as mktg_chnnl_name_direct,
mktg_dir.mktg_sub_chnnl_name as mktg_sub_chnnl_name_direct,

sdd.hcom_srch_dest_typ_name, 
sdd.hcom_srch_dest_name, 
sdd.hcom_srch_dest_cntry_name,
property_destination_id as hcom_srch_dest_id,

pmd.property_mkt_name as PSG_mkt_name,
pmd.property_mkt_regn_name as PSG_mkt_regn_name,
pmd.property_mkt_super_regn_name as PSG_mkt_super_regn_name,

sum(num_unique_viewers),
sum(num_unique_purchasers),
sum(num_unique_cancellers),
sum(num_active_purchasers),
sum(num_nil_net_order_purchasers),
sum(total_cancellations),
sum(net_orders),
sum(net_bkg_gbv),
sum(net_bkg_room_nights),
sum(net_omniture_gbv),
sum(net_omniture_room_nights),
sum(net_gross_profit),
sum(num_repeat_purchasers)
from                                                  
      (select cid, local_date, new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind,  
      property_destination_id, randomize(property_destination_id, ${hiveconf:hex.dim.pd.seed}, ${hiveconf:hex.dim.pd.separator}, true, ${hiveconf:hex.dim.pd.randomize.array})[0] 
      as property_destination_id_random, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,
      operating_system,all_mktg_seo,randomize(all_mktg_seo, ${hiveconf:hex.dim.mktg.seed}, ${hiveconf:hex.dim.mktg.separator}, true, 
      ${hiveconf:hex.dim.mktg.randomize.array})[0] as all_mktg_seo_random,all_mktg_seo_direct, 
      randomize(all_mktg_seo_direct, ${hiveconf:hex.dim.mktg.direct.seed}, ${hiveconf:hex.dim.mktg.direct.separator}, true, ${hiveconf:hex.dim.mktg.direct.randomize.array})[0] 
      as all_mktg_seo_direct_random,entry_page_name,supplier_property_id,
      randomize(supplier_property_id, ${hiveconf:hex.dim.sp.seed}, ${hiveconf:hex.dim.sp.separator}, true, ${hiveconf:hex.dim.sp.randomize.array})[0] as supplier_property_id_random,
      rep.variant_code,rep.experiment_code,rep.version_number,num_unique_viewers,num_unique_purchasers,num_unique_cancellers,num_active_purchasers,num_nil_net_order_purchasers,
      total_cancellations,net_orders,net_bkg_gbv,net_bkg_room_nights,net_omniture_gbv,net_omniture_room_nights,net_gross_profit,num_repeat_purchasers, experiment_name,variant_name,
      report_start_date,report_end_date,status,trans_date, test_manager,product_manager,pod,experiment_test_id from 
      (
           select cid, local_date, new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind,  
           property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,
           all_mktg_seo,all_mktg_seo_direct,entry_page_name,supplier_property_id,variant_code,experiment_code,version_number,
           num_unique_viewers,num_unique_purchasers,num_unique_cancellers,num_active_purchasers,num_nil_net_order_purchasers,total_cancellations,net_orders,net_bkg_gbv,net_bkg_room_nights,
           net_omniture_gbv,net_omniture_room_nights,net_gross_profit,num_repeat_purchasers
           from ${hiveconf:hex.fact.table}) metrics 
           inner join 
           (
               select experiment_code, experiment_name,variant_code,variant_name,version_number,report_start_date,report_end_date,status,trans_date,test_manager,product_manager,pod,
               experiment_test_id from ${hiveconf:hex.rep.table}
           ) rep 
           on 
           (
               metrics.variant_code=rep.variant_code and 
               metrics.experiment_code=rep.experiment_code and 
               metrics.version_number=rep.version_number
           )
      ) active_metrics
      left outer join 
      (
            select property_typ_name, property_parnt_chain_name, property_brand_name, property_super_regn_name, property_regn_id, property_regn_name,
            property_mkt_id, property_mkt_name, property_sub_mkt_id, property_sub_mkt_name, property_cntry_name, property_state_provnc_name, property_city_name, expe_half_star_rtg,
            property_parnt_chain_acct_typ_name, property_paymnt_choice_enabl_ind, property_cntrct_model_name, expe_lodg_property_id_random from
            (
                 select property_typ_name, property_parnt_chain_name, property_brand_name, property_super_regn_name, property_regn_id, property_regn_name,
                 property_mkt_id, property_mkt_name, property_sub_mkt_id, property_sub_mkt_name, property_cntry_name, property_state_provnc_name, property_city_name, expe_half_star_rtg,
                 property_parnt_chain_acct_typ_name, property_paymnt_choice_enabl_ind, property_cntrct_model_name, 
                 randomize(expe_lodg_property_id, ${hiveconf:hex.dim.sp.seed}, ${hiveconf:hex.dim.sp.separator}, false, ${hiveconf:hex.dim.sp.randomize.array}) as expe_lodg_property_id_arr
                 from dm.lodg_property_dim where expe_lodg_property_id<>-9998
            ) lodg_property_dim_inner
            LATERAL VIEW explode(expe_lodg_property_id_arr) tt as expe_lodg_property_id_random
       ) lpd 
       on (active_metrics.supplier_property_id_random=lpd.expe_lodg_property_id_random)
       left outer join dm.site_dim site 
       on (site.brand_id = 2 and active_metrics.cid=site.ian_business_partnr_id)
       left outer join 
       (
            select mktg_chnnl_name, mktg_sub_chnnl_name, mktg_code_random from 
            (
                 select mktg_chnnl_name, mktg_sub_chnnl_name, 
                 randomize(mktg_code, ${hiveconf:hex.dim.mktg.seed}, ${hiveconf:hex.dim.mktg.separator}, false, ${hiveconf:hex.dim.mktg.randomize.array}) mktg_code_arr
                 from hwwdev.web_analytic_mktg_code_dim_non_expedia where mktg_code<>'Unknown'
            ) web_analytic_mktg_code_dim_non_expedia_inner 
            LATERAL VIEW explode(mktg_code_arr) tt as mktg_code_random
       ) mktg 
       on (active_metrics.all_mktg_seo_random=mktg.mktg_code_random)
       left outer join 
       (
            select mktg_chnnl_name, mktg_sub_chnnl_name, mktg_code_random from 
            (
                 select mktg_chnnl_name, mktg_sub_chnnl_name, 
                 randomize(mktg_code, ${hiveconf:hex.dim.mktg.direct.seed}, ${hiveconf:hex.dim.mktg.direct.separator}, false, ${hiveconf:hex.dim.mktg.direct.randomize.array}) mktg_code_arr
                 from hwwdev.web_analytic_mktg_code_dim_non_expedia where mktg_code<>'Unknown'
            )  web_analytic_mktg_code_dim_non_expedia_inner
            LATERAL VIEW explode(mktg_code_arr) tt as mktg_code_random
       ) mktg_dir 
       on (active_metrics.all_mktg_seo_direct_random=mktg_dir.mktg_code_random)            
       left outer join 
       (
            select hcom_srch_dest_typ_name, hcom_srch_dest_name, hcom_srch_dest_cntry_name, hcom_srch_dest_property_mkt_key, hcom_srch_dest_id_random from 
            (
                select hcom_srch_dest_typ_name, hcom_srch_dest_name, hcom_srch_dest_cntry_name, hcom_srch_dest_property_mkt_key, 
                randomize(hcom_srch_dest_id, ${hiveconf:hex.dim.pd.seed}, ${hiveconf:hex.dim.pd.separator}, false, ${hiveconf:hex.dim.pd.randomize.array}) as hcom_srch_dest_id_arr
                from dm.hcom_srch_dest_dim where hcom_srch_dest_id<>-9998
            ) hcom_srch_dest_dim_inner
            LATERAL VIEW explode(hcom_srch_dest_id_arr) tt as hcom_srch_dest_id_random
       ) sdd 
       on (active_metrics.property_destination_id_random=sdd.hcom_srch_dest_id_random)
       left outer join dm.property_mkt_dim pmd 
       on (sdd.hcom_srch_dest_property_mkt_key=pmd.property_mkt_key)
       left outer join 
       (
            select OPERATING_SYSTEMS_ID, OPERATING_SYSTEMS from hwwdev.lz_hcom_dc_operating_systems
       ) os 
       on (active_metrics.operating_system=os.OPERATING_SYSTEMS_ID)
      left outer join 
      (
            select brwsr_id, brwsr_name, brwsr_typ_name from dm.brwsr_dim
      ) browsr 
      on (active_metrics.brwsr_id=browsr.brwsr_id)
      group by cid, local_date, new_visitor_ind,  page_assigned_entry_page_name,  site_sectn_name,  user_cntext_name,  browser_height,  browser_width,  mobile_ind,    
      platform_type,  days_until_stay,  length_of_stay,  number_of_rooms,  number_of_adults,  number_of_children,  children_in_search, entry_page_name, active_metrics.experiment_code, 
      active_metrics.experiment_name, active_metrics.variant_code, active_metrics.variant_name, active_metrics.version_number, active_metrics.report_start_date, 
      active_metrics.report_end_date, active_metrics.status, active_metrics.trans_date, active_metrics.test_manager, active_metrics.product_manager, active_metrics.pod, 
      active_metrics.experiment_test_id,  os.OPERATING_SYSTEMS,  browsr.brwsr_name, browsr.brwsr_typ_name,  lpd.property_typ_name, lpd.property_parnt_chain_name,  
      lpd.property_brand_name,  lpd.property_super_regn_name,  lpd.property_regn_id,  lpd.property_regn_name, lpd.property_mkt_id,  lpd.property_mkt_name,  
      lpd.property_sub_mkt_id,  lpd.property_sub_mkt_name,  lpd.property_cntry_name,  lpd.property_state_provnc_name,  lpd.property_city_name,  lpd.expe_half_star_rtg, 
      lpd.property_parnt_chain_acct_typ_name,  lpd.property_paymnt_choice_enabl_ind,  lpd.property_cntrct_model_name, site.site_super_regn_name, site.site_regn_name,
      site.site_cntry_name, mktg.mktg_chnnl_name, mktg.mktg_sub_chnnl_name,  mktg_dir.mktg_chnnl_name, mktg_dir.mktg_sub_chnnl_name,  sdd.hcom_srch_dest_typ_name,  
      sdd.hcom_srch_dest_name, sdd.hcom_srch_dest_cntry_name, property_destination_id, pmd.property_mkt_name, pmd.property_mkt_regn_name, pmd.property_mkt_super_regn_name;                                                                                                                                                    