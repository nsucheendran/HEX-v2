
CREATE TEMPORARY FUNCTION randomize as 'udf.GenericUDFRandomizeInput';

set hive.auto.convert.join=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.job.queue.name=${hiveconf:job.queue};
set mapred.max.split.size=256000000;
set mapred.compress.map.output=true;
set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.reduce.tasks=${hiveconf:agg.num.reduce.tasks};
-- set mapred.job.reduce.total.mem.bytes=99061748;
set hive.exec.max.created.files=10000000;

use ${hiveconf:hex.db};

insert overwrite table ${hiveconf:hex.agg.table} PARTITION (experiment_code,version_number,variant_code)
          select local_date,
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

                 active_metrics.experiment_name,
                 active_metrics.variant_name,
                 active_metrics.report_start_date,
                 active_metrics.report_end_date,
                 active_metrics.status,
                 active_metrics.trans_date as report_transaction_end_date,
                 case when active_metrics.test_manager is null then 'Unknown' else active_metrics.test_manager end as test_manager,
                 case when active_metrics.product_manager is null then 'Unknown' else active_metrics.product_manager end as product_manager,
                 case when active_metrics.pod is null then 'Unknown' else active_metrics.pod end as pod,
                 case when active_metrics.experiment_test_id is null then 'Unknown' else active_metrics.experiment_test_id end as experiment_test_id,

                 case when os.OPERATING_SYSTEMS is null then 'Unknown' else os.OPERATING_SYSTEMS end as operating_system_name,

                 case when browsr.brwsr_name is null then 'Unknown' else browsr.brwsr_name end as brwsr_name,
                 case when browsr.brwsr_typ_name is null then 'Unknown' else browsr.brwsr_typ_name end as brwsr_typ_name,

                 case when lpd.property_typ_name is null then 'Unknown' else lpd.property_typ_name end as property_typ_name, 
                 case when lpd.property_parnt_chain_name is null then 'Unknown' else lpd.property_parnt_chain_name end as property_parnt_chain_name, 
                 case when lpd.property_brand_name is null then 'Unknown' else lpd.property_brand_name end as property_brand_name, 
                 case when lpd.property_super_regn_name is null then 'Unknown' else lpd.property_super_regn_name end as property_super_regn_name, 
                 case when lpd.property_regn_id is null then -9998 else lpd.property_regn_id end as property_regn_id, 
                 case when lpd.property_regn_name is null then 'Unknown' else lpd.property_regn_name end as property_regn_name,
                 case when lpd.property_mkt_id is null then -9998 else lpd.property_mkt_id end as property_mkt_id, 
                 case when lpd.property_mkt_name is null then 'Unknown' else lpd.property_mkt_name end as property_mkt_name, 
                 case when lpd.property_sub_mkt_id is null then -9998 else lpd.property_sub_mkt_id end as property_sub_mkt_id, 
                 case when lpd.property_sub_mkt_name is null then 'Unknown' else lpd.property_sub_mkt_name end as property_sub_mkt_name, 
                 case when lpd.property_cntry_name is null then 'Unknown' else lpd.property_cntry_name end as property_cntry_name, 
                 case when lpd.property_state_provnc_name is null then 'Unknown' else lpd.property_state_provnc_name end as property_state_provnc_name, 
                 case when lpd.property_city_name is null then 'Unknown' else lpd.property_city_name end as property_city_name, 
                 case when lpd.expe_half_star_rtg is null then 'Unknown' else cast(lpd.expe_half_star_rtg as string) end as expe_half_star_rtg,
                 case when lpd.property_parnt_chain_acct_typ_name is null then 'Unknown' else lpd.property_parnt_chain_acct_typ_name end as property_parnt_chain_acct_typ_name, 
                 case when lpd.property_paymnt_choice_enabl_ind is null then 'Unknown' else lpd.property_paymnt_choice_enabl_ind end as property_paymnt_choice_enabl_ind, 
                 case when lpd.property_cntrct_model_name is null then 'Unknown' else lpd.property_cntrct_model_name end as property_cntrct_model_name,
                                           
                 case when site.site_super_regn_name is null then 'Unknown' else site.site_super_regn_name end as POSa_Super_Region, 
                 case when site.site_regn_name is null then 'Unknown' else site.site_regn_name end as POSa_Region,
                 case when site.site_cntry_name is null then 'Unknown' else site.site_cntry_name end as POSa_Country,

                 case when mktg.mktg_chnnl_name is null then 'Unknown' else mktg.mktg_chnnl_name end as mktg_chnnl_name,
                 case when mktg.mktg_sub_chnnl_name is null then 'Unknown' else mktg.mktg_sub_chnnl_name end as mktg_sub_chnnl_name,

                 case when mktg_dir.mktg_chnnl_name is null then 'Unknown' else mktg_dir.mktg_chnnl_name end as mktg_chnnl_name_direct,
                 case when mktg_dir.mktg_sub_chnnl_name is null then 'Unknown' else mktg_dir.mktg_sub_chnnl_name end as mktg_sub_chnnl_name_direct,

                 case when sdd.hcom_srch_dest_typ_name is null then 'Unknown' else sdd.hcom_srch_dest_typ_name end as hcom_srch_dest_typ_name, 
                 case when sdd.hcom_srch_dest_name is null then 'Unknown' else sdd.hcom_srch_dest_name end as hcom_srch_dest_name, 
                 case when sdd.hcom_srch_dest_cntry_name is null then 'Unknown' else sdd.hcom_srch_dest_cntry_name end as hcom_srch_dest_cntry_name,
                 case when property_destination_id is null then 'Unknown' else cast(property_destination_id as string) end as hcom_srch_dest_id,

                 case when pmd.property_mkt_name is null then 'Unknown' else pmd.property_mkt_name end as PSG_mkt_name,
                 case when pmd.property_mkt_regn_name is null then 'Unknown' else pmd.property_mkt_regn_name end as PSG_mkt_regn_name,
                 case when pmd.property_mkt_super_regn_name is null then 'Unknown' else pmd.property_mkt_super_regn_name end as PSG_mkt_super_regn_name,

                 case when coalesce(lpd.property_cntry_name,sdd.hcom_srch_dest_cntry_name) is null then 'Not Applicable'
                      when coalesce(property_cntry_name,hcom_srch_dest_cntry_name) = site.site_cntry_name then 'Domestic'
                      else 'International'
                  end as dom_intl_flag,

                 sum(num_unique_viewers),
                 sum(num_unique_purchasers),
                 sum(num_unique_cancellers),
                 sum(num_active_purchasers),
                 sum(num_inactive_purchasers),
                 sum(total_cancellations),
                 sum(net_orders),
                 sum(net_bkg_gbv),
                 sum(net_bkg_room_nights),
                 sum(net_omniture_gbv),
                 sum(net_omniture_room_nights),
                 sum(net_gross_profit),
                 sum(num_repeat_purchasers),
                 active_metrics.experiment_code,
                 active_metrics.version_number,
                 active_metrics.variant_code
            from (   select cid,
                            local_date, 
                            case when new_visitor_ind = 1 then 'new'
                                 when new_visitor_ind = 0 then 'return'
                                 else 'Not Applicable'
                             end as new_visitor_ind,
                            page_assigned_entry_page_name, 
                            site_sectn_name, 
                            user_cntext_name, 
                            case when browser_height > 0 and browser_height < 500 then '< 500'
                                 when browser_height >= 500 and browser_height < 600 then '>=500'
                                 when browser_height >= 600 and browser_height < 700 then '>=600'
                                 when browser_height >= 700 then '>=700'
                                 else 'Not Applicable'
                             end as browser_height,
                            case when browser_width > 0 and browser_width < 900 then '< 900'
                                 when browser_width >= 900 and browser_width < 1200 then '>=900'
                                 when browser_width >= 1200 then '>=1200'
                                 else 'Not Applicable'
                             end as browser_width, 
                            brwsr_id, 
                            mobile_ind,  
                            property_destination_id, 
                            randomize(property_destination_id, ${hiveconf:hex.agg.seed}, "###", true, ${hiveconf:hex.agg.pd.randomize.array})[0] as property_destination_id_random, 
                            platform_type, 
                            case when days_until_stay <=1 then 'Last Minute (0 -1)'
                                 when days_until_stay between 2 and 13 then '2 to 13 Days'
                                 when days_until_stay >=14 then 'Early Booker (>=14 Days)'
                                 else 'Unknown'
                             end as days_until_stay, 
                            case when length_of_stay <=4 then 'Short Break ( <= 4)'
                                 when length_of_stay >=5 then 'Long Break (>= 5)'
                                 else 'Unknown'
                             end as length_of_stay, 
                            case when number_of_rooms =1 then 'Single Room'
                                 when number_of_rooms >=2 then 'More than One Room'
                                 else 'Unknown'
                             end as number_of_rooms, 
                            case when coalesce(number_of_adults,0)=1 and coalesce(number_of_children,0) = 0 then '1 Adult, No Children'
                                 when coalesce(number_of_adults,0)=2 and coalesce(number_of_children,0) = 0 then '2 Adult, No Children'
                                 when coalesce(number_of_adults,0)>2 and coalesce(number_of_children,0) = 0 then '>2 Adult, No Children'
                                 when coalesce(number_of_adults,0)>0 and coalesce(number_of_children,0) > 0 then 'Adult with Children'
                                 else 'Unknown'
                             end as number_of_adults_children, 
                            case when children_in_search > 0 then 'true' 
                                 when children_in_search=0 then 'false' 
                                 else 'Not Applicable' 
                             end as children_in_search_flag,
                            operating_system_id,
                            all_mktg_seo_30_day,
                            randomize(all_mktg_seo_30_day, ${hiveconf:hex.agg.seed}, "###", true, ${hiveconf:hex.agg.mktg.randomize.array})[0] as all_mktg_seo_random,
                            all_mktg_seo_30_day_direct, 
                            randomize(all_mktg_seo_30_day_direct, ${hiveconf:hex.agg.seed}, "###", true, ${hiveconf:hex.agg.mktg.direct.randomize.array})[0] as all_mktg_seo_direct_random,
                            entry_page_name,
                            supplier_property_id,
                            randomize(supplier_property_id, ${hiveconf:hex.agg.seed}, "###", true, ${hiveconf:hex.agg.sp.randomize.array})[0] as supplier_property_id_random,
                            rep.variant_code,
                            rep.experiment_code,
                            rep.version_number,
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
                            experiment_name,
                            variant_name,
                            report_start_date,
                            report_end_date,
                            status,
                            trans_date, 
                            test_manager,
                            product_manager,
                            pod,
                            experiment_test_id 
                       from (          select cid, 
                                              local_date, 
                                              new_visitor_ind, 
                                              page_assigned_entry_page_name, 
                                              site_sectn_name, 
                                              user_cntext_name, 
                                              browser_height, 
                                              browser_width, 
                                              brwsr_id, 
                                              mobile_ind,  
                                              property_destination_id, 
                                              platform_type, 
                                              days_until_stay, 
                                              length_of_stay, 
                                              number_of_rooms, 
                                              number_of_adults, 
                                              number_of_children, 
                                              children_in_search,
                                              operating_system_id,
                                              all_mktg_seo_30_day,
                                              all_mktg_seo_30_day_direct,
                                              entry_page_name,
                                              supplier_property_id,
                                              variant_code,
                                              experiment_code,
                                              version_number,
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
                                              num_repeat_purchasers
                                         from ${hiveconf:stage.db}.${hiveconf:hex.fact.table}) metrics 
                                   inner join (select experiment_code, 
                                                      experiment_name,
                                                      variant_code,
                                                      variant_name,
                                                      version_number,
                                                      report_start_date,
                                                      report_end_date,
                                                      status,
                                                      trans_date,
                                                      test_manager,
                                                      product_manager,
                                                      pod,
                                                      experiment_test_id 
                                                 from ${hiveconf:stage.db}.${hiveconf:hex.report.table}
                                                where ${hiveconf:rep.where}
                                              ) rep 
                                           on metrics.variant_code=rep.variant_code 
                                          and metrics.experiment_code=rep.experiment_code 
                                          and metrics.version_number=rep.version_number) active_metrics
            left outer join (          select property_typ_name, 
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
                                              expe_lodg_property_id_random 
                                         from (select property_typ_name, 
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
                                                      randomize(expe_lodg_property_id, ${hiveconf:hex.agg.seed}, "###", false, ${hiveconf:hex.agg.sp.randomize.array}) as expe_lodg_property_id_arr
                                                 from dm.lodg_property_dim 
                                                where expe_lodg_property_id<>-9998) lodg_property_dim_inner
                                 LATERAL VIEW explode(expe_lodg_property_id_arr) tt as expe_lodg_property_id_random) lpd 
                         on (active_metrics.supplier_property_id_random=lpd.expe_lodg_property_id_random)
            left outer join dm.site_dim site 
                         on (site.brand_id = 2 and site.ian_business_partnr_id not in ('-9998', '0') and active_metrics.cid=site.ian_business_partnr_id)
            left outer join (          select mktg_chnnl_name, 
                                              mktg_sub_chnnl_name, 
                                              mktg_code_random 
                                         from (select mktg_chnnl_name, 
                                                      mktg_sub_chnnl_name, 
                                                      randomize(mktg_code, ${hiveconf:hex.agg.seed}, "###", false, ${hiveconf:hex.agg.mktg.randomize.array}) mktg_code_arr
                                                 from etldata.hcom_mktg_chnnl_dim 
                                                where mktg_code<>'Unknown') web_analytic_mktg_code_dim_non_expedia_inner 
                                 LATERAL VIEW explode(mktg_code_arr) tt as mktg_code_random) mktg 
                         on (active_metrics.all_mktg_seo_random=mktg.mktg_code_random)
            left outer join (          select mktg_chnnl_name, 
                                              mktg_sub_chnnl_name, 
                                              mktg_code_random 
                                         from (select mktg_chnnl_name, 
                                                      mktg_sub_chnnl_name, 
                                                      randomize(mktg_code, ${hiveconf:hex.agg.seed}, "###", false, ${hiveconf:hex.agg.mktg.direct.randomize.array}) mktg_code_arr
                                                 from etldata.hcom_mktg_chnnl_dim 
                                                where mktg_code<>'Unknown') web_analytic_mktg_code_dim_non_expedia_inner
                                 LATERAL VIEW explode(mktg_code_arr) tt as mktg_code_random) mktg_dir 
                         on (active_metrics.all_mktg_seo_direct_random=mktg_dir.mktg_code_random)            
            left outer join (          select hcom_srch_dest_typ_name, 
                                              hcom_srch_dest_name, 
                                              hcom_srch_dest_cntry_name, 
                                              hcom_srch_dest_property_mkt_key, 
                                              hcom_srch_dest_id_random 
                                         from (select hcom_srch_dest_typ_name, 
                                                      hcom_srch_dest_name, 
                                                      hcom_srch_dest_cntry_name, 
                                                      hcom_srch_dest_property_mkt_key, 
                                                      randomize(hcom_srch_dest_id, ${hiveconf:hex.agg.seed}, "###", false, ${hiveconf:hex.agg.pd.randomize.array}) as hcom_srch_dest_id_arr
                                                 from dm.hcom_srch_dest_dim where hcom_srch_dest_id<>-9998) hcom_srch_dest_dim_inner
                                 LATERAL VIEW explode(hcom_srch_dest_id_arr) tt as hcom_srch_dest_id_random) sdd 
                         on (active_metrics.property_destination_id_random=sdd.hcom_srch_dest_id_random)
            left outer join dm.property_mkt_dim pmd 
                         on (sdd.hcom_srch_dest_property_mkt_key=pmd.property_mkt_key)
            left outer join lz.lz_hcom_dc_operating_systems os 
                         on (active_metrics.operating_system_id=os.OPERATING_SYSTEMS_ID)
            left outer join dm.brwsr_dim browsr 
                         on (active_metrics.brwsr_id=browsr.brwsr_id)
        group by local_date, 
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
                 active_metrics.experiment_code, 
                 active_metrics.experiment_name, 
                 active_metrics.variant_code, 
                 active_metrics.variant_name, 
                 active_metrics.version_number, 
                 active_metrics.report_start_date, 
                 active_metrics.report_end_date, 
                 active_metrics.status, 
                 active_metrics.trans_date, 
                 active_metrics.test_manager, 
                 active_metrics.product_manager, 
                 active_metrics.pod, 
                 active_metrics.experiment_test_id,  
                 os.OPERATING_SYSTEMS,  
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
                 site.site_super_regn_name, 
                 site.site_regn_name,
                 site.site_cntry_name, 
                 mktg.mktg_chnnl_name, 
                 mktg.mktg_sub_chnnl_name,  
                 mktg_dir.mktg_chnnl_name, 
                 mktg_dir.mktg_sub_chnnl_name,  
                 sdd.hcom_srch_dest_typ_name,  
                 sdd.hcom_srch_dest_name, 
                 sdd.hcom_srch_dest_cntry_name, 
                 property_destination_id, 
                 pmd.property_mkt_name, 
                 pmd.property_mkt_regn_name, 
                 pmd.property_mkt_super_regn_name, 
                 case when coalesce(lpd.property_cntry_name,sdd.hcom_srch_dest_cntry_name) is null then 'Not Applicable' 
                      when coalesce(property_cntry_name,hcom_srch_dest_cntry_name) = site.site_cntry_name then 'Domestic' 
                      else 'International' 
                  end;
