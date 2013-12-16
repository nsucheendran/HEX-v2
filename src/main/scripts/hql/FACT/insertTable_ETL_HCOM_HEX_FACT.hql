use hwwdev;

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

insert overwrite table hwwdev.ETL_HCOM_HEX_FACT partition(experiment_code, version_number, variant_code)
    select sum(unique_viewer) as num_unique_viewers, 
    sum(unique_purchaser) as num_unique_purchasers, 
    sum(unique_canceller) as num_unique_cancellers, 
    sum(active_purchaser) as num_active_purchasers, 
    sum(nil_net_order_purchaser) as num_inactive_purchasers,   
    sum(num_cancellations) as total_cancellations, 
    sum(net_orders) as net_orders, 
    sum(net_bkg_gbv) as net_bkg_gbv, 
    sum(net_bkg_room_nights) as net_bkg_room_nights,
    sum(net_omniture_gbv) as net_omniture_gbv, 
    sum(net_omniture_room_nights) as net_omniture_room_nights,  
    sum(net_gross_profit) as net_gross_profit, 
    sum(repeat_purchaser) as num_repeat_purchasers, 
    cid, local_date, 
    new_visitor_ind, page_assigned_entry_page_name, 
    site_sectn_name, user_cntext_name, 
    browser_height, browser_width, 
    brwsr_id, mobile_ind, 
    destination_id, property_destination_id, 
    platform_type, days_until_stay, 
    length_of_stay, number_of_rooms, 
    number_of_adults, number_of_children, 
    children_in_search, operating_system_id, 
    all_mktg_seo_30_day, all_mktg_seo_30_day_direct, 
    entry_page_name,
    supplier_property_id, experiment_name, 
    variant_name, status, experiment_test_id,
    experiment_code, version_number, variant_code
    from 
    (
        select 1 as unique_viewer, 
        case when max(max_trans_by_itin)>0 then 1 else 0 end as unique_purchaser, 
        case when min(min_trans_by_itin)<0 then 1 else 0 end as unique_canceller, 
        case when sum(num_trans_by_itin)>0 then 1 else 0 end as active_purchaser, 
        case when (max(max_trans_by_itin)>0 and sum(num_trans_by_itin)==0) then 1 else 0 end as nil_net_order_purchaser, 
        sum(cancellation_by_itin) as num_cancellations, 
        sum(num_trans_by_itin) as net_orders, 
        sum(net_bkg_gbv_by_itin) as net_bkg_gbv, 
        sum(net_bkg_room_nights_by_itin) as net_bkg_room_nights,
        sum(net_omniture_gbv_by_itin) as net_omniture_gbv, 
        sum(net_omniture_room_nights_by_itin) as net_omniture_room_nights,  
        sum(net_gross_profit_by_itin) as net_gross_profit, 
        case when min(num_trans_by_itin)>0 and count(distinct booking_date_for_active_itin)>=2 then 1 
            else 
                case when min(num_trans_by_itin)<=0 and count(distinct booking_date_for_active_itin)>=3 then 1 
                    else 0 
                end 
        end as repeat_purchaser,
        guid, cid, local_date, variant_code, experiment_code, version_number,
        new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, 
        destination_id, property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, 
        children_in_search, operating_system_id, all_mktg_seo_30_day, all_mktg_seo_30_day_direct, entry_page_name, experiment_name, variant_name, status, experiment_test_id,
        supplier_property_id
        from 
        (
            select /*+ MAPJOIN(rep) */ sum(num_transactions) as num_trans_by_itin, sum(bkg_gbv) as net_bkg_gbv_by_itin, 
            sum(bkg_room_nights) as net_bkg_room_nights_by_itin, 
            sum(omniture_gbv) as net_omniture_gbv_by_itin, 
            sum(omniture_room_nights) as net_omniture_room_nights_by_itin, 
            sum(gross_profit) as net_gross_profit_by_itin, 
            max(num_transactions) as max_trans_by_itin, 
            min(num_transactions) as min_trans_by_itin, 
            case when min(num_transactions)<0 then min(num_transactions) else 0 end as cancellation_by_itin,
            case when sum(num_transactions)>0 then 1 else 0 end as num_active_itin, 
            case when sum(num_transactions)>0 then min(agg.trans_date) else "null" end as booking_date_for_active_itin, 
            guid, cid, itin_number, local_date, rep.variant_code, rep.experiment_code, rep.version_number,
            new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, 
            destination_id, property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, 
            children_in_search, operating_system_id, all_mktg_seo_30_day, all_mktg_seo_30_day_direct, entry_page_name, rep.experiment_name, rep.variant_name, rep.status, rep.experiment_test_id,
            supplier_property_id
            from 
            hwwdev.etl_hcom_hex_fact_staging agg 
            join 
            HWWDEV.HEX_REPORTING_REQUIREMENTS rep
            on (agg.variant_code=rep.variant_code and rep.experiment_code=agg.experiment_code and rep.version_number=agg.version_number)
            group by itin_number, guid, cid, local_date, rep.variant_code, rep.experiment_code, rep.version_number,
            new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, 
            destination_id, property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, 
            children_in_search, operating_system_id, all_mktg_seo_30_day, all_mktg_seo_30_day_direct, entry_page_name, rep.experiment_name, rep.variant_name, rep.status, rep.experiment_test_id,
            supplier_property_id
        ) agg_by_itin_num
        group by guid, cid, local_date, variant_code, experiment_code, version_number,
        new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, 
        destination_id, property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, 
        children_in_search, operating_system_id, all_mktg_seo_30_day, all_mktg_seo_30_day_direct, entry_page_name, experiment_name, variant_name, status, experiment_test_id,
        supplier_property_id
    ) agg_by_guid
    group by cid, local_date, variant_code, experiment_code, version_number,
    new_visitor_ind, page_assigned_entry_page_name, site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, 
    destination_id, property_destination_id, platform_type, days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, 
    children_in_search, operating_system_id, all_mktg_seo_30_day, all_mktg_seo_30_day_direct, entry_page_name, experiment_name, variant_name, status, experiment_test_id,
    supplier_property_id;