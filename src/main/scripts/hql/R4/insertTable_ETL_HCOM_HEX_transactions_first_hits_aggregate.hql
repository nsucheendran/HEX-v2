-- omniture's last transaction considered timestamp
R4.omniture_bookmark_datetm='2013-07-01.57.00.000000'
-- bkg_xref's last transaction considered timestamp
R4.booking_bookmark_datetm='2013-07-01.57.00.000000'
R4.bookmark_datetm=min(R4.omniture_bookmark_datetm,R4.booking_bookmark_datetm)
R4.min_report_date_yrmonth='2013-01'
R4.min_report_date='2013-01-01'
R4.today='2013-09-25'
R4.max_report_date='2013-09-25'
R4.max_record_date=min(R4.max_report_date, R4.today)
R4.max_record_yr_month=year-Month(R4.max_record_date)

use platdev;

insert into table ETL_HCOM_HEX_transactions_first_hits_aggregate partition(year_month, source)
select case when purchase_flag=false then 0-num_transactions when purchase_flag=true then num_transactions else cast(0 as bigint) end as num_transactions, bkg_gbv, bkg_room_nights,
gross_profit, guid, cid, local_date, trans_date, variant_code, experiment_code, version_number, new_visitor_ind, page_assigned_entry_page_name, 
site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, destination_id, property_destination_id, platform_type, 
days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,all_mktg_seo,all_mktg_seo_direct,
entry_page_name, substr(local_date, 1, 7) as year_month, source from 
(select count(1) as num_transactions, hits_by_report.variant_code, experiment_code,version_number, trans.guid, cid, hits_by_report.local_date as local_date, trans.purchase_flag, trans.trans_date as trans_date,
sum(bkg_gbv) as bkg_gbv, sum(bkg_room_nights) as bkg_room_nights, sum(gross_profit) as gross_profit, new_visitor_ind, page_assigned_entry_page_name, 
site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, destination_id, property_destination_id, platform_type, 
days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,all_mktg_seo,all_mktg_seo_direct,
entry_page_name, source
from
(select guid, purchase_flag, local_date as trans_date, gmt, bkg_gbv, bkg_room_nights, gross_profit, source from
hwwdev.ETL_HCOM_HEX_transactions 
where year_month>='${R4.min_report_date_yrmonth}'
and year_month<='${R4.max_record_yr_month}'
and local_date>='${R4.min_report_date}' 
and local_date<='${R4.max_record_date}') trans
join 
(
select /*+ MAPJOIN(rep) */ cid, variant_code,experiment_code, version_number, guid, local_date, gmt, report_start_date, report_end_date, last_updated_dt, 
case when trans_date is null or trans_date='' then report_end_date else trans_date end as trans_date, new_visitor_ind, page_assigned_entry_page_name, 
site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, destination_id, property_destination_id, platform_type, 
days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,all_mktg_seo,all_mktg_seo_direct,
entry_page_name from
(select guid, cid, local_date, gmt, experiment_variant_code, new_visitor_ind, page_assigned_entry_page_name, 
site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, destination_id, property_destination_id, platform_type, 
days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,all_mktg_seo,all_mktg_seo_direct,
entry_page_name
from etldata.etl_hcom_hex_first_assignment_hit 
where year_month>='${R4.min_report_date_yrmonth}' and year_month<='${R4.max_record_yr_month}' and local_date<='${R4.max_record_date}') first_hits
join
(select variant_code,experiment_code,version_number, report_start_date, case when report_end_date is null or report_end_date='' then '9999-99-99' 
else report_end_date end as report_end_date, last_updated_dt, trans_date
from platdev.HEX_REPORTING_REQUIREMENTS where 
(last_updated_dt>='${hiveconf:R4.bookmark_datetm}') or 
(report_start_date<='${hiveconf:R4.bookmark_datetm}' and report_end_date>='${hiveconf:R4.bookmark_datetm}')) rep
on (first_hits.experiment_variant_code=rep.variant_code)
where first_hits.local_date>=rep.report_start_date and 
first_hits.local_date<=rep.report_end_date
) hits_by_report
on (hits_by_report.guid=trans.guid)
where trans.trans_date<=hits_by_report.trans_date and hits_by_report.gmt<=trans.gmt and 
((hits_by_report.last_updated_dt>='${hiveconf:R4.bookmark_datetm}' and trans.trans_date>=hits_by_report.report_start_date)
or ((hits_by_report.last_updated_dt='' or hits_by_report.last_updated_dt is null) and ((trans.source='booking' and trans.trans_date>'${R4.booking_bookmark_datetm}') or 
(trans.source='omniture' and trans.trans_date>'${R4.booking_omniture_datetm}'))))
group by hits_by_report.variant_code, experiment_code, version_number, trans.guid, cid, trans.purchase_flag, hits_by_report.local_date, trans.trans_date, source, new_visitor_ind, page_assigned_entry_page_name, 
site_sectn_name, user_cntext_name, browser_height, browser_width, brwsr_id, mobile_ind, destination_id, property_destination_id, platform_type, 
days_until_stay, length_of_stay, number_of_rooms, number_of_adults, number_of_children, children_in_search,operating_system,all_mktg_seo,all_mktg_seo_direct,
entry_page_name) rep_hit_trans;