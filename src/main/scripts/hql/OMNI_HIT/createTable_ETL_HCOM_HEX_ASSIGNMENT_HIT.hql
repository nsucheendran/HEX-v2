use ${hiveconf:hex.fah.db};

DROP TABLE IF EXISTS ${hiveconf:hex.fah.table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hex.fah.table} (
  guid string, 
  cid int, 
  experiment_variant_code string, 
  local_date string, 
  gmt int, 
  gmt_timestamp string, 
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
  supplier_property_id string
)
PARTITIONED BY ( 
  year_month string)
STORED AS RCFILE
LOCATION "/data/HWW/ETLDATA/${hiveconf:hex.fah.table}";

ALTER TABLE ${hiveconf:hex.fah.table} ENABLE NO_DROP;

