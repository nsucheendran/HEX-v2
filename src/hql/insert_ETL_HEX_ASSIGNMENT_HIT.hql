insert into table platdev.ETL_HCOM_HEX_ASSIGNMENT_HIT 
  select coalesce(e1.c44, 'Unknown') as guid,
         e1.cid, 
         test2.test_variant_code as experiment_variant_code,
         e1.local_date,
         e1.gmt,
         e1.hit_data_id,
         e1.new_visitor_ind,
         e1.page_assigned_entry_page_name,
         e1.site_sectn_name,
         e1.user_cntext_name,
         e1.browser_height,
         e1.browser_width,
         e1.brwsr_id,
         e1.c302 as mobile_ind,
         e1.destination_id,
         e1.property_destination_id 
    from (         select temp.min_hit_data_id,
                          test_variant_code
                     from (  select firstValueNSort(hit_data_id, gmt, visit_page_number) min_hit_data_id, 
                                    cid,
                                    test_variant_code,
                                    c44
                               from etl_hcom_hit_data LATERAL VIEW explode(split(concat_ws(',',c154,c281),',')) tt as test_variant_code
                              where test_variant_code <> ''
                                and local_date = '2013-07-01'
                           group by cid, test_variant_code, c44) temp
          left outer join platdev.ETL_HCOM_HEX_ASSIGNMENT_HIT test1
                       on (temp.cid = test1.cid
                      and temp.test_variant_code = test1.experiment_variant_code
                      and coalesce(temp.c44, 'Unknown') = test1.guid)
                    where test1.guid is null) test2
    join etl_hcom_hit_data e1 
      on (e1.hit_data_id = test2.min_hit_data_id
     and e1.local_date = '2013-07-01');


