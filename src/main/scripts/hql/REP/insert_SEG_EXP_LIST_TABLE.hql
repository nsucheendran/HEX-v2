use ${hiveconf:hex.db};

insert overwrite table ${hiveconf:hex.db}.${hiveconf:hex.table}
          select experiment_code,
                 experiment_name,
                 version_number,
                 variant_code,
                 variant_name,
                 status,
                 experiment_test_id,
                 test_manager,
                 product_manager,
                 pod,
                 report_start_date,
                 report_end_date,
                 max(local_date) as latest_local_date
            from DM.RPT_HEXDM_SEG_UNPARTED
        group by experiment_code,
                 experiment_name,
                 version_number,
                 variant_code,
                 variant_name,
                 status,
                 experiment_test_id,
                 test_manager,
                 product_manager,
                 pod,
                 report_start_date,
                 report_end_date;