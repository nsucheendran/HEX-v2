use ${hiveconf:hex.fah.db};

alter table ${hiveconf:hex.fah.table} DROP IF EXISTS PARTITION(year_month='${hiveconf:part.year}-${hiveconf:part.month}');
