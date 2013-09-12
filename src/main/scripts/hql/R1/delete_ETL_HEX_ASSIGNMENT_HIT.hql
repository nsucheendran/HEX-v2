use ${hiveconf:hex.fah.db};

alter table ${hiveconf:hex.fah.table} DROP IF EXISTS PARTITION(year=${hiveconf:part.year}, month=${hiveconf:part.month});
