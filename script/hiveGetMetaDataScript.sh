#!/bin/bash
if [ ! -d "uniplore/hive" ];then
  mkdir -p uniplore/hive
fi
hive -e " show databases; exit ;" > uniplore/hive/databases

for database in `cat uniplore/hive/databases`
do
  if [ ! -d "uniplore/hive/${database}" ];then
    mkdir -p uniplore/hive/${database}
  fi
  rm -rf uniplore/hive/${database}/*  
  hive -e " use ${database} ;  show tables ; exit ;" > uniplore/hive/${database}/tables
  for table in `cat uniplore/hive/${database}/tables`
  do
     if [ ! -d "uniplore/hive/${database}/${table}" ];then
       mkdir -p uniplore/hive/${database}/${table}
     fi
     rm -rf uniplore/hive/${database}/${table}/*
     hive -e "use ${database} ; desc ${table} ; exit;" > uniplore/hive/${database}/${table}/columns
     hive -e "use ${database} ; desc formatted ${table} ; exit;" > uniplore/hive/${database}/${table}/desc_${table}
     sql="select ";
	cat uniplore/hive/${database}/${table}/columns | while read column
	do
          tmp=`echo ${column}|awk -F " " '{print $1}'`
          sql=$sql" count( distinct ${tmp}),";
          hive -e "use ${database} ; select ${tmp},count(${tmp}) from ${table} group by ${tmp} limit 100; exit;">uniplore/hive/${database}/${table}/desc_col_${tmp}
          if [ "${tmp}" == "data_src_org" ];then
            break
          fi
    done
    sql=$sql" count(*) from "${table}" ;"
    hive -e "use ${database} ; $sql exit ; ">pbc/hive/${database}/${table}/desc_table_${table}
  done
done
