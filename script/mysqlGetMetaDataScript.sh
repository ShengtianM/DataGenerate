#!/bin/bash
CNFPATH="./localhost.cnf"
DIRPRE="uniplore/mysql"
if [ ! -d "${DIRPRE}/" ];then
  mkdir -p ${DIRPRE}/
fi
mysql --defaults-file=${CNFPATH} -e " show databases;" > ${DIRPRE}/databases.txt
for database in `cat ${DIRPRE}/databases.txt`
do
  if [[ ${database} != *_schema ]] && [ "${database}" !=  "hive" ] && [ "${database}" !=  "Database" ] && [ "${database}" !=  "caravel_test" ] && [ "${database}" !=  "superset_20_6" ]; then
    if [ ! -d "${DIRPRE}/${database}/" ];then
      mkdir -p ${DIRPRE}/${database}
    fi
    rm  -rf ${DIRPRE}/${database}/*
    mysql --defaults-file=${CNFPATH} -e "SELECT CONCAT(table_schema,'.',table_name) AS 'Table Name', table_rows AS 'Number of Rows', CONCAT(data_length,'b') AS 'Data Size', CONCAT(index_length,'b') AS 'Index Size', CONCAT(data_length+index_length,'b') AS'Total'FROM information_schema.TABLES WHERE table_schema = '${database}';" > ${DIRPRE}/${database}/detail_${database}.txt
    mysql --defaults-file=${CNFPATH} -e " use ${database} ;  show tables ;" > ${DIRPRE}/${database}/tables.txt
 
    for table in `cat ${DIRPRE}/${database}/tables.txt`
    do
     if [[ ${table} != *ables* ]];then
       if [ ! -d "${DIRPRE}/${database}/${table}" ];then
        mkdir -p ${DIRPRE}/${database}/${table}
       fi
       rm -rf ${DIRPRE}/${database}/${table}/*
       mysql --defaults-file=${CNFPATH} -e "select column_name from information_schema.columns where table_name = '${table}' and table_schema = '${database}';" > ${DIRPRE}/${database}/${table}/columns.txt
       mysql --defaults-file=${CNFPATH} -e "use ${database} ; show columns in ${table} ;" > ${DIRPRE}/${database}/${table}/desc_${table}
       #sql="select "
       for column in `cat ${DIRPRE}/${database}/${table}/columns.txt`
	   do
         { 
         if [ "${column}" != "column_name" ]; then          
            mysql --defaults-file=${CNFPATH} -e  "use ${database} ; select ${column},count(${column}) as cc from ${table} group by ${column} order by cc desc limit 100; ">${DIRPRE}/${database}/${table}/desc_col_${column}
            mysql --defaults-file=${CNFPATH} -e "use ${database} ; select count(distinct ${column}) from ${table}; ">${DIRPRE}/${database}/${table}/dsc_col_${column}
          fi
         }
#&
        done
       #sql=$sql" count(*) from "${table}" ;"
      #wait 
     fi
    done
  fi
done