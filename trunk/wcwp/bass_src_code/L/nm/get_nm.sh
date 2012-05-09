#!/bin/sh
src_dir=$1
pattern_val=$2
cycle_id=$3
obj_dir=/bassdb1/etl/L/nm
log_dir=/bassdb1/etl/L/nm/nm_data
log_file=${log_dir}/nm_${cycle_id}.log
log_temp_file=${log_dir}/nm_temp.log

cd ${src_dir}
ls -l $pattern_val | awk '{print $9}' > ${log_temp_file}

while read sfilename
do
    if [ -f ${log_file} ] ; then
        exist_file_name=`grep ${sfilename} ${log_file}`
        if [ "${exist_file_name}" = "" ] ; then
              echo ${sfilename} >> ${log_file}
              cp ${sfilename} ${obj_dir}
        fi      
    else
        echo ${sfilename} > ${log_file}
        cp ${sfilename} ${obj_dir}
    fi
    
done < ${log_temp_file}


rm ${log_temp_file}

echo "取网管接口数据[$1,$2,$3]完毕!"
exit 0
