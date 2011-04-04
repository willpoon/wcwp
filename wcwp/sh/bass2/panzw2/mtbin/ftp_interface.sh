#!/bin/sh

host_ip=$1
user_id=$2
password=$3

task_id=$4
time_id=$5
file_name=${task_id}${time_id}

echo $host_ip
echo $user_id
echo $password
echo $file_name
echo $src_dir

#cd /bassapp/bass2/tcl/zhaolp/data
#cd /bassapp/bass2/panzw2/tcl/data
#data_dir=/bassapp/bass2/panzw2/tcl/data

ftp -n $host_ip <<EOF
prompt on 
user $user_id $password
bin
lcd /bassapp/bass2/panzw2/mtbin/data
cd /bassdb1/etl/L/mtload/backup/mtbackup
mput ${file_name}*.*
!rm ${file_name}*.*
bye
EOF >> ftp_interface.sh.log

#rm ${file_name}*.*

echo "接口数据文件传输结束!"


exit 0

