#!/bin/sh

host_ip=10.0.0.196
user_id=xzjf
password=02100C64

password=`/bassdb1/etl/L/boss/decode ${password}`
echo ${password}

obj_dir=/bassdb1/etl/L/ota
src_dir=$1
file_name=$2
file_list_all=file_list_all.lst
file_list_tmp=file_list_tmp.lst

echo "当前文件路径是：$src_dir"

ftp -n $host_ip <<end
prompt
user $user_id $password
bin
cd  $src_dir
lcd $obj_dir
dir $file_name $file_list_tmp
bye
end

cd $obj_dir
if [ ! -f ${obj_dir}/backup/$file_list_all ] ; then
   echo "" >  ${obj_dir}/backup/$file_list_all
fi

more  $file_list_tmp

echo "OTA数据文件下载开始!"

while read sfilename
do
     file_name_s=`echo ${sfilename} | awk '{print $9}'`
echo   $file_name_s  
     file_name_o=`grep $file_name_s ${obj_dir}/backup/${file_list_all} |cut -d= -f1`
     if [ "${file_name_o}" = "" ] ; then
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
cd  $src_dir
lcd $obj_dir
get $file_name_s
bye
end
echo $file_name_s >>  ${obj_dir}/backup/$file_list_all
     fi

done < $file_list_tmp


rm $file_list_tmp

echo "OTA数据文件下载完毕!"

exit 0

