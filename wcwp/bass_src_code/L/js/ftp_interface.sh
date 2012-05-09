#!/bin/sh

host_ip=$1
user_id=$2
password=$3

obj_dir=$4
src_dir=$5
file_name=$6
file_list_all=file_interface_all.lst
file_list_tmp=file_interface_tmp.lst

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

echo "接口数据文件下载开始!"

while read sfilename
do
     file_name_s=`echo ${sfilename} | awk '{print $9}'`
     file_name_o=`grep $file_name_s ${obj_dir}/backup/${file_list_all} |cut -d= -f1`
     
echo  "[${file_name_s},${file_name_o}]"    
     
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

done < ${obj_dir}/${file_list_tmp}


rm $file_list_tmp

echo "接口数据文件下载结束!"

exit 0

