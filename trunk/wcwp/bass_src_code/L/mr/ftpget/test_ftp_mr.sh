#!/bin/sh

host_ip=10.233.241.47
user_id=crbtbi
password=111016160B78

password=`/bassdb1/etl/L/boss/decode ${password}`



obj_dir=/bassdb1/etl/L/mr/ftpget
src_dir=$1
file_name=$2
file_list_all=file_list_all.lst
file_list_tmp=file_list_tmp.lst

ftp -n $host_ip <<end
pass
tcpwindow 900000
prompt
user $user_id $password
bin
cd  $src_dir
lcd $obj_dir
dir $file_name $file_list_tmp
bye
end

cd $obj_dir
if [ ! -f ${obj_dir}/$file_list_all ] ; then
   echo "" >  ${obj_dir}/$file_list_all
fi

echo "彩铃数据文件下载开始!"

while read sfilename
do
     file_name_s=`echo ${sfilename} | awk '{print $9}'`
     file_name_o=`grep $file_name_s ${obj_dir}/${file_list_all} |cut -d= -f1`
     

     
     if [ "${file_name_o}" = "" ] ; then
echo  "[${file_name_s},${file_name_o}]"    
ftp -n $host_ip <<end
pass
tcpwindow 900000
prompt
user $user_id $password
bin
cd  $src_dir
lcd $obj_dir
get $file_name_s

#删除文件 20080201 add
#取文件后，不删除远程主机上的文件，modify by lizy 2009-06-01 
#del $file_name_s

bye
end
echo $file_name_s >>  ${obj_dir}/$file_list_all
     fi

done < ${obj_dir}/${file_list_tmp}


#rm $file_list_tmp

echo "彩铃数据文件下载完毕!"

exit 0

