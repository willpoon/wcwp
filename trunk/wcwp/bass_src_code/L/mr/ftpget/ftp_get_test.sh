#!/bin/sh
#*************************************************************************************************************
#函数名：ftp_139mail_interface.sh
#功  能：从10.233.36.82主机循环取数据文件
#参  数：无
#      
#编写人: lizhanyong 
#编写时间 2010-04-29
#    
#输  出: 将文件get到172.16.5.43 /bassdb2/etl/L/mr/ftpget目录
#注意事项：循环取文件
#*************************************************************************************************************


##ftp 连接配置
host_ip=10.233.36.82
user_id=xzrbt
password=xzrbt

##ftp目录配置
remote_dir1=/work/info/month
remote_dir2=/work/info/day
local_dir=/bassdb1/etl/L/mr/ftpget

getfile_list_tmp=getfile_list_tmp.lst
#filename1=M9800120100300000001.AVL
#filename2=M9800220100300000001.AVL
#filename3=M9800320100300000001.AVL
#filename4=I9800620100428000001.zip
#filename5=I9800620100428000001.AVL

echo "ftp目录配置如下："
echo remote_dir1=$remote_dir1
echo remote_dir2=$remote_dir2
echo local_dir=$local_dir

while true  
	do
while read filename
do

echo "==================================================="
echo ${filename}
echo "开始下载彩铃接口数据文件,请稍侯..."
date

interface_code=`echo ${filename}|cut -c1-6` 
if [ "${interface_code}" = "I98006" ] ; then
remote_dir=/work/info/day
else
remote_dir=/work/info/month
fi

echo remote_dir=${remote_dir}

ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
get $filename
bye
end

echo "${filename}接口单元的数据文件下载完毕!"
mv $filename ${local_dir}/backup 
date

echo "==================================================="

done < ${local_dir}/${getfile_list_tmp}

sleep 60

done 

exit 0
