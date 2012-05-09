#!/bin/sh
#*************************************************************************************************************
#函数名：ftp_mmarket.sh
#功  能：从10.233.26.55 主机目录取数据文件
#参  数：用户名：xzgwjk 密码：xzgwjk
#      
#编写人: asiainfo
#编写时间 2011-04-15
#    
#输  出: 将文件get到172.16.5.43 /bassdb1/etl/L/mm 目录
#注意事项：此shell由load_mmarket.sh调用执行
#*************************************************************************************************************


##ftp 连接配置
host_ip=$1
user_id=$2
password=$3

##ftp目录配置
remote_dir=$4
local_dir=$5
filename=$6

echo "ftp目录配置如下："
echo remote_dir=$remote_dir
echo local_dir=$local_dir
echo FILENAME=$filename


echo "开始下载M-Market数据文件,请稍侯..."


if [ ! -f ${local_dir}/$filename ]; then
   
ftp -n $host_ip <<end
prompt off
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $filename
bye
end

echo "~~~~~~~m-market的$filename接口数据/校验文件下载完毕!"

else
echo "~~~~~~~现没有多余的$filename接口文件抽取"
fi



exit 0
