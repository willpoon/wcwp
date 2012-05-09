#!/bin/sh
#*************************************************************************************************************
#函数名：ftp_139mail_interface.sh
#功  能：从10.233.26.55主机mgame目录取数据文件
#参  数：无
#      
#编写人: lizhanyong 
#编写时间 2010-04-27
#    
#输  出: 将文件get到172.16.5.43 /bassdb2/etl/L/mgame目录
#注意事项：此shell由load_mgame.sh调用执行
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


echo "开始下载手机邮箱数据文件,请稍侯..."
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $filename
bye
end

echo "手机游戏接口单元的数据文件下载完毕!"

exit 0
