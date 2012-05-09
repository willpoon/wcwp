#!/bin/sh
#删除mdelete $filename 指令
host_ip=172.16.5.130
user_id=bass
password=59130A19081A21

password=`/bassdb1/etl/L/boss/decode ${password}`
echo ${password}

remote_dir=/data1/asiainfo/interface/imei/
local_dir=/bassdb1/etl/L/imei/work/
filename=*_$1_$2*
echo "开始下载接口单元『$1,$2』的数据文件，请稍侯......"
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
cd $remote_dir
lcd $local_dir
mget $filename
bye
end
echo "接口单元『$1,$2』的数据文件下载完毕!"
