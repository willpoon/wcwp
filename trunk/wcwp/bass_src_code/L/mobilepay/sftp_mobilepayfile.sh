#!/bin/sh
#*******************************************************
#函数名：sftp_mobilepayfile.sh
#功  能：获取手机支付接口信息，并ftp到172.16.5.43主机
#参  数： 
#author: zhaolp2      
#日  期: 2010-07-28	      
#输  出: 
#*******************************************************

filename=$1

sftp -oport=8522 xizang@211.138.236.196 << EOF
cd /home/xizang/send/
lcd /data1/interface/mobilepay/
get ${filename}
quit
EOF


echo "将10.233.26.55主机上的文件送到172.16.5.43"
ftp -n 172.16.5.43 <<end
prompt
user load load
bin
lcd /data1/interface/mobilepay
cd /bassdb1/etl/L/mobilepay
put $filename
bye
end

#删除10.233.26.55主机上的文件
cd /data1/interface/mobilepay
rm $filename
