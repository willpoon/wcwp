#!/bin/sh
#create by lizy 
#功能:每天凌晨5点33分crontab调度执行，将load_mr.sh重启一次，防止因ftp影响接口文件传输。

echo `date`

process_id=`ps -ef|grep load_mr_nfjd.sh|grep -v grep|awk '{print $2}'`
echo ${process_id}
    
      cd /bassdb1/etl/L/mr
      echo "kill load_mr.sh..."
      kill -9 ${process_id}   
      echo "run load_mr.sh..." 
      nohup ./load_mr_nfjd.sh &           

echo "程序执行完毕!"




