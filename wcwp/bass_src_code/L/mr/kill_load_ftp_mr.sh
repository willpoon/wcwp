#!/bin/sh
#create by lizy 
#����:ÿ���賿5��33��crontab����ִ�У���load_mr.sh����һ�Σ���ֹ��ftpӰ��ӿ��ļ����䡣

echo `date`

process_id=`ps -ef|grep load_mr_nfjd.sh|grep -v grep|awk '{print $2}'`
echo ${process_id}
    
      cd /bassdb1/etl/L/mr
      echo "kill load_mr.sh..."
      kill -9 ${process_id}   
      echo "run load_mr.sh..." 
      nohup ./load_mr_nfjd.sh &           

echo "����ִ�����!"




