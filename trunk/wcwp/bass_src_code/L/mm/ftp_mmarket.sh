#!/bin/sh
#*************************************************************************************************************
#��������ftp_mmarket.sh
#��  �ܣ���10.233.26.55 ����Ŀ¼ȡ�����ļ�
#��  �����û�����xzgwjk ���룺xzgwjk
#      
#��д��: asiainfo
#��дʱ�� 2011-04-15
#    
#��  ��: ���ļ�get��172.16.5.43 /bassdb1/etl/L/mm Ŀ¼
#ע�������shell��load_mmarket.sh����ִ��
#*************************************************************************************************************


##ftp ��������
host_ip=$1
user_id=$2
password=$3

##ftpĿ¼����
remote_dir=$4
local_dir=$5
filename=$6

echo "ftpĿ¼�������£�"
echo remote_dir=$remote_dir
echo local_dir=$local_dir
echo FILENAME=$filename


echo "��ʼ����M-Market�����ļ�,���Ժ�..."


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

echo "~~~~~~~m-market��$filename�ӿ�����/У���ļ��������!"

else
echo "~~~~~~~��û�ж����$filename�ӿ��ļ���ȡ"
fi



exit 0
