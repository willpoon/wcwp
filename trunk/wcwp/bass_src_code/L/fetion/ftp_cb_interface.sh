#!/bin/sh
#*************************************************************************************************************
#��������ftp_cb_interface.sh
#��  �ܣ���10.233.26.65����ȡ�����ļ�
#��  ������
#      
#��д��: zhaolp2
#��дʱ�� 2009-08-21
#    
#��  ��: ���ļ�get��10.233.23.60 /bassdb2/etl/L/cbĿ¼
#ע�������shell��build_cb.sh����ִ��
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


echo "��ʼ���ؽӿڵ�Ԫ�����ļ�,���Ժ�..."
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $filename
bye
end

echo "�ӿڵ�Ԫ�������ļ��������!"

exit 0
