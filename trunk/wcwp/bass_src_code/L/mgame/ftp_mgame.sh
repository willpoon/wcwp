#!/bin/sh
#*************************************************************************************************************
#��������ftp_139mail_interface.sh
#��  �ܣ���10.233.26.55����mgameĿ¼ȡ�����ļ�
#��  ������
#      
#��д��: lizhanyong 
#��дʱ�� 2010-04-27
#    
#��  ��: ���ļ�get��172.16.5.43 /bassdb2/etl/L/mgameĿ¼
#ע�������shell��load_mgame.sh����ִ��
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


echo "��ʼ�����ֻ����������ļ�,���Ժ�..."
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $filename
bye
end

echo "�ֻ���Ϸ�ӿڵ�Ԫ�������ļ��������!"

exit 0
