#!/bin/sh
#ɾ��mdelete $filename ָ��
host_ip=172.16.5.130
user_id=bass
password=59130A19081A21

password=`/bassdb1/etl/L/boss/decode ${password}`
echo ${password}

remote_dir=/data1/asiainfo/interface/imei/
local_dir=/bassdb1/etl/L/imei/work/
filename=*_$1_$2*
echo "��ʼ���ؽӿڵ�Ԫ��$1,$2���������ļ������Ժ�......"
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
cd $remote_dir
lcd $local_dir
mget $filename
bye
end
echo "�ӿڵ�Ԫ��$1,$2���������ļ��������!"
