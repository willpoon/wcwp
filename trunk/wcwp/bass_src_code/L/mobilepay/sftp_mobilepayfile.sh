#!/bin/sh
#*******************************************************
#��������sftp_mobilepayfile.sh
#��  �ܣ���ȡ�ֻ�֧���ӿ���Ϣ����ftp��172.16.5.43����
#��  ���� 
#author: zhaolp2      
#��  ��: 2010-07-28	      
#��  ��: 
#*******************************************************

filename=$1

sftp -oport=8522 xizang@211.138.236.196 << EOF
cd /home/xizang/send/
lcd /data1/interface/mobilepay/
get ${filename}
quit
EOF


echo "��10.233.26.55�����ϵ��ļ��͵�172.16.5.43"
ftp -n 172.16.5.43 <<end
prompt
user load load
bin
lcd /data1/interface/mobilepay
cd /bassdb1/etl/L/mobilepay
put $filename
bye
end

#ɾ��10.233.26.55�����ϵ��ļ�
cd /data1/interface/mobilepay
rm $filename
