#!/bin/sh
#****************************************************************************************
# ** ��������: upload_etldata_43.sh
# ** ������: �����10.233.30.61ʵʱ�ϴ�BOSS���ݵ���Ŀ¼
# ** ��������: ʵʱ
# ** ����ʾ��: ./upload_etldata_43.sh
# ** ����ʱ��: 2010-2-24 10:52:07
# ** �� �� ��: xufr
# ** ��    ��: 1.
# ** �޸���ʷ:
# **           �޸�����      �޸���      �޸�����
# **           -----------------------------------------------
# **
# ** Copyright(c) 2009 AsiaInfo Technologies (China), Inc.
# ** All Rights Reserved.
#****************************************************************************************
while [ true ];
do
date >> upload_etldata_43.log
cd /bassdb1/etl/L
if [ ! -f upload.tar.gz.success ]; then
	echo "������upload.tar.gz.success�ļ����ȴ�30�룬���¼��" >> upload_etldata_43.log
	sleep 30
	continue
fi
echo "����upload_running�ļ���������" >> upload_etldata_43.log
touch upload_running
echo "��ѹ���ļ�" >> upload_etldata_43.log
gunzip upload.tar.gz
echo "����ļ�" >> upload_etldata_43.log
tar xvf upload.tar
echo "�����ļ�����" >> upload_etldata_43.log
mv /bassdb1/etl/L/upload/boss/* /bassdb1/etl/L/boss/

if [ -f /bassdb1/etl/L/upload/xfer/* ]; then
ftp -n <<!
open 172.16.5.43
user ifboss ifboss
lcd /bassdb1/etl/L/upload/xfer
cd /bassdb1/etl/E/xfer/backup
prompt
bi
mput *
lcd /bassdb1/etl/L
cd /bassdb1/etl/E/xfer
put upload.tar.gz.success
by
!
fi

cd /bassdb1/etl/L
rm upload.tar
rm -r upload
rm upload.tar.gz.success
echo "�ȴ�30�룬���¼��" >> upload_etldata_43.log
echo "\n" >> upload_etldata_43.log
sleep 30
rm upload_running
done 
