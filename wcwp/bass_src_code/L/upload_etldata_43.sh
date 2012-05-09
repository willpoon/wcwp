#!/bin/sh
#****************************************************************************************
# ** 程序名称: upload_etldata_43.sh
# ** 程序功能: 处理从10.233.30.61实时上传BOSS数据到各目录
# ** 运行粒度: 实时
# ** 运行示例: ./upload_etldata_43.sh
# ** 创建时间: 2010-2-24 10:52:07
# ** 创 建 人: xufr
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
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
	echo "不存在upload.tar.gz.success文件，等待30秒，重新检测" >> upload_etldata_43.log
	sleep 30
	continue
fi
echo "创建upload_running文件，建立锁" >> upload_etldata_43.log
touch upload_running
echo "解压缩文件" >> upload_etldata_43.log
gunzip upload.tar.gz
echo "解包文件" >> upload_etldata_43.log
tar xvf upload.tar
echo "进行文件复制" >> upload_etldata_43.log
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
echo "等待30秒，重新检测" >> upload_etldata_43.log
echo "\n" >> upload_etldata_43.log
sleep 30
rm upload_running
done 
