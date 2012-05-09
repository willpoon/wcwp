#!/bin/sh
#****************************************************************************************
# ** 程序名称: upload_etldata_60.sh
# ** 程序功能: 根据西藏加载情况，实时上传BOSS数据到10.233.30.61主机
# ** 运行粒度: 实时
# ** 运行示例: ./upload_etldata_60.sh
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
#求取昨天的日期
yesterday()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=`echo "$1"|cut -c7-8`

        month=`expr $month + 0`
        day=`expr $day - 1`

        if [ $day -eq 0 ]; then
                month=`expr $month - 1`
                if [ $month -eq 0 ]; then
                        month=12
                        day=31
                        year=`expr $year - 1`
                else
                        case $month in
                                1|3|5|7|8|10|12) day=31;;
                                4|6|9|11) day=30;;
                                2)
                                        if [ `expr $year % 4` -eq 0 ]; then
                                                if [ `expr $year % 400` -eq 0 ]; then
                                                        day=29
                                                elif [ `expr $year % 100` -eq 0 ]; then
                                                        day=28
                                                else
                                                        day=29
                                                fi
                                        else
                                                day=28
                                        fi ;;
                        esac
                fi
        fi

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

#设定目录
workDir="/bassdb2/etl/L"
fileDir="/bassdb2/etl/L/boss/backup"
transDir="/bassdb2/etl/L/upload"
logDir="/bassdb2/etl/L/upload_log"

exeToday=`date '+%Y%m%d'`
checkTime=`date '+%Y%m%d%H%M%S'`
#if [ $checkTime -lt ${exeToday}011000 ]; then
#	callFileTransFlag=0
#	smsFileTransFlag=0
#fi
#if [ $checkTime -ge ${exeToday}011000 ] && [ $checkTime -lt ${exeToday}014000 ]; then
	callFileTransFlag=1
	smsFileTransFlag=0
#fi
#if [ $checkTime -ge ${exeToday}014000 ]; then
#	callFileTransFlag=1
#	smsFileTransFlag=1
#fi
exeToday=`date '+%Y%m%d'`

while [ true ]
do
today=`date '+%Y%m%d'`
if [ ! $exeToday = $today ]; then
	exeToday=$today
	callFileTransFlag=0
	smsFileTransFlag=0
fi

sday=`yesterday $today`
nowtime=`date '+%Y%m%d%H%M%S'`
date >> $logDir/upload_etldata_60_$today.log
cd $workDir
ftp -n <<!
open 172.16.5.43
user load load
cd $workDir
prompt
bi
get upload_running
by
!
if [ -f upload_running ]; then
	echo "存在upload_running文件，等待120秒后，重新检测" >> $logDir/upload_etldata_60_$today.log
	sleep 120
	rm upload_running
	continue
fi
echo "不存在upload_running文件,准备检测需要传送的接口文件" >> $logDir/upload_etldata_60_$today.log

cd $fileDir
if [ ! -f *.CHK ]; then
	echo "不存在需要传送的接口文件,等待120秒后，重新检测" >> $logDir/upload_etldata_60_$today.log
	sleep 120
	continue
fi
ls -l *.CHK | awk '{print $9}' > chkFile.List
while read fileName
do
	prefixFileName=`echo "$fileName" | cut -c1-20`
	fileDate=`echo "$fileName" | cut -c7-14`
	echo "prefixFileName=$prefixFileName"
	checkFileName="${prefixFileName}.CHK"
	dataFileName="${prefixFileName}.AVL.Z"
	echo "文件名称($checkFileName $dataFileName)，开始复制" >> $logDir/upload_etldata_60_$today.log
	if [ -f ${prefixFileName}* ]; then
		cp ${prefixFileName}* ${transDir}/boss/
	fi
	if [ ! -d ${fileDate} ]; then
		mkdir ${fileDate}
	fi
	mv ${prefixFileName}* ${fileDate}/
done < chkFile.List
if [ $nowtime -ge ${today}011000 ] && [ $callFileTransFlag -eq 0 ]; then
	echo "$nowtime需要传送语音清单文件,开始复制" >> $logDir/upload_etldata_60_$today.log
	cp /bassdb2/etl/E/boss/xfer/backup/$sday/DR_GSM_*${sday}* ${transDir}/xfer/
	cp /bassdb2/etl/E/boss/xfer/backup/DR_GSM_*${sday}* ${transDir}/xfer/
	callFileTransFlag=`expr $callFileTransFlag + 1`
elif [ $nowtime -ge ${today}014000 ] && [ $smsFileTransFlag -eq 0 ]; then
	echo "$nowtime需要传送短信清单文件,开始复制" >> $logDir/upload_etldata_60_$today.log
	cp /bassdb2/etl/E/boss/xfer/backup/$sday/DR_SMS_*${sday}* ${transDir}/xfer/
	cp /bassdb2/etl/E/boss/xfer/backup/DR_SMS_*${sday}* ${transDir}/xfer/
	smsFileTransFlag=`expr $smsFileTransFlag + 1`
fi

if [ ! -f ${transDir}/boss/* ]; then
	if [ ! -f ${transDir}/xfer/* ]; then
		echo "各目录下不存在文件，等待120秒，重新检测" >> $logDir/upload_etldata_60_$today.log
		sleep 120
		continue
	fi
fi

cd $workDir
echo "对文件打包" >> $logDir/upload_etldata_60_$today.log
tar cvf upload.tar upload/
echo "对文件压缩" >> $logDir/upload_etldata_60_$today.log
gzip -9 upload.tar
echo "删除打过包的文件" >> $logDir/upload_etldata_60_$today.log
rm upload/*/*

touch upload.tar.gz.success
echo "进行FTP传输" >> $logDir/upload_etldata_60_$today.log
ftp -n <<!
open 172.16.5.43
user load load
bi
tcpwindow 900000
cd /bassdb1/etl/L
lcd /bassdb2/etl/L
put upload.tar.gz
put upload.tar.gz.success
by
!
cd /bassdb2/etl/L
echo "删除压缩文件、锁文件、成功文件，等待60秒重新检测" >> $logDir/upload_etldata_60_$today.log
rm upload.tar.gz
rm upload_running
rm upload.tar.gz.success
sleep 60
echo "\n" >> $logDir/upload_etldata_60_$today.log
done
