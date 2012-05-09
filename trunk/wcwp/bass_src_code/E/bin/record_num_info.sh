#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
cdr_dir=/bassdb1/etl/E/xfer/data

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 connect reset
}

#2010-03-03 增加记录到BOSS表
insert_boss()
{
sqlplus jyfx/j0y8f2z3@xzsale <<SQLEND
insert into kt.BASS_HD_STAT(STAT_DAY,GSM,GGPRS,GPRS,SMS,MMS,ISMG,KJAVA,STM,WAP,WLAN,ADDVALUE,HDSUM,CURR_DATE)
values('${sday}','${cdr_call_num}','0','0','${cdr_sms_num}','0','0','0','0','0','0','0','${hd_sum}',sysdate);
commit;
SQLEND
}

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

	today=`date '+%Y%m%d'`
        sday=`yesterday $today`
        #sday=20100305
#today=`date '+%Y%m%d'`
#today=20100121

  current_date=`date '+%Y-%m-%d'`


cdr_sms=DR_SMS_L_${sday}
cdr_call=DR_GSM_[GP,S]_${sday}
#ls -C1 | awk '$0 ~ /DR_GSM_[A-Z][A-Z]*_${sday}/'

cd ${cdr_dir}

cdr_sms_num=`ls -l ${cdr_sms}*|wc -l`
cdr_call_num=`ls -C1 | grep "^DR_GSM_[A-Z]\{1,2\}_${sday}" | wc -l`

hd_sum=`expr ${cdr_sms_num} + ${cdr_call_num}`

#将话单统计结果插入boss表kt.BASS_HD_STAT，待发出短信
#insert_boss
rsh 10.233.23.60 -l ifboss /bassdb2/etl/E/boss/bin/insert_boss_oracle.sh ${sday} ${cdr_call_num} ${cdr_sms_num} ${hd_sum}

#cdr_call_num=`ls -l ${cdr_call}|wc -l`
cdr_sms_record_num=`cat ${cdr_sms}* | wc -l`
#cdr_call_record_num=`cat ${cdr_call}* | wc -l`
cdr_call_record_num=`ls -C1 | grep "^DR_GSM_[A-Z]\{1,2\}_${sday}" | xargs cat | wc -l`

cdr_sms_record_num=`expr ${cdr_sms_record_num} / 10000`
cdr_call_record_num=`expr ${cdr_call_record_num} / 10000`

echo ${cdr_sms_num}
echo ${cdr_call_num}
echo ${hd_sum}
echo ${cdr_sms_record_num}
echo ${cdr_call_record_num}



DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '${sday}短信清单文件总数:${cdr_sms_num} 记录条数:${cdr_sms_record_num}万,语音清单总数:${cdr_call_num} 记录条数:${cdr_call_record_num}万!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
DB2_SQL_EXEC > /dev/null


