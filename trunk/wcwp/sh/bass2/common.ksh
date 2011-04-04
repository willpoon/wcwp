yesterday()
{
	#usage:yesterday yyyymmdd
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

#得到某月最后一天
getlastday()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`

        month=`expr $month + 0`

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

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

#子字符串替换函数
ReplaceAllSubStr()
{
	echo $1 > sed$$.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
	rm sed$$.temp
	echo $sReturnString
	return 1
}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
echo `date` >> /bassdb1/etl/L/boss/db2_connect.log
echo $DB2_SQLCOMM >> /bassdb1/etl/L/boss/db2_connect.log

conn_cnt=0
while [ $conn_cnt -lt 3 ] 
do
	db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
	if [ $? != 0 ] ; then
		echo "数据库连接失败！" >> /bassdb1/etl/L/boss/db2_connect.log
		conn_cnt=`expr ${conn_cnt} + 1`
		echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/boss/db2_connect.log
	else
		echo "数据库连接成功！" >> /bassdb1/etl/L/boss/db2_connect.log
		break
	fi
done
eval $DB2_SQLCOMM

db2 commit
db2 terminate
}

#生成加载结果文件
build_result_file()
{
    file_name=$1
    flag=`echo "$file_name"|cut -c1-1`
    
    if [ $flag = "M" ] ; then
        file_date=`echo "$file_name"|cut -c7-12`
    else
        file_date=`echo "$file_name"|cut -c7-14`
    fi 
    
    if [ -f ${WORK_PATH}/log/${file_date}.log ] ; then
         echo "$1,$2" >> ${WORK_PATH}/log/${file_date}.log
    else 
         echo "$1,$2" >  ${WORK_PATH}/log/${file_date}.log
    fi
}


##!/usr/bin/ksh
getxlsdata(){
				if [ $# -ne 2 ];then
				echo getxlsdata [inputfile] [yyyymm]
				return 2
				fi
				inputfile=$1
				tmp_voice_outputfile=fmt_voice_${data_mon}.tmp
				tmp_sms_outputfile=fmt_sms_${data_mon}.tmp
				voice_outputfile=load_voice_${data_mon}.txt
				sms_outputfile=load_sms_${data_mon}.txt
				data_mon=$2
				#step1:copy 28*p  data
				#step2:format data 0.000,save as voice_yyyymm.csv
				#step3:upload to unix
				#step4:run the following program!
				#step5:re-paste -d "," to excel (do some checksum)
				#step6:load data into tmp-table
				#step7:insert data into g_s_05001_month/g_s_05002_month
				#voice
				nawk 'BEGIN{FS=",";OFS=","}{print $1,$2,$4*1000,$3*1000}'  $inputfile >$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $5,$6,$8*1000,$7*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $9,$10,$12*1000,$11*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6,$7}' dim.txt > dim_voice.tmp
				paste -d "," dim_voice.tmp $tmp_voice_outputfile > $voice_outputfile
				#sms
				nawk 'BEGIN{FS=",";OFS=","}{if($14 != 0){print $14,$13,$16*1000,$15*1000}}'  $inputfile >$tmp_sms_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6}' dim_sms.txt > dim_sms.tmp
				paste -d "," dim_sms.tmp $tmp_sms_outputfile > $sms_outputfile
				#rm tmp files
				rm *.tmp
				#print result
				cat $voice_outputfile
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				cat $sms_outputfile
				db2 terminate
				db2 connect to BASSDB56 user bass2 using bass2
				echo db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				echo db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 connect reset
				db2 terminate
}
