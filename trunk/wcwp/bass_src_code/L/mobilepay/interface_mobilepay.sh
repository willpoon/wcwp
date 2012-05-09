#!/bin/ksh
#*******************************************************
#函数名：interface_mobilepay.sh
#功  能：获取手机支付接口信息，并加载入库
#参  数： 
#author: zhaolp2      
#日  期: 2010-07-28	      
#输  出: 
#*******************************************************


#配置信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

EXEC_PATH=/bassdb1/etl/L/mobilepay
WORK_PATH=/bassdb1/etl/L/mobilepay
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`






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

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
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




today=`date '+%Y%m%d'`
sday=`yesterday $today`


if [ $# -eq 1 ] ; then
   sday=$1
fi


#接口定长信息
INTERFACE_LEN_FHIST="1 3,4 11,12 22,23 26,27 37,38 56,57 77,78 84,85 87,88 88,89 96,97 102,103 105,106 120,121 121,122 129,130 249,250 254,255 280"
INTERFACE_LEN_OPEN="1 3,4 11,12 22,23 41,42 49,50 54,55 74"
INTERFACE_LEN_CLOSE="1 3,4 11,12 22,23 41,42 49"
INTERFACE_LEN_MPCD="1 3,4 14,15 16,17 46,47 49,50 79"


set -A taskids "AOPEN" "ACLOSE" "AFHIST" "IMPCD"


i=0
for task_id in ${taskids[*]}
do
	
	
	echo $task_id
	
	length=`echo $task_id | awk -F '{print length($0)}'`
	
	tmp_task_id=$task_id
	
	task_id=`echo $task_id | cut -c2-${length}`
	
	
	
	filename=${task_id}_25210100_${sday}.dat
	chkfilename=${task_id}_25210100_${sday}.CFG
	control_code="TR1_L_${task_id}"
	#接口文件不存在，通过10.233.26.55获取接口文件
	while [ true ]
	do 
		if [ ! -f ${BACKUP_PATH}/$filename ]; then 
			rsh 10.233.26.55 -l xzgwjk /data1/interface/mobilepay/sftp_mobilepayfile.sh $filename
		fi
		
		if [ -f ${WORK_PATH}/$filename ]; then 
			break
		else 
		  mv ${BACKUP_PATH}/$filename ${WORK_PATH}/
		  if [ ! -f ${WORK_PATH}/$filename ]; then 
				sleep 1200
			else 
				break			
			fi	
		fi
	done
	
	#如果接口文件存在，执行加载动作
	if [ -f ${WORK_PATH}/$filename ]; then
	
		len_value="INTERFACE_LEN_${task_id}"
		
		
		len_value=`eval 'echo $'$len_value`
		
		
		file_rows_fact=`wc -l ${WORK_PATH}/${filename}|awk '{print $1}'`
		
		cd ${WORK_PATH}
		
		#获取倒入模板表名、接口名称、入库方式
		DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$tmp_task_id'\""
		table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
		DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$tmp_task_id'\""
		task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`	
		
		#通过模板表名生成对应的表名
		if [ "$table_name_templet" = "" ]; then
		    mv ${filename} ${TOLOAD_PATH}
		    mv ${chkfilename}  ${TOLOAD_PATH}            
		    ALARM_CONTENT="接口${task_id}没有配置入库信息!"
		    echo ${ALARM_CONTENT}
		    continue
		else
		    table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $sday`
		fi		
		
		
		DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$sday','$table_name','$filename','$chkfilename','$task_name',-1,'ETL',current timestamp,'A')\""
	        DB2_SQL_EXEC > /dev/null
		#加载文件${filename},并更新日志为开始
		DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""   
		DB2_SQL_EXEC > /dev/null
		
		DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/$filename of asc modified by dateformat=\\\"YYYYMMDD\\\"  method L (${len_value}) replace into ${table_name} \""
		echo $DB2_SQLCOMM;
		DB2_SQL_EXEC
		
		DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
		loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
		
		if [ "${loaded_cnt}" = "${file_rows_fact}" ] ; then
			echo "${filename}加载成功,记录数为:${file_rows_fact},并更新日志为完成"
			error_cnt=`expr $file_rows_fact - $loaded_cnt`
			mv ${filename} ${BACKUP_PATH}
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$file_rows_fact,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$sday'\""
			DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$sday'\""
			DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$sday'\""
			DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC  > /dev/null
		else
			ALARM_CONTENT="${filename}加载失败,加载记录数:${loaded_cnt},文本记录数:${file_rows_fact},并更新日志为失败,并告警"	
			echo ${ALARM_CONTENT}
			mv ${filename} ${ERROR_PATH}
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$sday'\""
			DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC  > /dev/null
			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_dsmp.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			DB2_SQL_EXEC  > /dev/null
		fi		
		
	fi

	
done

