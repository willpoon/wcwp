#!/bin/sh

#配置信息
DB2_OSS_DB="BASSDB"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
EXEC_PATH=/bassdb1/etl/L/ota
WORK_PATH=/bassdb1/etl/L/ota
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}


#接口定长信息
INTERFACE_LEN_950001="1 6,7 46,47 48,49 53,54 54,55 68"
INTERFACE_LEN_950002="1 14,15 15,16 55,56 74"
INTERFACE_LEN_950003="1 14,15 15,16 16,17 56,57 75,76 76"
INTERFACE_LEN_950004="1 14,15 16,17 21,22 37,38 39,40 42,43 44,45 544"



#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM
    db2 commit
    db2 connect reset
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

#求取上月的日期
lastmonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        month=`expr $month - 1`
        if [ $month -eq 0 ]; then
             month=12
             year=`expr $year - 1`
        fi
        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        echo $year$month
        return 1
}

#取得加载进程个数,并删除停止文件标志
if_run=`ps -ef|grep load_ota.sh|grep -v grep|wc -l`
if [ -f ${EXEC_PATH}/stop_load_ota ] ; then
   rm ${EXEC_PATH}/stop_load_ota	
fi

#参数判断
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "load_ota.sh已经在运行,不能复复执行!"
            exit	
        fi	
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "stop" > ${EXEC_PATH}/stop_load_ota
            echo "已生成文件停止标志,load_ota.sh程序执行完当前任务后即退出,请等待!"
            exit
        else	
            echo "没有load_ota.sh在运行!"
            exit	
        fi 
   else
        echo "错误的运行方式,正确的执行方式为:load_ota.sh [start|stop]"
   fi	 
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 1 ] ; then
            echo "load_ota.sh已经在运行,不能复复执行!"
            exit	
   fi		
else
   echo "错误的运行方式,正确的执行方式为:load_ota.sh [start|stop]"
   exit
fi

get_row()
{
  curr_row="0"
  chk_file_row=`wc -l $1|awk '{print $1}'`
  data_file_row="0"
  while read srec
  do
       single_file_row=`echo ${srec}|cut -d'|' -f5`
       data_file_row=`expr $data_file_row + $single_file_row`
       curr_row=`expr $curr_row + 1`
       if [ "${chk_file_row}" = "${curr_row}" ]; then
            echo "$data_file_row"
            return 1	
       fi
  done < $1
}

load_data()
{
  chk_file_name=$1
  rec_len_val=$2
  load_table_name=$3
  curr_row="0"
  while read srec
  do
       curr_row=`expr $curr_row + 1`
       date_file_name=`echo ${srec}|cut -d'|' -f1`
       if [ "${chk_file_row}" != "${curr_row}" ]; then
              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${date_file_name} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" method L (${rec_len_val}) insert into ${load_table_name} nonrecoverable\""
              DB2_SQL_EXEC
       fi
  done < $1
}

#主SHELL
while [ true ]
do
        #若停止标志文件存在，则程序退出
        if [ -f ${EXEC_PATH}/stop_load_ota ]; then
            rm ${EXEC_PATH}/stop_load_ota
            break
        fi
        
        today=`date '+%Y%m%d'`
        sday=`yesterday $today`
        #sday=20090220
        smonth=`lastmonth $today`
        echo $smonth
        
        #./ftp_ota.sh   "/OTA_DATA/950001/${sday}/chk"    "A*.CHK"
        #./ftp_ota.sh   "/OTA_DATA/950001/${sday}/data"   "A*.AVL"
        #./ftp_ota.sh   "/OTA_DATA/950002/${sday}/chk"    "A*.CHK"
        #./ftp_ota.sh   "/OTA_DATA/950002/${sday}/data"   "A*.AVL"
        #./ftp_ota.sh   "/OTA_DATA/950003/${sday}/chk"    "A*.CHK"
        #./ftp_ota.sh   "/OTA_DATA/950003/${sday}/data"   "A*.AVL"
        #./ftp_ota.sh   "/OTA_DATA/950004/${sday}/chk"    "A*.CHK"
        #./ftp_ota.sh   "/OTA_DATA/950004/${sday}/data"   "A*.AVL"
        #./ftp_ota.sh   "/OTA_DATA/950004/${smonth}/chk"  "M*.CHK"
        #./ftp_ota.sh   "/OTA_DATA/950004/${smonth}/data" "M*.AVL"

        cd $WORK_PATH
        while read schkfilename
        do
              
              if [ "${schkfilename}" = "" ] ; then
                  break
              fi
              
              #获取任务代码,接口代码,调度任务名称,记录定长信息,及数据大小和
              chk_file_name=${schkfilename}
              task_id=`echo "$schkfilename"|cut -c1-7`
              flag=`echo "$schkfilename"|cut -c1-1`
              interface_code=`echo "$schkfilename"|cut -c2-7`
              control_code="TR1_L_${task_id}"
              len_val=`grep "INTERFACE_LEN_${interface_code}" ${EXEC_PATH}/load_ota.sh |cut -d= -f2`
              month_id=`echo ${chk_file_name}|cut -c8-13`
              time_id=`echo ${chk_file_name}|cut -c8-15`
              
        #added by lizhanyong 2008-10-17	 数据质量管理
         rec_cnt=`wc -l ${task_id}${time_id}*.AVL|awk '{print $1}'`
	 file_time=`ls -trl ${task_id}${time_id}*.AVL |awk '{print $8}` 
	 avl_byte=`ls -trl ${task_id}${time_id}*.AVL |awk '{print $5}`            
	/bassdb1/etl/L/interface_control.sh $task_id $file_time $rec_cnt $avl_byte
	     
	#added end 
              
              if [ "$flag" = "M" ]; then
              	  scycleid=`echo $month_id`
              	  
              else
                  scycleid=`echo $time_id`
              fi	
              
              #获取倒入模板表名、接口名称、入库方式
              if [ "${interface_code}" = "950004"  ] ; then
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	            table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	            task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	            load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`	
	      else
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id like '_${interface_code}'\""
	            echo ${DB2_SQLCOMM}
	            table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id like '_${interface_code}'\""
	            task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	            DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id like '_${interface_code}'\""
	            load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`	
	      fi    
	        
                
              #判断是否存在配置
              if  [ "${table_name_templet}" = "" ] ; then
                     mv  ${WORK_PATH}/${task_id}* ${BACKUP_PATH}
                     echo "${DB2_SQLCOMM}"
                     echo "${chk_file_name}配置信息不存在!"
                     continue
              else
                     table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
        	     table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
              fi
              
              #判断是否存在配置
              if  [ "${len_val}" = "" ] ; then
                     mv  ${task_id}* ${BACKUP_PATH}
                     ehco "${chk_file_name}定长配置信息不存在!"
                     continue
              fi	
              
              #清空数据表
	      DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
	      DB2_SQL_EXEC > /dev/null

              #更新运行日志状态为开始
	      DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
	      DB2_SQL_EXEC > /dev/null
	      
	      #2008-11-03 配合数据质量管理，修改此部分程序，在表BASS2.ETL_TASK_RUNNING和BASS2.ETL_TASK_log中增加三个字段bass_tablename,avl_filename,chk_filename
	      #记录加载日志
	      avl_filename=`ls -trl ${task_id}${time_id}*.AVL |awk '{print $9}` 
	      chk_filename=`ls -trl ${task_id}${time_id}*.CHK |awk '{print $9}`	       
	      echo table_name=$table_name
	      echo avl_filename=$avl_filename
	      echo chk_filename=$chk_filename
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$avl_filename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
	      DB2_SQL_EXEC > /dev/null  
	      #2008-11-03 修改结束

              echo "加载${sfilename}数据开始(${chk_file_name},${table_name})!" 
              load_data "${WORK_PATH}/${chk_file_name}"  "${len_val}" "${table_name}"
              echo "加载${sfilename}数据完成!"
              
              #加载记录数
              DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
              load_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              fact_cnt=`get_row ${chk_file_name}`

              #判断加载是否成功
              if [ "${load_cnt}" != "${fact_cnt}" ] ; then
                    ALARM_CONTENT="${sfilename}加载失败,加载记录数:${load_cnt},文本记录数:${fact_cnt},更新日志失败并告警!"	
                    echo $ALARM_CONTENT
                    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
		    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
		    DB2_SQL_EXEC  > /dev/null
		    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_ota.sh',1,'${sfilename}加载失败',current timestamp,-1)\""
		    DB2_SQL_EXEC > /dev/null
		    mv  ${task_id}${time_id}*.AVL ${ERROR_PATH}
                    mv  ${chk_file_name}          ${ERROR_PATH}
              else
                    echo "${sfilename}加载成功,记录数为:${fact_cnt},并更新日志为完成!"
              	    error_cnt=`expr $fact_cnt - $load_cnt`
                    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$fact_cnt,load_sheet_cnt=$load_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    #DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""   
		    DB2_SQL_EXEC  > /dev/null
		    mv  ${task_id}${time_id}*.AVL ${BACKUP_PATH}
                    mv  ${chk_file_name}          ${BACKUP_PATH}
              fi
                            
        done<<!
	        `ls -l *.CHK |awk '{print $9}' `
!
        sleep 600

done
