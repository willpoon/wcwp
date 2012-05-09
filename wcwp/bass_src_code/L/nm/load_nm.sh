#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
EXEC_PATH=/bassdb1/etl/L/nm
WORK_PATH=/bassdb1/etl/L/nm
BAK_PATH=${EXEC_PATH}/backup
ERR_PATH=${EXEC_PATH}/error

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}


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

#取得加载进程个数,并删除停止文件标志
if_run=`ps -ef|grep load_nm.sh|grep -v grep|wc -l`
if [ -f ${EXEC_PATH}/stop_load_nm ] ; then
   rm ${EXEC_PATH}/stop_load_nm	
fi

#参数判断
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "load_nm.sh已经在运行,不能复复执行!"
            exit	
        fi	
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "stop" > ${EXEC_PATH}/stop_load_nm
            echo "已生成文件停止标志,load_nm.sh程序执行完当前任务后即退出,请等待!"
            exit
        else	
            echo "没有load_nm.sh在运行!"
            exit	
        fi 
   else
        echo "错误的运行方式,正确的执行方式为:load_nm.sh [start|stop]"
   fi	 
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 1 ] ; then
            echo "load_nm.sh已经在运行,不能复复执行!"
            exit	
   fi		
else
   echo "错误的运行方式,正确的执行方式为:load_nm.sh [start|stop]"
   exit
fi

#主SHELL
while [ true ]
do
        
        #若停止标志文件存在，则程序退出
        if [ -f ${EXEC_PATH}/stop_load_nm ]; then
            rm ${EXEC_PATH}/stop_load_nm
            break
        fi
       
        today=`date '+%Y%m%d'`
        
        #today=20090329
        echo $today
        sday=`yesterday $today`
        echo $sday
        
        #取得相应的数据文件和校验文件
        get_nm.sh ${WORK_PATH}/nm_data/${today}/DATA  "*.AVL" ${today}
        get_nm.sh ${WORK_PATH}/nm_data/${today}/CHK   "*.CHK" ${today}        

          
        #循环加载每个数据文件
        cd ${WORK_PATH}
        ls -l *.AVL |awk '{print $9}' > ./file.lst
        while read sfilename
        do    
              
              
              echo $sfilename
              #若停止标志文件存在,退出循环
	      if [ -f ${EXEC_PATH}/stop_load_nm ]; then
		     break
	      fi
	      
              #解析文件名,生成接口单元名,月份,粒度标志,调度代码等
              task_id=`echo ${sfilename}|cut -c1-6`
              month_id=`echo ${sfilename}|cut -c7-12`
              time_id=`echo ${sfilename}|cut -c7-14` 
              flag=`echo ${sfilename}|cut -c1-1`
              chk_file_name=`echo ${sfilename}|cut -c1-20`
              chk_file_name=${chk_file_name}".CHK"
              control_code=`echo ${sfilename}|cut -c2-6`
              control_code="TR1_L_${control_code}"
              
              if [ "$flag" = "M" ]; then
              	  scycleid=`echo $month_id`
              else
                  scycleid=`echo $time_id`
              fi	

              #判断对应的校验文件是否存在
              if [ ! -f ${WORK_PATH}/${chk_file_name} ]  ; then
              	  mv  ${WORK_PATH}/${sfilename} ${ERR_PATH}
                  echo "数据文件${sfilename}对应的校验文件不存在!"
              	  continue
              fi
              
              
              echo "=====>11111"
              #added by LiZhanyong 2008-10-14 ,增加对校验文件为空时的判断处理
              #判断对应的校验文件是否为空
              chk_file_num=`wc -l ${WORK_PATH}/${chk_file_name}|awk '{print $1}'`
              if [ ${chk_file_num} -eq 0 ]  ; then
              	  mv  ${WORK_PATH}/${sfilename} ${ERR_PATH}
              	  mv  ${WORK_PATH}/${chk_file_name} ${ERR_PATH}
                  echo "数据文件${sfilename}对应的校验文件为空!" 
              	  continue
              fi	
              #added end
              
              #比较数据文件大小与校验文件中记录的大小是否一致
              fact_size=`ls -l ${WORK_PATH}/${sfilename}|awk '{print $5}'`             
              
              
              chk_size=`grep ${sfilename} ${WORK_PATH}/${chk_file_name}`             
              
              
              chk_size=`echo ${chk_size}|cut -d',' -f2`  
              
                           
              if [ ${fact_size} -ne ${chk_size} ] ; then
              	  mv  ${WORK_PATH}/${sfilename}     ${ERR_PATH}
                  mv  ${WORK_PATH}/${chk_file_name} ${ERR_PATH}
                  echo "${sfilename}实际大小与校验文件中大小不相等${fact_size}!=${chk_size}"
              	  continue
              fi	
	      
	      
	      
              #比较数据文件记录行数与校验文件中记录数是否一致
              fact_rec_num=`wc -l ${WORK_PATH}/${sfilename}|awk '{print $1}'`              
              chk_rec_num=`grep ${sfilename} ${WORK_PATH}/${chk_file_name}`
              chk_rec_num=`echo ${chk_rec_num}|cut -d',' -f3`  
              if [ ${fact_rec_num} -ne ${chk_rec_num} ] ; then
              	  mv  ${WORK_PATH}/${sfilename}     ${ERR_PATH}
                  mv  ${WORK_PATH}/${chk_file_name} ${ERR_PATH}
                  echo "${sfilename}实际记录数与校验文件中记录不相等${fact_rec_num}!=${chk_rec_num}"
              	  continue
              fi
              
              #added by lizhanyong 2008-10-16	 数据质量管理
	     file_time=`ls -trl $sfilename |awk '{print $8}`        
	     /bassdb1/etl/L/interface_control.sh $task_id $file_time $fact_rec_num $fact_size
	     
	     #added end 
        	
              #获取倒入模板表名、接口名称、入库方式
              DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id like '${task_id}'\""
              table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id like '${task_id}'\""
              task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id like '${task_id}'\""
              load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`	
             
              #加载文件${sfilename},并更新日志为开始
              DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""   
              DB2_SQL_EXEC > /dev/null
              
              #通过模板表名生成对应的表名
              if [ "$table_name_templet" = "" ]; then
        	    ALARM_CONTENT="接口${task_id}没有配置入库信息！"
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_nm.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC > /dev/null
                    echo ${ALARM_CONTENT}
                    mv ${sfilename}     ${ERR_PATH}
                    mv ${chk_file_name} ${ERR_PATH}
        	    continue
              else
                     table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
        	     table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
              fi	
              
              DB2_SQLCOMM="db2 \"delete from ${table_name} \""
              DB2_SQL_EXEC  > /dev/null 		
              		
              sed 's/,/$/g' ${sfilename} >  ${sfilename}.load
              
              #2008-11-03 配合数据质量管理，修改此部分程序，在表BASS2.ETL_TASK_RUNNING和BASS2.ETL_TASK_log中增加三个字段bass_tablename,avl_filename,chk_filename
              #记录加载日志
              echo table_name=$table_name
              echo sfilename=$sfilename
              echo chk_file_name=$chk_file_name
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$sfilename','$chk_file_name','$task_name',-1,'ETL',current timestamp,'A')\""
	      DB2_SQL_EXEC > /dev/null
	      #2008-11-03 修改结束
		
              #导入数据load_method="0"就用LOAD,load_method=1就用import
              if [ $load_method = "0" ]; then
                      echo "LOADING ..."
                      DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${sfilename}.load of del modified by coldel$ timestampformat=\\\"YYYYMMDDHHMMSS\\\" fastparse anyorder warningcount 1000  insert into ${table_name}\""
                      DB2_SQL_EXEC 
                      echo "LOADED"
              else
              	      echo "IMPORT ..."
              	      DB2_SQLCOMM="db2 \"import from ${WORK_PATH}/${sfilename}.load of del modified by coldel$ timestampformat=\\\"YYYYMMDDHHMMSS\\\" messages /dev/null insert into ${table_name}\""
              	      DB2_SQL_EXEC 
              	      echo "IMPORTED"
              fi	
              
              rm ${sfilename}.load		
              
              #判断加载是否成功
              DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
              loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              rec_cnt=`wc -l ${sfilename} |awk '{print $1}'`
              
              #记录加载日志
              if [ "${rec_cnt}" = "${loaded_cnt}" ] ; then
              	    echo "${sfilename}加载成功,记录数为:${rec_cnt},并更新日志为完成!"
              	    error_cnt=`expr $rec_cnt - $loaded_cnt`
                    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$rec_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    #DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
                    mv ${sfilename}     ${BAK_PATH}
                    mv ${chk_file_name} ${BAK_PATH}
              else
                    ALARM_CONTENT="${sfilename}加载失败,加载记录数:${loaded_cnt},文本记录数:${rec_cnt},更新日志失败并告警!"	
                    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_nm.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC  > /dev/null
                    echo ${ALARM_CONTENT}
                    mv ${sfilename}     ${ERR_PATH}
                    mv ${chk_file_name} ${ERR_PATH}
              fi      
        
        done < ./file.lst 
       
        sleep 300
        
done

echo "load_nm.sh程序正常退出!"
exit