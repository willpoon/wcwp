#!/bin/ksh
#*******************************************************
#函数名：load_boss.sh
#功  能：获取BOSS|CRM接口信息，并加载入库
#参  数：无 
#author:       
#日  期: 
#输  出: 
#修改记录：
#		2011-04-18	zhaolp2		增加接口表runstats
#2012-02-22 modified by 孔祥然，根据2012-02-16的应急切换会议修改，由校验记录数与入库记录数（或文件记录数）不等即告警改为相差10条之上才告警
#		2012-03-06	zhaolp2		将程序修改为多线程
#*******************************************************

#最大子进程数
_MAX_PROCESS=2

#1、数据库连接信息(全局变量)
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
DB_SCHEMA="bass2"
DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo $DB2_OSS_PASSWD


#2、路径信息
PROGRAM_NAME="load_boss_nfjd.sh"
WORK_PATH=/bassdb1/etl/L/boss
TOLOAD_PATH=${WORK_PATH}/toload
ERROR_PATH=${WORK_PATH}/error
BAK_PATH=${WORK_PATH}/backup
STOP_FILE="stop_load_boss"
RUNNING_FILE="running_load_boss"

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
	_TBNAME=$1
	echo ${_TBNAME} > sed${_TBNAME}.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed${_TBNAME}.temp`
	rm sed${_TBNAME}.temp
	echo $sReturnString
	return 1
}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
	SQLBUFF=$1
	echo `date` >> /bassdb1/etl/L/boss/db2_connect.log
	echo $SQLBUFF >> /bassdb1/etl/L/boss/db2_connect.log
	
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
	eval $SQLBUFF
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

#加载接口文件
load_data()
{
	sfilename=$1	
	echo "`date`: 接口${sfilename}开始加载........."
	today=`date '+%Y%m%d'`
        sday=`yesterday $today`

	#2.1、获取接口代码和周期
	task_id=`echo "$sfilename"|cut -c1-6`
	avl_filename=`echo "$sfilename"|cut -c1-24`
	load_avl_filename=`echo "$avl_filename".load`
	load_sfilename=`echo "$load_avl_filename".Z`
		
	pure_filename=`echo "$sfilename"|cut -c1-20`
	chk_filename=`echo "$pure_filename".CHK`
	sfrequency=`echo "$sfilename"|cut -c1-1`
	
	echo sfrequency=$sfrequency
	
	if [ $sfrequency = "M" ]; then
		smonth=`echo "$sfilename"|cut -c7-12`
		sday=""
		scycleid=`echo $smonth`
			
		#2.1.1、判断是否是回退月接口
		DB2_SQLCOMM="db2 \"select 'xxxxxx',TASK_ID_DAY from bass2.ETL_ROLLBACK_TASK_MAP where TASK_ID_MONTH='$task_id'\""
		sTASK_ID_DAY=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
		if [ "$sTASK_ID_DAY" != "" ]; then
			task_id=$sTASK_ID_DAY
			sday=`getlastday $smonth`
			scycleid=`echo $sday`
		fi
	else
		sday=`echo "$sfilename"|cut -c7-14`
		smonth=`echo "$sday"|cut -c1-6`
		scycleid=`echo $sday`
		
		#2.1.2、判断是否是要回退的日接口
                if [ $scycleid = `getlastday $smonth` ]; then
			DB2_SQLCOMM="db2 \"select 'xxxxxx',TASK_ID_DAY from bass2.ETL_ROLLBACK_TASK_MAP where TASK_ID_DAY='$task_id'\""
			sTASK_ID_DAY=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
			
			if [ "$sTASK_ID_DAY" != "" ]; then
				#2.1.2.1、备份接口文件
				mv $sfilename $BAK_PATH 
				mv $chk_filename $BAK_PATH
				continue
			fi
		fi
	fi
	
	date
	
	#2.2、获取倒入模板表名、接口名称、入库方式
	DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	table_name_templet=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	task_name=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	load_method=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	
	
	#2008-11-03 配合数据质量管理，修改此部分程序，在表BASS2.ETL_TASK_RUNNING和BASS2.ETL_TASK_log中增加三个字段bass_tablename,avl_filename,chk_filename
	#2.3.3.2、已配置倒入模板表
	echo $table_name_templet
	tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $sday`
	tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $smonth`
         
        echo tablename=$tablename
        echo avl_filename=$avl_filename
        echo chk_filename=$chk_filename
	
	#2.3、倒入任务开始
	#2.3.1、删除已有任务
	DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#2.3.2、增加新的任务
	DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$tablename','$avl_filename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
	echo DB2_SQLCOMM=$DB2_SQLCOMM
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#2008-11-03修改结束
	
	#2.3.2、更新调度日志表
	control_code=`echo $task_id|cut -c2-6`
	control_code=`echo TR1_L_$control_code`
	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#add by zhaolp2 20090608   增加校验文件是否存在的判断,不增加以下代码会出现在校验文件未生成或者传输错误的情况下，程序会一直阻塞，导致后续接口不能进行加载
	#判断对应的校验文件是否存在
	if [ ! -f ${WORK_PATH}/${chk_filename} ]  ; then
		echo "数据文件${sfilename}对应的校验文件不存在!"
		sleep 30
		continue
	fi
	
	if [ "$table_name_templet" = "" ]; then
		#2.3.3.1、没配置倒入模板表
		echo "接口$task_id没有配置入库信息！"
		DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='接口没有配置入库信息！' where task_id='$task_id' and cycle_id='$scycleid'\""
		DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null

		#2.3.3.11、备份接口文件
		mv $sfilename $TOLOAD_PATH
		mv $chk_filename $TOLOAD_PATH
	else
	
		#2.3.3.2、已配置倒入模板表  调整此部分到上面
		#tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $sday`
		#tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $smonth`
		
		#2.3.3.3、生成接口临时处理文件
		cp $sfilename $load_sfilename
		
		#2.3.3.4、解压缩文件
		uncompress -f $load_sfilename
					
		#2.3.3.5、核对行数
		end_cnt=`grep AVL $chk_filename |awk -F$ '{print $3}'`
		rec_cnt=`wc -l $load_avl_filename |awk '{print $1}'`
		
		avl_byte=`ls -trl $load_avl_filename |awk '{print $5}'`   
		file_time=`ls -trl $load_avl_filename |awk '{print $8}'`
		
		#added by lizhanyong 2008-09-07	 数据质量核查
				
   		#java -Djava.ext.dirs=/bassdb1/etl/L/quality/interface/lib -Xms256m -Xmx512m com.carnation.qualityone.rpc.interfaceCheck $avl_filename  
   		cd $WORK_PATH
                                     
    		#added end	
              
              	#added by lizhanyong 2008-10-16	 数据质量管理
              
     		/bassdb1/etl/L/interface_control.sh $task_id $file_time $rec_cnt $avl_byte
     
     		cd $WORK_PATH
     
              	#added end    		
								
                  
		if [ $end_cnt -ne $rec_cnt ]; then
		
			ALARM_CONTENT=`echo BOSS接口告警信息：接口名称为${task_name}，接口文件${sfilename}和${chk_filename}文件记录行数不符！`
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set ERROR_MSG='${ALARM_CONTENT}' where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			
			#插入调度告警表
			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			
		fi
		
		
		date
				
		#2.3.3.7、倒入数据load_method="0"就用LOAD，load_method=1就用import
		if [ $load_method = "0" ]; then
			echo "LOADING ..."
			DB2_SQLCOMM="db2 \"load client from $WORK_PATH/$load_avl_filename of del modified by coldel$ timestampformat=\\\"YYYYMMDDHHMMSS\\\" fastparse anyorder warningcount 1000 messages /dev/null replace into $tablename\""				
			DB2_SQL_EXEC "$DB2_SQLCOMM"
			echo ${DB2_SQLCOMM}
		else
			echo "IMPORT ..."
			DB2_SQLCOMM="db2 \"import from $WORK_PATH/$load_avl_filename of del modified by coldel$ timestampformat=\\\"YYYYMMDDHHMMSS\\\"  replace into $tablename\""				
			DB2_SQL_EXEC "$DB2_SQLCOMM"
		fi
		
		DB2_SQLCOMM="db2 \"runstats on table ${DB_SCHEMA}.${tablename} on all columns and indexes all\""	
		echo $DB2_SQLCOMM			
		DB2_SQL_EXEC "$DB2_SQLCOMM"
		
		date			

		#2.3.3.8、记录倒入记录条数信息，LOAD方式取LOG文件中的信息，IMPORT方式取结果表记录条数
		echo "GET LOAD RECORD"
		DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from bass2.$tablename\""			
		loaded_cnt=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
		echo ${DB2_SQLCOMM}

		#2.3.3.9、根据判断导入是否正确记录相应的日志(在此需要用户字符串比较方式,否则查询失败用数字比较将出错)
		if [ "${rec_cnt}" = "${loaded_cnt}" ] ; then
			#2.3.3.9.1、导入正确
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$end_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			#DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select * from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC "$DB2_SQLCOMM"
			build_result_file "$sfilename" "正确"
		else
			if [ "${loaded_cnt}" = "" ] ; then
				#2.3.3.9.2、导入出错(从表中查询记录数失败,一般是表异常,因此可以确定导入也失败了)
				ALARM_CONTENT=`echo 文件${sfilename}导入失败,查询导入记录数失败而导致！`
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$end_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				cp $sfilename    $ERROR_PATH
				cp $chk_filename $ERROR_PATH
				build_result_file "$sfilename" "失败"
			else
	                	error_cnt=`expr $end_cnt - $loaded_cnt`
	                	if [ $end_cnt -ne 0 -a $loaded_cnt -eq 0 ]; then
		                	#2.3.3.9.3、导入全部出错
		                	ALARM_CONTENT=`echo 接口文件:$sfilename倒入记录全部失败,原因未明！`
		                	DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$end_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	cp $sfilename    $ERROR_PATH
		                        cp $chk_filename $ERROR_PATH
		                        build_result_file "$sfilename" "失败"
	                	else
		                	#2.3.3.9.4、导入部分出错(此时将告警,告不影响调度执行)
		                	
		                	# 根据2012-02-16应急方案讨论，当校验文件值与加载数据文件值之差在[0,10]区间，允许后续程序继续执行
		                	if [ $error_cnt -gt 10 -o $error_cnt -lt 0 ]; then	              	
		                		DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
		                		DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
					else 
			                	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""   
			                	DB2_SQL_EXEC "$DB2_SQLCOMM"								                		
		                	fi
		                	
		                	DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$end_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select * from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null

		                	ALARM_CONTENT=`echo 接口文件:$sfilename导入时部分失败,失败记录数为${error_cnt}!`
		                	DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	build_result_file "$sfilename" "失败"
	                	fi
			fi
		fi      
			#2.3.3.10、备份接口文件,删除接口临时处理文件
			mv $sfilename $BAK_PATH
			mv $chk_filename $BAK_PATH
			rm $load_avl_filename
	fi
	echo "`date`: 接口${sfilename}加载结束........."
}

#主shell
function main
{
	cd $WORK_PATH

	#处理运行参数
	if [ $# -eq 1 ] ; then
		if [  "$1" = "stop"   ] ; then
			if [ -f $RUNNING_FILE ] ; then
				touch $STOP_FILE
				echo "已生成停止标志文件,等待退出!"
				exit
			fi
			echo "程序没在运行！"
			exit
		fi
	fi
	
	if [ -f $RUNNING_FILE ] ; then
		echo "程序已运行！"
		exit
	fi
	
	
	touch $RUNNING_FILE
	while [ true ]
	do
		
		cd $WORK_PATH
		
		#若停止标志文件存在，则程序退出
		if [ -f $STOP_FILE ]; then
			rm $RUNNING_FILE
			rm $STOP_FILE
			break
		fi
		
		ls -l *.AVL.Z |awk '{print $9}' > ./$$
		echo `ls -l *.AVL.Z |awk '{print $9}'`
		
		while read sfilename
		do
			#2.0、若停止标志文件存在，则退出循环
			if [ -f $STOP_FILE ]; then
				break
			fi
		
			pid=$$
			#将子代理进程放入后台执行
			
			load_data $sfilename >> ${WORK_PATH}/log/${sfilename}.load.log & 
			
			#load_data $sfilename & 
			
			#判断该主进程发起了几个子代理进程,如果超过$_MAX_PROCESS,则等待其他进程结束
			while [ `ps -ef|awk '$3 == '$pid'' | wc -l` -gt $_MAX_PROCESS ] 
			do 
				echo "已达到最大进程数,等待空闲窗口..................."
			    	sleep 3	
			done 
			
			echo "子进程个数：" `ps -ef|awk '$3 == '$pid'' | wc -l`
		
		done < ./$$
		rm ./$$
		
		while [ `ps -ef|awk '$3 == '$pid'' | wc -l` -gt 1 ]         
	        do 
	        	echo echo "子进程个数：" `ps -ef|awk '$3 == '$pid'' | wc -l`
	        	#防止程序下一轮扫描目录时，上一轮的加载操作未完成，造成接口重复加载
	        	echo "子进程未完全结束,父进程退出等待..................."
	            	sleep 10
	        done 		
		
		
		echo "程序休眠60秒!"
		sleep 60
	done	
	echo "程序正常结束!"
}

main
