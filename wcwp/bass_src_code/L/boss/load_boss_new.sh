#!/bin/ksh
#*******************************************************
#��������load_boss.sh
#��  �ܣ���ȡBOSS|CRM�ӿ���Ϣ�����������
#��  ������ 
#author:       
#��  ��: 
#��  ��: 
#�޸ļ�¼��
#		2011-04-18	zhaolp2		���ӽӿڱ�runstats
#2012-02-22 modified by ����Ȼ������2012-02-16��Ӧ���л������޸ģ���У���¼��������¼�������ļ���¼�������ȼ��澯��Ϊ���10��֮�ϲŸ澯
#		2012-03-06	zhaolp2		�������޸�Ϊ���߳�
#*******************************************************

#����ӽ�����
_MAX_PROCESS=2

#1�����ݿ�������Ϣ(ȫ�ֱ���)
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
DB_SCHEMA="bass2"
DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo $DB2_OSS_PASSWD


#2��·����Ϣ
PROGRAM_NAME="load_boss_nfjd.sh"
WORK_PATH=/bassdb1/etl/L/boss
TOLOAD_PATH=${WORK_PATH}/toload
ERROR_PATH=${WORK_PATH}/error
BAK_PATH=${WORK_PATH}/backup
STOP_FILE="stop_load_boss"
RUNNING_FILE="running_load_boss"

#��ȡ���������
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

#�õ�ĳ�����һ��
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

#���ַ����滻����
ReplaceAllSubStr()
{
	_TBNAME=$1
	echo ${_TBNAME} > sed${_TBNAME}.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed${_TBNAME}.temp`
	rm sed${_TBNAME}.temp
	echo $sReturnString
	return 1
}

#��DB2���ݿ���ִ��SQL
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
			echo "���ݿ�����ʧ�ܣ�" >> /bassdb1/etl/L/boss/db2_connect.log
			conn_cnt=`expr ${conn_cnt} + 1`
			echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/boss/db2_connect.log
		else
			echo "���ݿ����ӳɹ���" >> /bassdb1/etl/L/boss/db2_connect.log
			break
		fi
	done
	eval $SQLBUFF
	db2 commit
	db2 terminate
}

#���ɼ��ؽ���ļ�
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

#���ؽӿ��ļ�
load_data()
{
	sfilename=$1	
	echo "`date`: �ӿ�${sfilename}��ʼ����........."
	today=`date '+%Y%m%d'`
        sday=`yesterday $today`

	#2.1����ȡ�ӿڴ��������
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
			
		#2.1.1���ж��Ƿ��ǻ����½ӿ�
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
		
		#2.1.2���ж��Ƿ���Ҫ���˵��սӿ�
                if [ $scycleid = `getlastday $smonth` ]; then
			DB2_SQLCOMM="db2 \"select 'xxxxxx',TASK_ID_DAY from bass2.ETL_ROLLBACK_TASK_MAP where TASK_ID_DAY='$task_id'\""
			sTASK_ID_DAY=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
			
			if [ "$sTASK_ID_DAY" != "" ]; then
				#2.1.2.1�����ݽӿ��ļ�
				mv $sfilename $BAK_PATH 
				mv $chk_filename $BAK_PATH
				continue
			fi
		fi
	fi
	
	date
	
	#2.2����ȡ����ģ��������ӿ����ơ���ⷽʽ
	DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	table_name_templet=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	task_name=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	load_method=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
	
	
	#2008-11-03 ����������������޸Ĵ˲��ֳ����ڱ�BASS2.ETL_TASK_RUNNING��BASS2.ETL_TASK_log�����������ֶ�bass_tablename,avl_filename,chk_filename
	#2.3.3.2�������õ���ģ���
	echo $table_name_templet
	tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $sday`
	tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $smonth`
         
        echo tablename=$tablename
        echo avl_filename=$avl_filename
        echo chk_filename=$chk_filename
	
	#2.3����������ʼ
	#2.3.1��ɾ����������
	DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#2.3.2�������µ�����
	DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$tablename','$avl_filename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
	echo DB2_SQLCOMM=$DB2_SQLCOMM
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#2008-11-03�޸Ľ���
	
	#2.3.2�����µ�����־��
	control_code=`echo $task_id|cut -c2-6`
	control_code=`echo TR1_L_$control_code`
	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
	
	#add by zhaolp2 20090608   ����У���ļ��Ƿ���ڵ��ж�,���������´���������У���ļ�δ���ɻ��ߴ�����������£������һֱ���������º����ӿڲ��ܽ��м���
	#�ж϶�Ӧ��У���ļ��Ƿ����
	if [ ! -f ${WORK_PATH}/${chk_filename} ]  ; then
		echo "�����ļ�${sfilename}��Ӧ��У���ļ�������!"
		sleep 30
		continue
	fi
	
	if [ "$table_name_templet" = "" ]; then
		#2.3.3.1��û���õ���ģ���
		echo "�ӿ�$task_idû�����������Ϣ��"
		DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='�ӿ�û�����������Ϣ��' where task_id='$task_id' and cycle_id='$scycleid'\""
		DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null

		#2.3.3.11�����ݽӿ��ļ�
		mv $sfilename $TOLOAD_PATH
		mv $chk_filename $TOLOAD_PATH
	else
	
		#2.3.3.2�������õ���ģ���  �����˲��ֵ�����
		#tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $sday`
		#tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $smonth`
		
		#2.3.3.3�����ɽӿ���ʱ�����ļ�
		cp $sfilename $load_sfilename
		
		#2.3.3.4����ѹ���ļ�
		uncompress -f $load_sfilename
					
		#2.3.3.5���˶�����
		end_cnt=`grep AVL $chk_filename |awk -F$ '{print $3}'`
		rec_cnt=`wc -l $load_avl_filename |awk '{print $1}'`
		
		avl_byte=`ls -trl $load_avl_filename |awk '{print $5}'`   
		file_time=`ls -trl $load_avl_filename |awk '{print $8}'`
		
		#added by lizhanyong 2008-09-07	 ���������˲�
				
   		#java -Djava.ext.dirs=/bassdb1/etl/L/quality/interface/lib -Xms256m -Xmx512m com.carnation.qualityone.rpc.interfaceCheck $avl_filename  
   		cd $WORK_PATH
                                     
    		#added end	
              
              	#added by lizhanyong 2008-10-16	 ������������
              
     		/bassdb1/etl/L/interface_control.sh $task_id $file_time $rec_cnt $avl_byte
     
     		cd $WORK_PATH
     
              	#added end    		
								
                  
		if [ $end_cnt -ne $rec_cnt ]; then
		
			ALARM_CONTENT=`echo BOSS�ӿڸ澯��Ϣ���ӿ�����Ϊ${task_name}���ӿ��ļ�${sfilename}��${chk_filename}�ļ���¼����������`
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set ERROR_MSG='${ALARM_CONTENT}' where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			
			#������ȸ澯��
			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			
		fi
		
		
		date
				
		#2.3.3.7����������load_method="0"����LOAD��load_method=1����import
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

		#2.3.3.8����¼�����¼������Ϣ��LOAD��ʽȡLOG�ļ��е���Ϣ��IMPORT��ʽȡ������¼����
		echo "GET LOAD RECORD"
		DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from bass2.$tablename\""			
		loaded_cnt=`DB2_SQL_EXEC "$DB2_SQLCOMM" | grep xxxxxx|awk '{print $2}'`
		echo ${DB2_SQLCOMM}

		#2.3.3.9�������жϵ����Ƿ���ȷ��¼��Ӧ����־(�ڴ���Ҫ�û��ַ����ȽϷ�ʽ,�����ѯʧ�������ֱȽϽ�����)
		if [ "${rec_cnt}" = "${loaded_cnt}" ] ; then
			#2.3.3.9.1��������ȷ
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$end_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			#DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select * from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
			DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC "$DB2_SQLCOMM"
			build_result_file "$sfilename" "��ȷ"
		else
			if [ "${loaded_cnt}" = "" ] ; then
				#2.3.3.9.2���������(�ӱ��в�ѯ��¼��ʧ��,һ���Ǳ��쳣,��˿���ȷ������Ҳʧ����)
				ALARM_CONTENT=`echo �ļ�${sfilename}����ʧ��,��ѯ�����¼��ʧ�ܶ����£�`
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$end_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
				DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
				cp $sfilename    $ERROR_PATH
				cp $chk_filename $ERROR_PATH
				build_result_file "$sfilename" "ʧ��"
			else
	                	error_cnt=`expr $end_cnt - $loaded_cnt`
	                	if [ $end_cnt -ne 0 -a $loaded_cnt -eq 0 ]; then
		                	#2.3.3.9.3������ȫ������
		                	ALARM_CONTENT=`echo �ӿ��ļ�:$sfilename�����¼ȫ��ʧ��,ԭ��δ����`
		                	DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$end_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""   
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	cp $sfilename    $ERROR_PATH
		                        cp $chk_filename $ERROR_PATH
		                        build_result_file "$sfilename" "ʧ��"
	                	else
		                	#2.3.3.9.4�����벿�ֳ���(��ʱ���澯,�治Ӱ�����ִ��)
		                	
		                	# ����2012-02-16Ӧ���������ۣ���У���ļ�ֵ����������ļ�ֵ֮����[0,10]���䣬��������������ִ��
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

		                	ALARM_CONTENT=`echo �ӿ��ļ�:$sfilename����ʱ����ʧ��,ʧ�ܼ�¼��Ϊ${error_cnt}!`
		                	DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
		                	DB2_SQL_EXEC "$DB2_SQLCOMM" > /dev/null
		                	build_result_file "$sfilename" "ʧ��"
	                	fi
			fi
		fi      
			#2.3.3.10�����ݽӿ��ļ�,ɾ���ӿ���ʱ�����ļ�
			mv $sfilename $BAK_PATH
			mv $chk_filename $BAK_PATH
			rm $load_avl_filename
	fi
	echo "`date`: �ӿ�${sfilename}���ؽ���........."
}

#��shell
function main
{
	cd $WORK_PATH

	#�������в���
	if [ $# -eq 1 ] ; then
		if [  "$1" = "stop"   ] ; then
			if [ -f $RUNNING_FILE ] ; then
				touch $STOP_FILE
				echo "������ֹͣ��־�ļ�,�ȴ��˳�!"
				exit
			fi
			echo "����û�����У�"
			exit
		fi
	fi
	
	if [ -f $RUNNING_FILE ] ; then
		echo "���������У�"
		exit
	fi
	
	
	touch $RUNNING_FILE
	while [ true ]
	do
		
		cd $WORK_PATH
		
		#��ֹͣ��־�ļ����ڣ�������˳�
		if [ -f $STOP_FILE ]; then
			rm $RUNNING_FILE
			rm $STOP_FILE
			break
		fi
		
		ls -l *.AVL.Z |awk '{print $9}' > ./$$
		echo `ls -l *.AVL.Z |awk '{print $9}'`
		
		while read sfilename
		do
			#2.0����ֹͣ��־�ļ����ڣ����˳�ѭ��
			if [ -f $STOP_FILE ]; then
				break
			fi
		
			pid=$$
			#���Ӵ�����̷����ִ̨��
			
			load_data $sfilename >> ${WORK_PATH}/log/${sfilename}.load.log & 
			
			#load_data $sfilename & 
			
			#�жϸ������̷����˼����Ӵ������,�������$_MAX_PROCESS,��ȴ��������̽���
			while [ `ps -ef|awk '$3 == '$pid'' | wc -l` -gt $_MAX_PROCESS ] 
			do 
				echo "�Ѵﵽ��������,�ȴ����д���..................."
			    	sleep 3	
			done 
			
			echo "�ӽ��̸�����" `ps -ef|awk '$3 == '$pid'' | wc -l`
		
		done < ./$$
		rm ./$$
		
		while [ `ps -ef|awk '$3 == '$pid'' | wc -l` -gt 1 ]         
	        do 
	        	echo echo "�ӽ��̸�����" `ps -ef|awk '$3 == '$pid'' | wc -l`
	        	#��ֹ������һ��ɨ��Ŀ¼ʱ����һ�ֵļ��ز���δ��ɣ���ɽӿ��ظ�����
	        	echo "�ӽ���δ��ȫ����,�������˳��ȴ�..................."
	            	sleep 10
	        done 		
		
		
		echo "��������60��!"
		sleep 60
	done	
	echo "������������!"
}

main
