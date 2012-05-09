#!/bin/ksh
#*******************************************************
#��������load_vgop_dmkdb_test.sh
#��  �ܣ���ȡVGOP�ӿ���Ϣ�����������
#��  ���� 
#author: zhaolp2      
#��  ��: 2010-09-02	      
#��  ��: 
#��  ��:1.2012-4-11 By Liwei LOAD_BUG1-Сʱ�ӿڼ���δ��¼��־ 
#         2012-4-24 ע�͵�LOAD_BUG1(�ع�running��task��ṹ�ֶ�cycle_id)
#         !!!!!�˳����VGOP�ӿ�����ʱ����ʹ�ã�
#*******************************************************


#������Ϣ
DB2_OSS_DB="DMKDB"
DB2_OSS_USER="dmkmark"
DB2_OSS_PASSWD="0906060C131962"
DB_SCHEMA="VGOP"

EXEC_PATH=/bassdb1/etl/L/vgop/
WORK_PATH=/bassdb1/etl/L/vgop/zhouc/
DATA_PATH=${WORK_PATH}/data
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`






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

#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

#���ַ����滻����
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
   filename=$1
fi

	
	length=`echo $filename | awk -F '{print length($0)}'`
	
	
	if [ $length -eq 45 ]; then
		echo "Сʱ�ӿ�"
		interface_id=`echo $filename | cut -c30-34`
		chkfilename=`echo $filename | cut -c1-37`.verf
		time_id=`echo $filename | cut -c9-18`
		task_id=VGOP_H_${interface_id}
	fi
	
	if [ $length -eq 43 ] ; then
		echo "�սӿ�"
		interface_id=`echo $filename | cut -c28-32`
		chkfilename=`echo $filename | cut -c1-35`.verf
		time_id=`echo $filename | cut -c9-16`
		task_id=VGOP_D_${interface_id}
	fi
	
	if  [ $length -eq 41 ] ; then
		echo "�½ӿ�"
		interface_id=`echo $filename | cut -c26-30`
		chkfilename=`echo $filename | cut -c1-33`.verf
		time_id=`echo $filename | cut -c9-14`
		task_id=VGOP_M_${interface_id}
	fi
	
		echo $task_id
		
		echo ${interface_id}
	
	configFlag=`grep ${task_id} ${WORK_PATH}/load_common_dmkdb.cfg |grep -v "#"| wc -l`
	
	echo "configFlag $configFlag"
	
	if [ ${configFlag} -eq 0 ] ; then 
		echo "$task_id δ������(��ʱ���ü����ļ�)�������˳�!"
		exit
	fi
	
	control_code=TR1_DMK_${task_id}
	#ͨ�������ļ���ȡģ������ƺͽӿ���������
	echo $control_code
	table_name_templet=`grep -i ${task_id} ${WORK_PATH}/load_common_dmkdb.cfg|grep -v "#"| awk -F',' '{print $2}'`
	task_name=`grep -i ${task_id} ${WORK_PATH}/load_common_dmkdb.cfg |grep -v "#"| awk -F',' '{print $3}'`
	table_name=${table_name_templet}_${time_id}
	echo "table_name====>" $table_name	
	
	
	#����ӿ��ļ����ڣ�ִ�м��ض���
	if [ -f ${DATA_PATH}/$filename -o -f ${DATA_PATH}/${time_id}/$filename ]; then
	
		mv ${DATA_PATH}/${time_id}/$filename ${WORK_PATH}/
		mv ${DATA_PATH}/${time_id}/$chkfilename ${WORK_PATH}/
		mv ${DATA_PATH}/$filename ${WORK_PATH}/
		mv ${DATA_PATH}/$chkfilename ${WORK_PATH}/
		
		file_rows_fact=`wc -l ${WORK_PATH}/${filename}|awk '{print $1}'`
		
		cd ${WORK_PATH}	
  
		DB2_SQLCOMM="db2 \"drop table ${DB_SCHEMA}.${table_name}\""
	        DB2_SQL_EXEC 
	        
		DB2_SQLCOMM="db2 \"create table ${DB_SCHEMA}.$table_name like ${DB_SCHEMA}.$table_name_templet in TBS_VGOP index in tbs_index partitioning key (ROW_NO) using hashing not logged initially\""
	        DB2_SQL_EXEC 
  
    ##LOAD_BUG1 liwei 2012-4-11
	  #if [ $length -eq 45 ]; then
	  #	time_id=`echo $filename | cut -c9-16`
	  #fi
  
		DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$time_id','$table_name','$filename','$chkfilename','$task_name',-1,'ETL',current timestamp,'A')\""
    DB2_SQL_EXEC > /dev/null
		#�����ļ�${filename},��������־Ϊ��ʼ
		DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""   
		DB2_SQL_EXEC > /dev/null
		
		#��ŷԪ�����������ַ������²��������滻������db2��֧��ŷԪ������Ϊ�ֶηָ����������޸��ַ�������
		LANG=C;         export NLS_LANG
		LC_ALL=; export LC_ALL
		
		#��ŷԪ�����滻����Ԫ����
		sed 's/�/$/g' $filename > ${filename}.load
		sed 's///g' ${filename}.load > $filename
		
		DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${filename} of del modified by coldel$ fastparse anyorder warningcount 1000 messages /dev/null replace into ${DB_SCHEMA}.${table_name} NONRECOVERABLE \""
		echo $DB2_SQLCOMM;
		DB2_SQL_EXEC
		
		DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${DB_SCHEMA}.${table_name} \""
		loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
		
		if [ "${loaded_cnt}" = "${file_rows_fact}" ] ; then
			echo "${filename}���سɹ�,��¼��Ϊ:${file_rows_fact},��������־Ϊ���"
			error_cnt=`expr $file_rows_fact - $loaded_cnt`
			
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$file_rows_fact,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$time_id'\""
      DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$time_id'\""
      DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$time_id'\""
      DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC  > /dev/null
			
			#2012-5-9 By Liwei �ļ�����BACKUP_PATH
			if [ ! -d ${BACKUP_PATH}/$time_id ] ; then
			   mkdir $time_id
			fi			
			rm ${filename}.load 
			mv ${DATA_PATH}/$filename ${BACKUP_PATH}/${time_id}/$filename
			mv ${DATA_PATH}/$chkfilename ${BACKUP_PATH}/${time_id}/$chkfilename
		else
			ALARM_CONTENT="${filename}����ʧ��,���ؼ�¼��:${loaded_cnt},�ı���¼��:${file_rows_fact},��������־Ϊʧ��,���澯"	
			echo ${ALARM_CONTENT}
			
			rm ${filename}.load ${ERROR_PATH}
			mv ${filename} ${ERROR_PATH}
			mv ${chkfilename} ${ERROR_PATH}			
			
			DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$time_id'\""
			DB2_SQL_EXEC > /dev/null
			DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
			DB2_SQL_EXEC  > /dev/null
			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_vgop_dmkdb.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			DB2_SQL_EXEC  > /dev/null
		fi		
		
	fi

	exit
