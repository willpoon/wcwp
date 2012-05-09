#!/bin/sh
#*************************************************************************************************************
#��������load_mmarket.sh
#��  �ܣ���10.233.26.55����/data1/interface/mm/data/{sday}Ŀ¼ȡ�����ļ�,�������
#��  ������
#
#��д��: asiainfo
#��дʱ�� 2011-04-15
#
#��  ��: �ӿڼ������
#ע�������shell��crontab����,�����е�����ftp_mmarket.sh
#
#ִ�з�ʽ������ÿ����crontab����ִ�У�Ҳ���ֹ�ִ�С��ֹ�ִ�п��Ը�һ��������Ҳ���Ը�����������ִ�з�ʽ���£�
#          ִ�з�ʽ:load_mmarket.sh yyyymmdd ,Ϊȡ�ض�ĳһ��Ľӿڣ���./load_mmarket.sh 20100402 Ϊȡ2010��4��1�ŵ�ȫ���ӿ��ļ�
#          ִ�з�ʽ:load_mmarket.sh yyyymmdd �ӿڱ�� ,Ϊȡ�ض�ĳһ���ĳ���ӿڣ���./load_mgame.sh 20100402 A96001 Ϊȡ 2010��4��1�Žӿ�A96001���ļ�
#
#����˵�����ӿ�ԭ�ļ�������'^_'��0x1F���ָ��,���²����ǲ���0x1F�Ļ�ȡ��ʽ
#*************************************************************************************************************

#��ȡ���µ�����
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
echo `date` >> /bassdb1/etl/L/mm/db2_connect.log
echo $DB2_SQLCOMM >> /bassdb1/etl/L/mm/db2_connect.log

conn_cnt=0
while [ $conn_cnt -lt 3 ]
do
	db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
	if [ $? != 0 ] ; then
		echo "���ݿ�����ʧ�ܣ�" >> /bassdb1/etl/L/mm/db2_connect.log
		conn_cnt=`expr ${conn_cnt} + 1`
		echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/mm/db2_connect.log
	else
		echo "���ݿ����ӳɹ���" >> /bassdb1/etl/L/mm/db2_connect.log
		break
	fi
done
eval $DB2_SQLCOMM

db2 commit
db2 terminate
}

#�ַ����滻����
ReplaceAllSubStr()
{
     echo $1 > sed$$.temp
     sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
     rm sed$$.temp
     echo $sReturnString
     return 1
}


#�ӿںŶ�Ӧ�ļ���Ϣ

#������Ϣ

PROGRAM_NAME="load_mmarket.sh"
WORK_PATH=/bassdb1/etl/L/mm
DATA_PATH=/bassdb1/etl/L/mm/data
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload


#���ݿ�������Ϣ
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

#����
DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`

#�л��ع���Ŀ¼
cd $WORK_PATH

echo "����m-market�ӿ����ݿ�ʼ ......"
date





#ȡ�ü��ؽ��̸���,��ɾ��ֹͣ�ļ���־
if_run=`ps -ef|grep load_mmarket.sh|grep -v grep|wc -l`
if [ -f ${WORK_PATH}/stop_load_mm ] ; then
   rm ${WORK_PATH}/stop_load_mm
fi

#�����ж�
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "load_mmarket.sh�Ѿ�������,���ܸ���ִ��!"
            exit
        fi
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "stop" > ${WORK_PATH}/stop_load_mm
            echo "�������ļ�ֹͣ��־load_mmarket.sh����ִ���굱ǰ������˳�,��ȴ�!"
            exit
        else
            echo "û��load_mmarket.sh������!"
            exit
        fi
   else
        echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_mmarket.sh [start|stop]"
   fi
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 1 ] ; then
            echo "load_mmarket.sh�Ѿ�������,���ܸ���ִ��!"
            exit
   fi
else
   echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_mmarket.sh [start|stop]"
   exit
fi



#
#
#while read x y z
#do
#	if [ $z = "D" ];then 
#
#	  yyyymm=`echo "$yyyymmdd"|cut -c1-6`
#	  smonth=$yyyymm
#		dt=$sday
#		interface_id=${x}
#		file_name=${y}
#		sfilename=${file_name}_30_${dt}
#	
#	else 
#	if [ $z = "M" ];then 
#	  
#	  smonth=`lastmonth $today`
#		dt=$smonth
#		interface_id=${x}
#		file_name=${y}
#		sfilename=${file_name}_30_${dt}00
#	
#	fi
#fi
#
#echo $dt
#echo $interface_id
#echo $file_name
#echo $sfilename
#
#
#cd ${DATA_PATH}/${smonth}
#echo ${DATA_PATH}/${smonth}
#
#cat ${sfilename}*.txt > ${TOLOAD_PATH}/load_${sfilename}.dat
#
#cd ${TOLOAD_PATH} 
#
#echo $sfilename
#
#
#done<common.cfg


cd ${DATA_PATH}

while [ true ]
do

      #��ֹͣ��־�ļ����ڣ�������˳�
      if [ -f ${WORK_PATH}/stop_load_mm ]; then
            break
      fi


  while read sfilename
  do

        if [ "${sfilename}" = "" ] ; then
        	    break
        fi

        #�����ļ���,����,�ӿڴ���,У���ļ���,�Լ��·�
        time_id=`echo "${sfilename}"|awk -F'_' '{print $4}'`
        mark_id=`echo "${sfilename}"|awk -F'_' '{print $1}'`
        data_file=`echo "${sfilename}"|awk -F'_' '{print $2}'`
        
        city_id=`echo "${sfilename}"|awk -F'_' '{print $3}'`
        month_id=`echo "$time_id"|cut -c1-6`
        time_flag=`echo "$time_id"|cut -c7-8`
        load_file=`echo "${mark_id}_${data_file}_${city_id}_${time_id}"`
        txt_file=`echo "${mark_id}_${data_file}_${city_id}_${time_id}"_*.txt`
#        chk_file=`echo "${mark_id}_${data_file}_${city_id}_${time_id}".chk`
        
        if [ "${time_flag}" = "00" ]; then
        	scycleid=`echo ${month_id}`
          table_name_templet=`echo "DWD_MM_${data_file}"_YYYYMM|tr [a-z] [A-Z]`
        else
          scycleid=`echo $time_id`
          table_name_templet=`echo "DWD_MM_${data_file}"_YYYYMMDD|tr [a-z] [A-Z]`
        fi        
        
        echo time_id=$time_id
        echo mark_id=$mark_id
        echo data_file=$data_file
        echo city_id=$city_id
        echo month_id=$month_id
        echo time_flag=$time_flag
        echo scycleid=$scycleid
        echo load_file=$load_file
        echo txt_file=$txt_file
        echo chk_file=$chk_file
        echo table_name_templet=$table_name_templet


  	#�ж϶�Ӧ��У���ļ��Ƿ����,������������������ѭ����������һ�ӿڴ���
#     if [ ! -f ${DATA_PATH}/${chk_file} ]  ; then
#       echo "�ӿڼ��ظ澯��Ϣ�������ļ�${txt_file}��Ӧ��У���ļ�������!"
#  	   sleep 30
#       continue
#  
#  	 else
  	 
  	  #�Ƚ��������ļ�ת�ƣ�ת�Ƶ�toloadĿ¼��
  	  cat ${DATA_PATH}/${txt_file} > ${TOLOAD_PATH}/${load_file}.dat
  	  cd ${TOLOAD_PATH}
  	  temp_loadfile=`echo "${load_file}".dat`
  	  
  	  echo ${temp_loadfile}
  	  
#  	  cd ${DATA_PATH}
#      mv ${DATA_PATH}/${chk_file} ${TOLOAD_PATH}/${chk_file}
      
      #rm ${DATA_PATH}/${txt_file}
  	 
      #ͳ�������ļ���¼��
      
  		avl_cnt=`wc -l ${TOLOAD_PATH}/${temp_loadfile} |awk '{print $1}'`
  		avl_byte=`ls -trl ${TOLOAD_PATH}/${temp_loadfile} |awk '{print $5}'`
  		file_time=`ls -trl ${TOLOAD_PATH}/${temp_loadfile} |awk '{print $8}'`
  
  		echo avl_cnt=$avl_cnt
  
#     fi


      #��ȡ������ӿ�ID���ӿ����ơ���ⷽʽ
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',task_id from bass2.ETL_LOAD_TABLE_MAP where table_name_templet='$table_name_templet'\""
  		task_id=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		control_code="TR1_L_${task_id}"
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where table_name_templet='$table_name_templet'\""
  		task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where table_name_templet='$table_name_templet'\""
  		load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo task_id=$task_id
  		echo control_code=$control_code
      echo task_name=$task_name
      echo load_method=$load_method


      #�ж��Ƿ��������
      if  [ "${table_name_templet}" = "" ] ; then
             mv  ${TOLOAD_PATH}/${temp_loadfile} ${BACKUP_PATH}
#             mv  ${TOLOAD_PATH}/${chk_file} ${BACKUP_PATH}
             echo "�ӿ�${task_id}������Ϣ������!"
             continue
      else
             table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
	           table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
      fi


      #�жϽӿڼ���Ŀ����Ƿ����
      DB2_SQLCOMM="db2 \"select 'xxxxxx',tabname from syscat.tables where tabname=upper('$table_name')\""
  		table_name_target=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo table_name_target=$table_name_target
  		if [ "${table_name_target}" = "" ]; then
  			ALARM_CONTENT=`echo "�ӿڼ��ظ澯��Ϣ���ӿ�$task_id����Ŀ���$table_name�����ڣ�"`        
  			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  			DB2_SQL_EXEC
  			echo ALARM_CONTENT=$ALARM_CONTENT
  
  			#�ӿ��ļ�����errorĿ¼��������һ�ӿڴ���
  			mv ${TOLOAD_PATH}/${temp_loadfile} $ERROR_PATH
#  			mv ${TOLOAD_PATH}/${chk_file} $ERROR_PATH
  			continue
  	  fi


      #����ӿ���������ʼ
      #������ݱ�
	    DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
	    DB2_SQL_EXEC > /dev/null
	    echo $DB2_SQLCOMM

  	  #���ü�������
  	  #ɾ����������
  	  echo "ɾ���������� ..."
  		DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
  		DB2_SQL_EXEC > /dev/null
  		echo $DB2_SQLCOMM
  
  
  	  #�����µ�����
  	  echo "�����µ����� ..."
  		DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$txt_file','$chk_file','$task_name',-1,'ETL',current timestamp,'A')\""
  		DB2_SQL_EXEC > /dev/null
  		echo $DB2_SQLCOMM


      #����������־״̬Ϊ��ʼ
      DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
      DB2_SQL_EXEC > /dev/null
      echo $DB2_SQLCOMM



	    echo "����${temp_loadfile}���ݿ�ʼ!"
      DB2_SQLCOMM="db2 \"load client from ${TOLOAD_PATH}/${temp_loadfile} of del modified by coldel0x1F timestampformat=\\\"YYYYMMDDHHMMSS\\\"  insert into ${table_name} nonrecoverable\""
      DB2_SQL_EXEC > /dev/null
      echo $DB2_SQLCOMM
      echo "����${temp_loadfile}�������!"

      #ȡ���ı���¼���ͼ��ؼ�¼��
      DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
      load_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`


      #�жϼ����Ƿ�ɹ�
      if [ "${load_cnt}" != "${avl_cnt}" ] ; then
            ALARM_CONTENT="${temp_loadfile}����ʧ��,���ؼ�¼��:${load_cnt},�ı���¼��:${avl_cnt},������־ʧ�ܲ��澯!"
            echo $ALARM_CONTENT
            DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
            DB2_SQL_EXEC > /dev/null
            echo $DB2_SQLCOMM
						DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
						DB2_SQL_EXEC  > /dev/null
						echo $DB2_SQLCOMM
						DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_mmarket.sh',1,'${temp_loadfile}����ʧ��',current timestamp,-1)\""
						DB2_SQL_EXEC > /dev/null
  			    mv ${TOLOAD_PATH}/${temp_loadfile} $ERROR_PATH
  			    mv ${TOLOAD_PATH}/${chk_file} $ERROR_PATH

      else
            echo "${temp_loadfile}���سɹ�,��¼��Ϊ:${avl_cnt},��������־Ϊ���!"
      	    error_cnt=`expr $avl_cnt - $load_cnt`
            DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$avl_cnt,load_sheet_cnt=$load_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
            DB2_SQL_EXEC > /dev/null
            echo $DB2_SQLCOMM
            DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
            DB2_SQL_EXEC > /dev/null
            echo $DB2_SQLCOMM
            DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
            DB2_SQL_EXEC > /dev/null
            echo $DB2_SQLCOMM
            DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""
						DB2_SQL_EXEC  > /dev/null
						echo $DB2_SQLCOMM
						mv  ${TOLOAD_PATH}/${temp_loadfile} ${BACKUP_PATH}
#            mv  ${TOLOAD_PATH}/${chk_file} ${BACKUP_PATH}
            echo ${txt_file}
            cd ${DATA_PATH}
            rm ${txt_file}

      fi

        done<<!
	        `ls -l *.txt |awk '{print $9}' `
!

        sleep 600

done
