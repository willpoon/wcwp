#!/bin/sh

#������Ϣ
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
EXEC_PATH=/bassdb1/etl/L/mr
WORK_PATH=/bassdb1/etl/L/mr
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}


#�ӿڶ�����Ϣ
INTERFACE_LEN_98001="1 15,16 17,18 19,20 34,35 36,37 38,39 41,42 44,45 58,59 72"
INTERFACE_LEN_98002="1 15,16 45,46 55,56 57,58 72,73 86,87 100"
INTERFACE_LEN_98003="1 15,16 45,46 55,56 57,58 72,73 74,75 76,77 78,79 98,99 118,119 132,133 152,153 172,173 186,187 200"
INTERFACE_LEN_98004="1 18,19 68,69 71,72 86,87 100,101 114,115 116"
INTERFACE_LEN_98005="1 18,19 36"
INTERFACE_LEN_98006="1 30,31 80,81 85,86 87,88 137,138 187,188 237,238 287,288 301,302 315,316 329,330 343,344 344,345 354,355 360"
INTERFACE_LEN_98007="1 18,19 68,69 73,74 79,80 89,90 91"
INTERFACE_LEN_98008="1 6,7 26,27 27,28 28,29 38,39 40,41 48,49 56,57 76"
INTERFACE_LEN_98009="1 15,16 29,30 31,32 37,38 39,40 41"
INTERFACE_LEN_98010="1 15,16 29,30 34,35 64,65 70,71 80,81 95,96 97"
INTERFACE_LEN_98011="1 15,16 29,30 31,32 61,62 67,68 77,78 79"
#INTERFACE_LEN_980912="1 20,21 22,23 36,37 38,39 62,63 86,87 116,117 122,123 130,131 132,133 156,157 170,171 184,185 188"
INTERFACE_LEN_98012="1 20,21 22,23 36,37 46,47 70,71 94,95 124,125 130,131 138,139 140,141 164,165 178,179 192,193 202"
INTERFACE_LEN_98013="1 18,19 48,49 58,59 72,73 86"
INTERFACE_LEN_98014="1 18,19 48,49 58,59 60,61 62,63 64,65 84,85 104,105 118"
INTERFACE_LEN_98015="1 18,19 48"
INTERFACE_LEN_98016="1 14,15 29,30 34,35 64,65 74,75 76,77 82"


#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM
    db2 commit
    db2 connect reset
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

#ȡ�ü��ؽ��̸���,��ɾ��ֹͣ�ļ���־
if_run=`ps -ef|grep load_mr_nfjd.sh|grep -v grep|wc -l`
if [ -f ${EXEC_PATH}/stop_load_mr ] ; then
   rm ${EXEC_PATH}/stop_load_mr
fi

#�����ж�
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "load_mr_nfjd.sh�Ѿ�������,���ܸ���ִ��!"
            exit
        fi
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 1 ] ; then
            echo "stop" > ${EXEC_PATH}/stop_load_mr
            echo "�������ļ�ֹͣ��־load_mr_nfjd.sh����ִ���굱ǰ������˳�,��ȴ�!"
            exit
        else
            echo "û��load_mr_nfjd.sh������!"
            exit
        fi
   else
        echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_mr_nfjd.sh [start|stop]"
   fi
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 1 ] ; then
            echo "load_mr_nfjd.sh�Ѿ�������,���ܸ���ִ��!"
            exit
   fi
else
   echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_mr_nfjd.sh [start|stop]"
   exit
fi

#��SHELL
while [ true ]
do
        #��ֹͣ��־�ļ����ڣ�������˳�
        if [ -f ${EXEC_PATH}/stop_load_mr ]; then
            rm ${EXEC_PATH}/stop_load_mr
            break
        fi

        cd $WORK_PATH
        today=`date '+%Y%m%d'`
        sday=`yesterday $today`
        #sday=20120419
        smonth=`lastmonth $today`
        sHour=`date '+%H'`

       if [ ${sHour} -ge  5 ]  ; then
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98001${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98009${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98010${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98011${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98012${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "A98016${sday}*"
       fi

       if [ ${sHour} -ge  5 ]  ; then
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "I98007${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "I98008${sday}*"
           ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    "I98015${sday}*"
           #./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport    I98006${sday}*
       fi


       #if [ ${sHour} -ge  11 ]  ; then
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98001${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98002${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98003${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98004${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98005${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98013${smonth}*"
       #    ./ftp_mr.sh  /home/ztecrbt/testUpload/uploadExport  "M98014${smonth}*"
       #fi

       #if [ ${sHour} -ge  10 ]  ; then
       # echo "98006 ftp begin...	`date`"
       #    ./ftp_mr.sh  /work/info/day    I98006${sday}*.zip
       # echo "98006 ftp end!	`date`"
       #fi


        while read sfilename
        do

              if [ "${sfilename}" = "" ] ; then
              	    break
              fi

              #��ֹͣ��־�ļ����ڣ�������˳�
              if [ -f ${EXEC_PATH}/stop_load_mr ]; then
                    break
              fi

              #�����ļ���,����,�ӿڴ���,У���ļ���,�Լ��·�
              task_id=`echo "$sfilename"|cut -c1-6`
              interface_code=`echo "$sfilename"|cut -c2-6`
              month_id=`echo ${sfilename}|cut -c7-12`
              time_id=`echo ${sfilename}|cut -c7-14`
              pure_filename=`echo "$sfilename"|cut -c1-20`
	      chk_filename=`echo "$pure_filename".CHK`
	      control_code=`echo ${sfilename}|cut -c1-6`
              control_code="TR1_L_${control_code}"
              flag=`echo "$sfilename"|cut -c1-1`
              if [ "$flag" = "M" ]; then
              	  scycleid=`echo $month_id`
              else
                  scycleid=`echo $time_id`
              fi


############# liuzhilong 20101028 ���Ӵ��� �жϽӿ�ΪA98012��M98002��M98003��M98013��M98014 �Ƿ��Ѻϲ��ļ�
						 if [ "$task_id" = "A98012" ]||[ "$task_id" = "M98002" ]||[ "$task_id" = "M98003" ]||[ "$task_id" = "M98013" ]||[ "$task_id" = "M98014" ]; then
						 	 filenum=`ls -l $task_id$time_id*AVL |wc -l`
						 	 cheknum=`awk -F} '{print $3}' ${task_id}${time_id}000000.SUM  |sed s/"{"//`
						 	 #�ж��ļ������Ƿ�����
						 	 if [ $filenum = $cheknum ] ; then
						 	 		#�ϲ��ļ�
						 	 		rm -f tmp_${task_id}${time_id}000000.AVL
						 	 		cat $task_id$time_id*AVL >> tmp_${task_id}${time_id}000000.AVL

									#����У���ļ�
						 	 		n="${task_id}${time_id}000000.AVL"
									s=`ls -l tmp_${task_id}${time_id}000000.AVL |awk -F" "  '{print $5}'`
									r=`wc -l tmp_${task_id}${time_id}000000.AVL |awk -F" "  '{print $1}'`
									d="${time_id}"
									t=`date '+%Y%m%d%H%M%S'`
									printf "%-40s%-20s%-20s%8s%14s\n"  $n $s $r $d $t > tmp_${task_id}${time_id}000000.CHK
									# ��ԭ�ָ���ļ����� ./backup/splitfile
									mv $task_id$time_id* ${BACKUP_PATH}/splitfile
									mv tmp_${task_id}${time_id}000000.AVL ${task_id}${time_id}000000.AVL
									mv tmp_${task_id}${time_id}000000.CHK ${task_id}${time_id}000000.CHK
							 else
							 		echo $task_id�ļ�����δ����!
							  	continue
							 fi
						 fi
####################################################

              #�ж϶�Ӧ��У���ļ��Ƿ����
              if [ ! -f ${WORK_PATH}/${chk_filename} ]  ; then
              	  #mv  ${WORK_PATH}/${sfilename} ${ERROR_PATH}
                  echo "�����ļ�${sfilename}��Ӧ��У���ļ�������!"
              	  continue
              fi

              #�Ƚ������ļ���¼������У���ļ��м�¼���Ƿ�һ��
              chk_rec=`grep ${sfilename} ${WORK_PATH}/${chk_filename}`
              file_size=`echo ${chk_rec}|cut -d' ' -f2`
              chk_rec_num=`echo ${chk_rec}|cut -d' ' -f3`
              fact_rec_num=`wc -l ${WORK_PATH}/${sfilename}|awk '{print $1}'`

             #added by lizhanyong 2008-10-16	 ������������
             avl_byte=`ls -trl $sfilename |awk '{print $5}`
	     file_time=`ls -trl $sfilename |awk '{print $8}`
	     /bassdb1/etl/L/interface_control.sh $task_id $file_time $fact_rec_num $avl_byte

	     #added end

              if [ ${fact_rec_num} -ne ${chk_rec_num} ] ; then
              	  mv  ${WORK_PATH}/${sfilename} ${ERROR_PATH}
                  mv  ${WORK_PATH}/${chk_filename} ${ERROR_PATH}
                  echo "${sfilename}ʵ�ʼ�¼����У���ļ��м�¼�����${fact_rec_num}!=${chk_rec_num}"
              	  continue
              fi

              #��ȡ����ģ��������ӿ����ơ���ⷽʽ
	      DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	      table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	      DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	      task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
	      DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	      load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`

	      #�ж��Ƿ��������
              if  [ "${table_name_templet}" = "" ] ; then
                     mv  ${WORK_PATH}/${sfilename} ${BACKUP_PATH}
                     mv  ${WORK_PATH}/${chk_filename} ${BACKUP_PATH}
                     echo "${sfilename}������Ϣ������!"
                     continue
              else
                     table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
        	     table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
              fi

              #ȡ�ö�����Ϣ
              len_val=`grep "INTERFACE_LEN_${interface_code}" ${WORK_PATH}/load_mr_nfjd.sh |cut -d= -f2`

              #�ж��Ƿ��������
              if  [ "${len_val}" = "" ] ; then
                     mv  ${WORK_PATH}/${sfilename} ${BACKUP_PATH}
                     mv  ${WORK_PATH}/${verf_name} ${BACKUP_PATH}
                     echo "${sfilename}����������Ϣ������!"
                     continue
              fi

              #������ݱ�
	      DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
	      DB2_SQL_EXEC > /dev/null
	      echo $DB2_SQLCOMM

              #����������־״̬Ϊ��ʼ
	      DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
	      DB2_SQL_EXEC > /dev/null

	      #2008-11-03 ����������������޸Ĵ˲��ֳ����ڱ�BASS2.ETL_TASK_RUNNING��BASS2.ETL_TASK_log�����������ֶ�bass_table_name,avl_filename,chk_filename
	      #��¼������־
	      echo table_name=$table_name
	      echo sfilename=$sfilename
	      echo chk_filename=$chk_filename
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$sfilename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
	      DB2_SQL_EXEC > /dev/null
	      #2008-11-03 �޸����

	      echo "����${sfilename}���ݿ�ʼ!"
              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${sfilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""
              echo $DB2_SQLCOMM
              DB2_SQL_EXEC
              echo "����${sfilename}�������!"

              #ȡ���ı���¼���ͼ��ؼ�¼��
              DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
              load_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              fact_cnt=`wc -l ${WORK_PATH}/${sfilename} |awk '{print $1}'`

              #�жϼ����Ƿ�ɹ�
              if [ "${load_cnt}" != "${fact_cnt}" ] ; then
                    ALARM_CONTENT="${sfilename}����ʧ��,���ؼ�¼��:${load_cnt},�ı���¼��:${fact_cnt},������־ʧ�ܲ��澯!"
                    echo $ALARM_CONTENT
                    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
		    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
		    DB2_SQL_EXEC  > /dev/null
		    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_mr_nfjd.sh',1,'${sfilename}����ʧ��',current timestamp,-1)\""
		    DB2_SQL_EXEC > /dev/null
		    mv  ${WORK_PATH}/${sfilename} ${ERROR_PATH}
                    mv  ${WORK_PATH}/${chk_filename} ${ERROR_PATH}
                    mv -f ${WORK_PATH}/${task_id}*SUM ${ERROR_PATH}
              else
                    echo "${sfilename}���سɹ�,��¼��Ϊ:${fact_cnt},��������־Ϊ���!"
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
		    mv  ${WORK_PATH}/${sfilename} ${BACKUP_PATH}
                    mv  ${WORK_PATH}/${chk_filename} ${BACKUP_PATH}
                    mv -f ${WORK_PATH}/${task_id}*SUM ${BACKUP_PATH}
              fi

        done<<!
	        `ls -l *.AVL |awk '{print $9}' `
!

        sleep 180

done
