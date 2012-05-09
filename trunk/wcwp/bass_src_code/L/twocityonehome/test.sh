#!/bin/sh

#������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

EXEC_PATH=/bassdb2/etl/L/twocityonehome
WORK_PATH=/bassdb2/etl/L/twocityonehome
BACKUP_PATH=${WORK_PATH}/data/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

start_interface_code=99901
end_interface_code=99902

#�ӿڶ�����Ϣ
INTERFACE_LEN_99901="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"
INTERFACE_LEN_99902="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"



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
if_run=`ps -ef|grep load_dsmp.sh |wc -l`
if [ -f ${EXEC_PATH}/load_dsmp_stop ] ; then
   rm ${EXEC_PATH}/load_dsmp_stop	
fi

#�����ж�
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "load_dsmp�Ѿ�������,���ܸ���ִ��!"
            exit	
        fi	
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "stop" > ${EXEC_PATH}/load_dsmp_stop
            echo "�������ļ�ֹͣ��־,load_dsmp����ִ���굱ǰ������˳�,��ȴ�!"
            exit
        else	
            echo "û��load_dsmp������!"
            exit	
        fi 
   else
        echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_dsmp [start|stop]"
   fi	 
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 2 ] ; then
            echo "load_dsmp�Ѿ�������,���ܸ���ִ��!"
            exit	
   fi		
else
   echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_dsmp [start|stop]"
   exit
fi

#��SHELL
while [ true ]
do
        
        #��ֹͣ��־�ļ����ڣ�������˳�
        if [ -f ${EXEC_PATH}/load_dsmp_stop ]; then
            rm ${EXEC_PATH}/load_dsmp_stop
            break
        fi
        
        #ȡ�õ�����,���µ�����
        today=`date '+%Y%m%d'`
        sday=`yesterday $today`
        smonth=`lastmonth $today`
        echo `date`
      
        #ȡ�ӿ��ļ�
       ./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${sday}???.???.891
       #./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${smonth}???.???.891

        if [ -f ${WORK_PATH}/data/C${sday}???.???.891 ] ; then
              cd        $WORK_PATH/data
              cp        C${sday}???.???.891 ${WORK_PATH}/backup
              echo "�ļ�����!"

         ## ���½����޳��ļ�ͷ��¼��β��¼
              ls -l C${sday}???.???.891 |awk '{print $9}' > file.list
              echo `ls -l C${sday}???.???.891 |awk '{print $9}'`
          while read sfilename
	           do
	              echo "${sfilename}�ļ��޳�ͷ��¼��β��¼��ʼ!"
	              sfilename=`echo ${sfilename}`
	              rec_cnt=`wc -l $sfilename|awk '{print $1}'`
	              rec_cnt1=`expr $rec_cnt - 1`
	              rec_cnt2=`expr $rec_cnt1 - 1`

	              echo $rec_cnt
	              echo $rec_cnt1
	              echo $rec_cnt2
	              #awk '{if(NR < `aaa=expr $rec_cnt`) print NR}' $sfilename >$sfilename.AVL

	              #echo "awk '{if(NR < $rec_cnt) print NR}' $sfilename >$sfilename.AVL.2"
	              head -$rec_cnt1 $sfilename > $sfilename.tmp
	              tail -$rec_cnt2 $sfilename.tmp > $sfilename.AVK
	              #rm $filename

	        done < ./file.list

	        rm ./*.tmp
	        rm ./file.list
	        echo "�޳��ļ�ͷ��¼��β��¼����!"

        else
              echo "�ļ�������!"
        fi

         #�����ļ��ϲ����жϺϲ��Ƿ�ɹ�
              day_datafile=A99901${sday}000000.AVL
              day_chkfile=A99901${sday}000000.CHK
              echo $day_datafile
              echo $day_chkfile
             # month_datafile=M99902${smonth}000000.AVL

              cat *AVK > ${day_datafile}
              if [ $? -eq 0 ]
              then
               rm ./*AVK
    	         echo "�ϲ��ļ��ɹ�!"

    	       #�ļ��ϲ��ɹ�������У���ļ�
    	         datafile_byte=`ls -l $day_datafile|awk '{print $5}'`
    	         datafile_count=`wc -l $day_datafile|awk '{print $1}'`
    	         datafile_date=`date +%Y%m%d%H%M%S`
    	         echo ${day_datafile}\$${datafile_byte}\$${datafile_count}\$${sday}\$${datafile_date} > ./$day_chkfile

              else
                 rm ${datafilename}
    	           echo "�ϲ��ļ�ʧ��!"
    	           continue
              fi

        #ȡ���ļ�,�������ļ���У���ļ��ƶ������ݼ���Ŀ¼

        current_interface_code=${start_interface_code}
        while [ ${current_interface_code} -lt ${end_interface_code} ]
        do
             current_date=${sday}
                    mv A${current_interface_code}${current_date}*.CHK   ${WORK_PATH}
                    mv A${current_interface_code}${current_date}*.AVL   ${WORK_PATH}
                    echo "�ļ��ƶ�������Ŀ¼�ɹ���"


             current_date=${smonth}
                    mv M${current_interface_code}${current_date}*.CHK   ${WORK_PATH}
                    mv M${current_interface_code}${current_date}*.AVL   ${WORK_PATH}

             current_interface_code=`expr $current_interface_code + 1`
             echo  current_interface_code=$current_interface_code
        done

                            
        #������
         cd $WORK_PATH
         ls -l *.AVL |awk '{print $9}' > ./file.lst
         echo `ls -l *.AVL |awk '{print $9}'`
        while read sfilename
        do
              
              if [ "${sfilename}" = "" ] ; then 
              	    break
              fi	
             
              #��ֹͣ��־�ļ����ڣ�������˳�
              if [ -f ${EXEC_PATH}/load_dsmp_stop ]; then
                  rm ${EXEC_PATH}/load_dsmp_stop
                  break
              fi
             
              #�����ļ���,���ɽӿڵ�Ԫ��,�·�,���ȱ�־,���ȴ����
              task_id=`echo ${sfilename}|cut -c1-6`
              month_id=`echo ${sfilename}|cut -c7-12`
              time_id=`echo ${sfilename}|cut -c7-14` 
              flag=`echo ${sfilename}|cut -c1-1`
              interface_code=`echo ${sfilename}|cut -c2-6`
              control_code="TR1_L_${interface_code}"
              if [ "$flag" = "M" ]; then
              	  scycleid=`echo $month_id`
              else
                  scycleid=`echo $time_id`
              fi	
              
              #��ȡ����ģ��������ӿ����ơ���ⷽʽ
              DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
              table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
              task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
              load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`	
              
              #ͨ��ģ��������ɶ�Ӧ�ı���
              if [ "$table_name_templet" = "" ]; then
                    rm ${datafilename}
                    mv ${sfilename}*.AVL ${TOLOAD_PATH}
              	    mv ${sfilename}.CHK  ${TOLOAD_PATH}            
        	    ALARM_CONTENT="�ӿ�${task_id}û�����������Ϣ!"
                    echo ${ALARM_CONTENT}
        	    continue
              else
        	    table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
        	    table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
              fi	
             
              #���ɺϲ����У���ļ���,�Լ�ԭʼУ���ļ���
              datafilename="${sfilename}000000.del"
              chkfilename="${sfilename}.CHK"
              
              #�����ļ��ϲ����жϺϲ��Ƿ�ɹ�
              cat ${sfilename} > ${datafilename}
              if [ $? -eq 0 ]
              then
    	         echo "${sfilename}�ϲ��ļ��ɹ�!"
              else
                 rm ${datafilename}
    	         echo "${sfilename}�ϲ��ļ�ʧ��!"
    	         continue
              fi
              
              #�ж�У���ļ��Ƿ����
              if [  ! -f ${chkfilename} ] ; then  
                  rm ${datafilename}
                  mv ${sfilename}*.AVL  ${ERROR_PATH} 
                  echo "${sfilename}��У���ļ�������!"
                  continue
              fi
              
              #ȡ���ļ��ϲ���ļ�¼�������ļ���С
              file_size_fact=`ls -l ${datafilename} |awk '{print $5}'`
              file_rows_fact=`wc -l ${WORK_PATH}/${datafilename}|awk '{print $1}`
             
              #ȡ�ô��ļ���У���ļ��ļ�¼���ʹ�С
              file_size_chk=`grep AVL ${chkfilename}|awk -F'|' '{print $4}'|awk '{(tot+=$1)}; END{ print "total:" tot} '|grep total|cut -d':' -f2`
              file_rows_chk=`grep AVL ${chkfilename}|awk -F'|' '{print $5}'|awk '{(tot+=$1)}; END{ print "total:" tot} '|grep total|cut -d':' -f2`
              
              #added by lizhanyong 2008-10-16	 ������������
              
	      file_time=`ls -trl $datafilename |awk '{print $8}`
	     /bassdb2/etl/L/interface_control.sh $task_id $file_time $file_rows_fact $file_size_fact
	     cd $WORK_PATH
	      #added end 
              
              #�����ļ�${sfilename},��������־Ϊ��ʼ
              DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""   
              DB2_SQL_EXEC > /dev/null
              
              #�ж��ļ���¼�ʹ�С�Ƿ���У���ļ��е�һ��
              if [  "${file_size_fact}" = "${file_size_chk}" -a "${file_rows_fact}" = "${file_rows_chk}" ] ; then
                   echo "�ӿ�${task_id}�ļ���С����¼����У���ļ��е�һ��!"
              else
                   rm ${datafilename}
                   mv ${sfilename}*.AVL ${ERROR_PATH}
              	   mv ${sfilename}.CHK  ${ERROR_PATH}
                   ALARM_CONTENT="�ӿ�${task_id}�ļ���С���¼����У���ļ��еĲ�һ��!"
                   DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
                   DB2_SQL_EXEC  > /dev/null
                   #DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_dsmp.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                   #DB2_SQL_EXEC > /dev/null
                   echo ${ALARM_CONTENT}
        	   continue
              fi
              
              #���ϲ����ļ����������ֶ�	
              if [ -f ${datafilename}.tmp ] ; then
                 rm ${datafilename}.tmp 
              fi	
              if [ "${flag}" = "A" -o "${flag}" = "I"  ] ; then 
                    sed 's/$/'${time_id}'/' ${datafilename}  > ${datafilename}.tmp   
                    rm ${datafilename}
                    mv ${datafilename}.tmp ${datafilename} 
              fi
              if [ "${flag}" = "M"  ] ; then 
                    sed 's/$/'${month_id}'/' ${datafilename}  > ${datafilename}.tmp   
                    rm ${datafilename}
                    mv ${datafilename}.tmp ${datafilename} 
              fi
              
              
              #ȡ�ýӿڵĶ�����Ϣ
              len_val=`grep "INTERFACE_LEN_${interface_code}" ${EXEC_PATH}/load_dsmp.sh |cut -d= -f2`
              
	      #��ֹ�ظ�����,ɾ����������
              if [ ${flag} = "P" -o ${flag} = "M" -o ${flag} = "I"  ] ; then 
                    DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
	            DB2_SQL_EXEC > /dev/null   
              else 
                    DB2_SQLCOMM="db2 \"delete from ${table_name} where bill_date='${time_id}'\""
                    DB2_SQL_EXEC > /dev/null 
              fi
	      
	      #2008-11-03 ����������������޸Ĵ˲��ֳ����ڱ�BASS2.ETL_TASK_RUNNING��BASS2.ETL_TASK_log�����������ֶ�bass_tablename,avl_filename,chk_filename
	      #��¼ETL��־
	      echo table_name=$table_name
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$datafilename','$chkfilename','$task_name',-1,'ETL',current timestamp,'A')\""
	      DB2_SQL_EXEC > /dev/null
	      #2008-11-03 �޸Ľ���

	      echo "����${datafilename}���ݿ�ʼ!" 
              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\"  timeformat=\\\"HHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""
              DB2_SQL_EXEC
              echo ${DB2_SQLCOMM}
              echo "����${datafilename}�������!"
              
              #�жϼ����Ƿ�ɹ�
              if [ "${flag}" = "P" -o "${flag}" = "M" -o "${flag}" = "I"  ] ; then     	
                   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
                   loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              else      
                   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} where bill_date='${time_id}' \""
                   loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              fi
              if [ "${loaded_cnt}" = "${file_rows_fact}" ] ; then
              	    echo "${datafilename}���سɹ�,��¼��Ϊ:${file_rows_fact},��������־Ϊ���"
              	    error_cnt=`expr $file_rows_fact - $loaded_cnt`
              	    mv ${sfilename}*.AVL ${BACKUP_PATH}
              	    mv ${sfilename}.CHK  ${BACKUP_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$file_rows_fact,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
              else
                    ALARM_CONTENT="${datafilename}����ʧ��,���ؼ�¼��:${loaded_cnt},�ı���¼��:${file_rows_fact},��������־Ϊʧ��,���澯"	
                    echo ${ALARM_CONTENT}
                    mv ${sfilename}*.AVL ${ERROR_PATH}
              	    mv ${sfilename}.CHK  ${ERROR_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_dsmp.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC  > /dev/null
             fi   
             rm ${datafilename}
              
       done< ./file.lst 
       
       rm ./file.lst 
       
       sleep 600 
done

exit
