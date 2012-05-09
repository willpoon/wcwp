#!/bin/sh

#������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

EXEC_PATH=/bassdb1/etl/L/twocityonehome
WORK_PATH=/bassdb1/etl/L/twocityonehome
BACKUP_PATH=${WORK_PATH}/data/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

start_interface_code=99901
end_interface_code=99903

#�ӿڶ�����Ϣ
INTERFACE_LEN_99901="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"
INTERFACE_LEN_99902="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"


#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM
    db2 commit
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
if_run=`ps -ef|grep load_2city1home.sh |wc -l`
if [ -f ${EXEC_PATH}/load_2city1home_stop ] ; then
   rm ${EXEC_PATH}/load_2city1home_stop
fi

#�����ж�
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "load_2city1home�Ѿ�������,���ܸ���ִ��!"
            exit
        fi
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "stop" > ${EXEC_PATH}/load_2city1home_stop
            echo "�������ļ�ֹͣ��־,load_2city1home����ִ���굱ǰ������˳�,��ȴ�!"
            exit
        else
            echo "û��load_2city1home������!"
            exit
        fi
   else
        echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_2city1home [start|stop]"
   fi
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 2 ] ; then
            echo "load_2city1home�Ѿ�������,���ܸ���ִ��!"
            exit
   fi
else
   echo "��������з�ʽ,��ȷ��ִ�з�ʽΪ:load_2city1home [start|stop]"
   exit
fi

#��SHELL

        #��ֹͣ��־�ļ����ڣ�������˳�
        if [ -f ${EXEC_PATH}/load_2city1home_stop ]; then
            rm ${EXEC_PATH}/load_2city1home_stop
            break
        fi

        #ȡ�õ�����,���µ�����
        today=`date '+%Y%m%d'`
        #sday1=`yesterday $today`
        #sday2=`yesterday $sday1`
        #sday=`yesterday $sday2`
        sday=20091111
        echo sday=$sday
        smonth=`lastmonth $today`
        echo smonth=$smonth
        echo `date`

        #ȡ�ӿ��ļ�
        
#        2009��11��������������һ�ң�boss�����Ϸ��ļ������ţ��ӿ����ֱ�Ӵ�bossȡ�ļ�
#        ������10.233.30.50
#        ftp���opencrm/opencrm
#        �ļ�Ŀ¼��/data1/opencrm/zhout/twocityonehome/cfile
#        �������ļ���ʽ��CYYYYMMDDNNN.ZZZ.TTT  
#        ��ȫ���ļ���ʽ��CYYYYMM999.ZZZ.TTT

       ./ftp_interface.sh  10.233.30.50 opencrm opencrm ${WORK_PATH}/data  /data1/opencrm/zhout/twocityonehome/cfile C${sday}???.891.???
       ./ftp_interface.sh  10.233.30.50 opencrm opencrm ${WORK_PATH}/data  /data1/opencrm/zhout/twocityonehome/cfile C${smonth}???.891.???
        
#       ./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${sday}???.???.891
#       ./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${smonth}???.???.891

        if [ -f ${WORK_PATH}/data/C${sday}???.891.??? -o ${WORK_PATH}/data/C${smonth}???.891.??? ] ; then
              cd   $WORK_PATH/data
              cp   C${sday}???.891.??? ${WORK_PATH}/backup
              cp   C${smonth}???.891.??? ${WORK_PATH}/backup
              echo "�ļ�����!"

         ## ���½����޳��ļ�ͷ��¼��β��¼ 
         echo "current_path=`pwd`"
         mark1=1
         mark2=3
         mark=$mark1
         echo $mark
       while [ ${mark} -lt ${mark2} ]
          do 
             if [  "${mark}" = "${mark1}"  ] ; then          
                 ls -l C${sday}???.891.??? |awk '{print $9}' > file.list
                 echo `ls -l C${sday}???.891.??? |awk '{print $9}'`
              
               else 
                 ls -l C${smonth}???.891.??? |awk '{print $9}' > file.list
                 echo `ls -l C${smonth}???.891.??? |awk '{print $9}'`
             fi 
                          
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
	         mark=`expr $mark + 1`
	         echo mark=$mark
          
        done  
	        	       	       
	        echo "�޳��ļ�ͷ��¼��β��¼����!"

        else
              echo "�ļ�������!"
        fi

         #�����ļ��ϲ����жϺϲ��Ƿ�ɹ�
              day_datafile=A99901${sday}000000.AVL
              day_chkfile=A99901${sday}000000.CHK
              month_datafile=M99902${smonth}000000.AVL 
              month_chkfile=M99902${smonth}000000.CHK
              
              echo day_datafile=$day_datafile
              echo day_chkfile=$day_chkfile
              echo month_datafile=$month_datafile
              echo month_chkfile=$month_chkfile 
             
             #�ϲ��������ļ�
              cat C${sday}???.891.???.AVK > ${day_datafile}              
              
              if [ $? -eq 0 ]
              then
               rm ./C${sday}???.891.???.AVK
               rm ./C${sday}???.891.???
    	         echo "�ϲ��ļ��ɹ�!"

    	       #�ļ��ϲ��ɹ�������У���ļ�
    	         day_datafile_byte=`ls -l $day_datafile|awk '{print $5}'`
    	         day_datafile_count=`wc -l $day_datafile|awk '{print $1}'`
    	         day_datafile_date=`date +%Y%m%d%H%M%S`
    	             	         
    	         echo ${day_datafile}\$${day_datafile_byte}\$${day_datafile_count}\$${sday}\$${day_datafile_date} > ./$day_chkfile
    	         
    	         else
                 rm ${day_datafile}
    	           echo "�ϲ��ļ�ʧ��!"
    	           continue
              fi
    	         
    	         #�ϲ��������ļ�
    	         cat C${smonth}???.891.???.AVK > ${month_datafile}
              
               if [ $? -eq 0 ]
               then
                 rm ./C${sday}???.891.???.AVK
                 rm ./C${sday}???.891.???
    	           echo "�ϲ��ļ��ɹ�!"
    	         
    	         month_datafile_byte=`ls -l $month_datafile|awk '{print $5}'`
    	         month_datafile_count=`wc -l $month_datafile|awk '{print $1}'`
    	         month_datafile_date=`date +%Y%m%d%H%M%S`
    	         echo ${month_datafile}\$${month_datafile_byte}\$${month_datafile_count}\$${smonth}\$${month_datafile_date} > ./$month_chkfile

              else
                 rm ${month_datafile}
    	           echo "�ϲ��ļ�ʧ��!"
    	           continue
              fi

        #ȡ���ļ�,�������ļ���У���ļ��ƶ������ݼ���Ŀ¼

        current_interface_code=${start_interface_code}
        while [ ${current_interface_code} -lt ${end_interface_code} ]
        do
                    mv A${current_interface_code}${sday}*.CHK   ${WORK_PATH}
                    mv A${current_interface_code}${sday}*.AVL   ${WORK_PATH}

                    mv M${current_interface_code}${smonth}}*.CHK   ${WORK_PATH}
                    mv M${current_interface_code}${smonth}}*.AVL   ${WORK_PATH}
                    
                    echo "�ļ��ƶ�������Ŀ¼�ɹ���"
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
               datafilename=${sfilename}
               pure_filename=`echo "$sfilename"|cut -c1-20`
		           chkfilename=`echo "$pure_filename".CHK`
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
                     mv ${datafilename} ${TOLOAD_PATH}
               	     mv ${chkfilename}  ${TOLOAD_PATH}
         	           ALARM_CONTENT="�ӿ�${task_id}û�����������Ϣ!"
                     echo ${ALARM_CONTENT}
         	     continue
               else
         	     table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
         	     table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
               fi

               #�ж�У���ļ��Ƿ����
               if [  ! -f ${chkfilename} ] ; then
                   mv ${datafilename}  ${ERROR_PATH}
                   echo "${datafilename}��У���ļ�������!"
                   continue
               fi

               #ȡ���ļ��ϲ���ļ�¼�������ļ���С
               avl_size=`ls -l ${datafilename} |awk '{print $5}'`
               avl_cnt=`wc -l ${datafilename}|awk '{print $1}`

               #ȡ�ô��ļ���У���ļ��ļ�¼���ʹ�С
               chk_size=`grep AVL ${chkfilename} |awk -F$ '{print $2}'`
               chk_cnt=`grep AVL ${chkfilename} |awk -F$ '{print $3}'`


               #added by lizhanyong 2008-10-16	 ������������

 	              file_time=`ls -trl $datafilename |awk '{print $8}`
 	              /bassdb1/etl/L/interface_control.sh $task_id $file_time $avl_cnt $avl_size
 	              cd $WORK_PATH
 	             #added end

               #�����ļ�${sfilename},��������־Ϊ��ʼ
               DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
               DB2_SQL_EXEC > /dev/null

               #�ж��ļ���¼�ʹ�С�Ƿ���У���ļ��е�һ��
               if [  "${avl_size}" = "${chk_size}" -a "${avl_cnt}" = "${chk_cnt}" ] ; then
                    echo "�ӿ�${task_id}�ļ���С����¼����У���ļ��е�һ��!"
               else
                    mv ${datafilename} ${ERROR_PATH}
               	    mv ${chkfilename}  ${ERROR_PATH}
                    ALARM_CONTENT="�ӿ�${task_id}�ļ���С���¼����У���ļ��еĲ�һ��!"

                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
                    echo ${ALARM_CONTENT}
         	     continue
         	    fi


              #ȡ�ýӿڵĶ�����Ϣ
              len_val=`grep "INTERFACE_LEN_${interface_code}" ${EXEC_PATH}/load_2city1home.sh |cut -d= -f2`

	            #��ֹ�ظ�����,ɾ����������
             # if [ ${flag} = "P" -o ${flag} = "M" -o ${flag} = "I" -o ${flag} = "A" ] ; then
              DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
              echo $DB2_SQLCOMM
	            DB2_SQL_EXEC > /dev/null
            #  else
            #        DB2_SQLCOMM="db2 \"delete from ${table_name} where bill_date='${time_id}'\""
            #        DB2_SQL_EXEC > /dev/null
            #  fi

	      #2008-11-03 ����������������޸Ĵ˲��ֳ����ڱ�BASS2.ETL_TASK_RUNNING��BASS2.ETL_TASK_log�����������ֶ�bass_tablename,avl_filename,chk_filename
	      #��¼ETL��־
	      echo table_name=$table_name
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$datafilename','$chkfilename','$task_name',-1,'ETL',current timestamp,'A')\""
	            DB2_SQL_EXEC > /dev/null
	      #2008-11-03 �޸Ľ���

	      echo "����${datafilename}���ݿ�ʼ!"

              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\"  timeformat=\\\"HHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""
              echo $DB2_SQLCOMM
              DB2_SQL_EXEC
              echo ${DB2_SQLCOMM}

        echo "����${datafilename}�������!"

              #�жϼ����Ƿ�ɹ�
              #if [ "${flag}" = "P" -o "${flag}" = "M" -o "${flag}" = "I" -o "${flag}" = "A" ] ; then
                   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
                   loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              #else
              #     DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} where bill_date='${time_id}' \""
              #     loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              #fi

              if [ "${loaded_cnt}" = "${avl_cnt}" ] ; then
              	    echo "${datafilename}���سɹ�,��¼��Ϊ:${avl_cnt},��������־Ϊ���"
              	    error_cnt=`expr $avl_cnt - $loaded_cnt`
              	    mv ${datafilename} ${BACKUP_PATH}
              	    mv ${chkfilename}  ${BACKUP_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$avl_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
              else
                    ALARM_CONTENT="${datafilename}����ʧ��,���ؼ�¼��:${loaded_cnt},�ı���¼��:${avl_cnt},������־Ϊʧ��,���澯"
                    echo ${ALARM_CONTENT}
                    mv ${datafilename} ${ERROR_PATH}
              	    mv ${chkfilename}  ${ERROR_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_dsmp.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC  > /dev/null
             fi
             rm ${WORK_PATH}/${datafilename}
             rm ${WORK_PATH}/${chkfilename}

       done < ./file.lst

       rm ./file.lst
       
       echo "����������ɣ�" 
        
exit