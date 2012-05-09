#!/bin/sh
#*************************************************************************************************************
#��������load_mgame.sh
#��  �ܣ���10.233.26.55����mgameĿ¼ȡ�����ļ�,�������
#��  ������
#
#��д��: lizhanyong
#��дʱ�� 2010-04-30
#
#��  ��: �ӿڼ������
#ע�������shell��crontab����,�����е�����ftp_mgame.sh
#
#ִ�з�ʽ������ÿ����crontab����ִ�У�Ҳ���ֹ�ִ�С��ֹ�ִ�п��Ը�һ��������Ҳ���Ը�����������ִ�з�ʽ���£�
#          ִ�з�ʽ:load_mgame.sh yyyymmdd ,Ϊȡ�ض�ĳһ��Ľӿڣ���./load_mgame.sh 20100401 Ϊȡ2010��4��1�ŵ�ȫ���ӿ��ļ�
#          ִ�з�ʽ:load_mgame.sh yyyymmdd �ӿڱ�� ,Ϊȡ�ض�ĳһ���ĳ���ӿڣ���./load_mgame.sh 20100401 I43001 Ϊȡ 2010��4��1�Žӿ�I43001���ļ�
#
#����˵�����ӿ�ԭ�ļ�������'\t'���Ʊ�����ָ��,���²����ǲ����Ʊ���Ļ�ȡ��ʽ
#     	   a=a
#     	   b=b
#     	   c=c
#     	   echo $a["\t"]$b[" "]$c
#     	   �滻�Ʊ������䣬sed 's/	/$/g' ��ע��˴�������'\t',��Ҫ��һ��TAB��
#     	   ��db2 load ��������ָ���ָ��Ϊ�Ʊ��������Ҫ���ļ��е��Ʊ�������滻
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
echo `date` >> /bassdb1/etl/L/mgame/db2_connect.log
echo $DB2_SQLCOMM >> /bassdb1/etl/L/mgame/db2_connect.log

conn_cnt=0
while [ $conn_cnt -lt 3 ]
do
	db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
	if [ $? != 0 ] ; then
		echo "���ݿ�����ʧ�ܣ�" >> /bassdb1/etl/L/mgame/db2_connect.log
		conn_cnt=`expr ${conn_cnt} + 1`
		echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/mgame/db2_connect.log
	else
		echo "���ݿ����ӳɹ���" >> /bassdb1/etl/L/mgame/db2_connect.log
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

#�ӿں���ӿ��ļ���ƥ�亯��
MatchFileName()
{
	if [ "$1"="I43001" ] ; then
	  INTERFILE_NAME="KPI_FEEUSER"	  
  else if [ "$1"="I43002" ] ; then
  	INTERFILE_NAME="ORDER"
  else if [ "$1"="I43003" ] ; then
    INTERFILE_NAME="SERVICE"
  else if [ "$1"="I43004" ] ; then
    INTERFILE_NAME="USER"
  else if [ "$1"="I43004" ] ; then
  	INTERFILE_NAME="USER_TST"   
  fi
  fi
  fi
  fi
  fi 	 	
}

#�ӿںŶ�Ӧ�ļ���Ϣ
   #I43001��KPI�����û��嵥        
   #I43002����Ϸҵ���ײͶ����嵥
   #I43003���ֻ���Ϸҵ���嵥
   #I43004����Ϸҵ��ʹ���û��嵥
   #I43005��������Ϸҵ��ʹ���û��嵥 

#������Ϣ

PROGRAM_NAME="load_mgame.sh"
WORK_PATH=/bassdb1/etl/L/mgame
DATA_PATH=/bassdb1/etl/L/mgame/data
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

echo "�����ֻ���Ϸ�ӿ����ݿ�ʼ ..."
date

   
   
#�ֹ�ִ�У���һ������ʱ���滻����
if [ $# -eq 1 ] ; then
   sday=$1 
   yyyymm=`echo "$sday"|cut -c1-6`
   
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame KPI_FEEUSER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ORDER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame SERVICE${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER_TST${sday}891.???.dat  
   
#�ֹ�ִ�У�����������ʱ���滻���ںͽӿڱ��룬���ҽ�����ӿڱ����Ӧ���ļ�
 else if [ $# -eq 2 ] ; then 
	 sday=$1  
	 yyyymm=`echo "$sday"|cut -c1-6`    
	 INTERFACE_CODE=$2
	 	
	 	#��ȡ�ӿںŶ�Ӧ�Ľӿ��ļ��� 
	 MatchFileName $INTERFACE_CODE
	 	 
	 echo "��ʼ����ӿں�${INTERFACE_CODE}��Ӧ���ļ�${INTERFILE_NAME}${sday}891.???.dat ...	"
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ${INTERFILE_NAME}${sday}891.???.dat	
   
else 
  #��������ʱ����������������
  echo "��ʼ��������ӿ��ļ� ..."   
  
   today=`date '+%Y%m%d'`
   sday=`yesterday $today`
   yyyymm=`echo "$sday"|cut -c1-6`
  
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame KPI_FEEUSER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ORDER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame SERVICE${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER_TST${sday}891.???.dat
  
fi
fi

##########����KPI_FEEUSER${sday}891.???.dat�ļ�������AVL��CHK�ļ�

  
     if [ -f KPI_FEEUSER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43001${sday}000000.AVL
     	     chk_file_name=I43001${sday}000000.CHK
     	     data_file_tmp=I43001${sday}000000.tmp
       	                 
           #�����ʱ��$���ָ����������Ƚ��ļ��е�$�޳����������Ʊ���滻Ϊ$��
            sed 's/\$//g' KPI_FEEUSER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' KPI_FEEUSER${sday}891.???.dat > $data_file_name
            #cp KPI_FEEUSER${sday}891.???.dat  $data_file_name
            mv KPI_FEEUSER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #ȡ�ô�С,��¼����
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #�ļ�����ʱ��
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #����У���ļ�
            echo "I43001${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
###########����ORDER${sday}891.???.dat�ļ�������AVL��CHK�ļ�
  
     if [ -f ORDER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43002${sday}000000.AVL
     	     chk_file_name=I43002${sday}000000.CHK
     	     data_file_tmp=I43002${sday}000000.tmp
  
           #�����ʱ��$���ָ����������Ƚ��ļ��е�$�޳����������Ʊ���滻Ϊ$��
            sed 's/\$//g' ORDER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp ORDER${sday}891.???.dat  $data_file_name
            mv ORDER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #ȡ�ô�С,��¼����
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #�ļ�����ʱ��
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #����У���ļ�
            echo "I43002${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
###########����SERVICE${sday}891.???.dat�ļ�������AVL��CHK�ļ�
  
     if [ -f SERVICE${sday}891.???.dat ] ; then
  
     	     data_file_name=I43003${sday}000000.AVL
     	     chk_file_name=I43003${sday}000000.CHK
           data_file_tmp=I43003${sday}000000.tmp
            
            #�����ʱ��$���ָ����������Ƚ��ļ��е�$�޳����������Ʊ���滻Ϊ$��
     	      sed 's/\$//g' SERVICE${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp SERVICE${sday}891.???.dat  $data_file_name
            mv SERVICE${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #ȡ�ô�С,��¼����
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #�ļ�����ʱ��
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #����У���ļ�
            echo "I43003${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
##########����USER${sday}891.???.dat�ļ�������AVL��CHK�ļ�
  
     if [ -f USER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43004${sday}000000.AVL
     	     chk_file_name=I43004${sday}000000.CHK
     	     data_file_tmp=I43004${sday}000000.tmp
  
            #�����ʱ��$���ָ����������Ƚ��ļ��е�$�޳����������Ʊ���滻Ϊ$��
     	      sed 's/\$//g' USER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            cp $data_file_name $data_file_tmp
            sed 's///g' $data_file_tmp > $data_file_name
            #cp USER${sday}891.???.dat  $data_file_name
            mv USER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #ȡ�ô�С,��¼����
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #�ļ�����ʱ��
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #����У���ļ�
            echo "I43004${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
###########����USER_TST${sday}891.???.dat�ļ�������AVL��CHK�ļ�
  
     if [ -f USER_TST${sday}891.???.dat ] ; then
  
     	     data_file_name=I43005${sday}000000.AVL
     	     chk_file_name=I43005${sday}000000.CHK  
     	     data_file_tmp=I43005${sday}000000.tmp
            
            #�����ʱ��$���ָ����������Ƚ��ļ��е�$�޳����������Ʊ���滻Ϊ$��
            sed 's/\$//g' USER_TST${sday}891.???.dat > $data_file_tmp 
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp USER_TST${sday}891.???.dat  $data_file_name
            mv USER_TST${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #ȡ�ô�С,��¼����
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #�ļ�����ʱ��
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #����У���ļ�
            echo "I43005${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
################################���²��ֿ�ʼ�������ݼ���######################################################
  
   ls -l *.AVL |awk '{print $9}' > ./mgame_loadfile.lst
   echo `ls -l *.AVL |awk '{print $9}'`
  
   while read sfilename
     do
  
    #1�����û����Ҫ���ص��ļ���ֱ���˳�
     if [ "${sfilename}" = "" ] ; then
         break
     fi
  
    echo "===============================���ؽӿ��ļ�${sfilename}��ʼ============================================"
  
    #2����ȡ�ӿڴ�������ڡ���������
  
       #��ȡ�ӿںš������ļ�����У���ļ���
  	  task_id=`echo "$sfilename"|cut -c1-6`
  	  avl_filename=${sfilename}
  	  pure_filename=`echo "$sfilename"|cut -c1-20`
  	  chk_filename=`echo "$pure_filename".CHK`
  
  	  #�ӿڼ��ص�������
  	  control_code=`echo $task_id|cut -c1-6`
  	  control_code=`echo TR1_L_$control_code`
  
  	  #�ӿڼ����ļ��ϵ�������Ϣ
  	   month_id=`echo ${sfilename}|cut -c7-12`
       day_id=`echo ${sfilename}|cut -c7-14`
  
      #��ȡ�ӿ��ļ�����������
       sfrequency=`echo "$sfilename"|cut -c1-1`
       if [ $sfrequency = "M" ]; then
  	    	scycleid=`echo $month_id`
  	    else
  	      scycleid=`echo $day_id`
  	   fi
  
  	 echo task_id=$task_id
  	 echo avl_filename=$avl_filename
  	 echo chk_filename=$chk_filename
  	 echo scycleid=$scycleid
  	 echo control_code=$control_code
  
  
  	#3���ж϶�Ӧ��У���ļ��Ƿ����,������������������ѭ����������һ�ӿڴ���
     if [ ! -f ${WORK_PATH}/${chk_filename} ]  ; then
       echo "�ӿڼ��ظ澯��Ϣ�������ļ�${sfilename}��Ӧ��У���ļ�������!"
  	   sleep 30
       continue
  
  	 else
      #ͳ�������ļ���¼�����ļ���С����¼У���ļ��м�¼��
  
  		chk_cnt=`grep AVL $chk_filename |awk -F$ '{print $3}'`
  		avl_cnt=`wc -l $avl_filename |awk '{print $1}'`
  		avl_byte=`ls -trl $avl_filename |awk '{print $5}'`
  		file_time=`ls -trl $avl_filename |awk '{print $8}'`
  
  		echo chk_cnt=$chk_cnt
  		echo avl_cnt=$avl_cnt
  
     fi
  
    #4���жϽӿ��ļ���¼����У���ļ���¼�Ƿ�һ��
  		if [ $chk_cnt -ne $avl_cnt ]; then
  			ALARM_CONTENT=`echo "�ӿڼ��ظ澯��Ϣ���ӿ�����Ϊ${task_name}���ӿ��ļ�${avl_filename}��${chk_filename}�ļ���¼����������"`  			
  		  #������ȸ澯��
  			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  			DB2_SQL_EXEC
  			echo ALARM_CONTENT=$ALARM_CONTENT				
  
  
  		  #���ļ��Ƶ�ERROR_PATH
  			mv $sfilename $ERROR_PATH
  		  mv $chk_filename $ERROR_PATH
  			continue
  		fi
  
  
    #5����ȡ����ģ��������ӿ����ơ���ⷽʽ
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo table_name_templet=$table_name_templet
      echo task_name=$task_name
      echo load_method=$load_method
  
  
  	#6���жϽӿڵ���ģ����Ƿ�����
  	 if [ "$table_name_templet" = "" ]; then
  		#��û���õ���ģ�����ֱ�Ӹ澯
  		ALARM_CONTENT=`echo "�ӿڼ��ظ澯��Ϣ���ӿ�$task_idû�����������Ϣ����ETL_LOAD_TABLE_MAP��"`  		
  		DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  	  DB2_SQL_EXEC
  	  echo ALARM_CONTENT=$ALARM_CONTENT		  
  
  		#�ӿ��ļ�����toloadĿ¼��������һ�ӿڴ���
  		mv $avl_filename $TOLOAD_PATH
  		mv $chk_filename $TOLOAD_PATH
  		continue
  
    else
     #����õ��ӿڱ�����
  	   tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $day_id`
  	   tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $month_id`
  
       echo tablename=$tablename
       echo avl_filename=$avl_filename
       echo chk_filename=$chk_filename
  
     fi
  
    #7���жϽӿڼ���Ŀ����Ƿ����
      DB2_SQLCOMM="db2 \"select 'xxxxxx',tabname from syscat.tables where tabname=upper('$tablename')\""
  		table_name_target=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo table_name_target=$table_name_target
  		if [ "${table_name_target}" = "" ]; then
  			ALARM_CONTENT=`echo "�ӿڼ��ظ澯��Ϣ���ӿ�$task_id����Ŀ���$tablename�����ڣ�"`        
  			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  			DB2_SQL_EXEC
  			echo ALARM_CONTENT=$ALARM_CONTENT
        
  
  			#�ӿ��ļ�����toloadĿ¼��������һ�ӿڴ���
  			mv $avl_filename $TOLOAD_PATH
  			mv $chk_filename $TOLOAD_PATH
  			continue
  	  fi
  
  
  
   #8������ӿ���������ʼ
  
  	 #8.1�����ü�������
  	  #ɾ����������
  	  echo "ɾ���������� ..."
  		DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	  #�����µ�����
  	  echo "�����µ����� ..."
  		DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$tablename','$avl_filename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	 #8.2�����µ�����־��flag=1��ʾ����ִ��
  		echo "���µ�����־������ʱ�䣬״̬��Ϣ ..."
  		DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	 #8.3����������:load_method="0"����LOAD��load_method=1����import
  			if [ $load_method = "0" ]; then
  				echo "LOADING ..."
  				DB2_SQLCOMM="db2 \"load client from $WORK_PATH/$avl_filename of del modified by coldel$ fastparse anyorder warningcount 1000 messages /dev/null replace into $tablename\""
  				echo ${DB2_SQLCOMM}
  				DB2_SQL_EXEC
  
  			else
  				echo "IMPORT ..."
  				DB2_SQLCOMM="db2 \"import from $WORK_PATH/$load_avl_filename of del modified by coldel$ replace into $tablename\""
  				echo ${DB2_SQLCOMM}
  				DB2_SQL_EXEC
  
  			fi


		 #8.4����ȡ�����¼������Ϣ
		  echo "GET LOAD RECORD ..."
			DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from bass2.$tablename\""
			loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
			echo loaded_cnt=$loaded_cnt

		 #8.5���жϵ����Ƿ���ȷ����¼��Ӧ����־(�ڴ���Ҫ�û��ַ����ȽϷ�ʽ,�����ѯʧ�������ֱȽϽ�����)
			if [ "${avl_cnt}" = "${loaded_cnt}" ] ; then
			  #8.5.1��������ȫ��ȷ
			  
			  error_cnt=`expr $avl_cnt - $loaded_cnt`
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$chk_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""
				DB2_SQL_EXEC > /dev/null 	  				
				echo "�ӿڼ���У�飺���ݼ�����ȫ��ȷ��"
				
				#���ݼ�����ȫ��ȷ�����������������������
	       /bassdb1/etl/L/interface_control.sh $task_id $file_time $avl_cnt $avl_byte
	   
	      #���ݼ�����ȫ��ȷ�����ݽӿ��ļ���dataĿ¼
	       cd $WORK_PATH
			   mv $avl_filename $DATA_PATH
			   mv $chk_filename $DATA_PATH
									      			
      else
      if [ "${loaded_cnt}" = "" ] ; then
        #8.5.2���������(�ӱ��в�ѯ��¼��ʧ��,һ���Ǳ��쳣,��˿���ȷ������Ҳʧ����)
        ALARM_CONTENT=`echo �ӿڼ���У�飺"�ļ�${sfilename},��ѯ�����¼��ʧ�ܣ�"`       	
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$chk_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
				DB2_SQL_EXEC > /dev/null				
				mv $avl_filename $ERROR_PATH
			  mv $chk_filename $ERROR_PATH
			  echo ALARM_CONTENT=$ALARM_CONTENT
			  		  
      else
        error_cnt=`expr $avl_cnt - $loaded_cnt`
        if [ ${error_cnt} -ne 0 ]; then
			    #8.5.3���������
			    ALARM_CONTENT=`echo "�ӿڼ���У�飺�ӿ��ļ�$sfilename�����¼ʧ��,ʧ�ܼ�¼��Ϊ${error_cnt}��"`			    	
			    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$chk_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
			    DB2_SQL_EXEC > /dev/null
			    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
			    DB2_SQL_EXEC > /dev/null
			    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			    DB2_SQL_EXEC > /dev/null
			    mv $avl_filename $ERROR_PATH
			    mv $chk_filename $ERROR_PATH
			    echo ALARM_CONTENT=$ALARM_CONTENT			    	    
		    fi
      fi                 			
		fi
		
		echo "����ӿ��ļ�$sfilename��ϣ�"
		
	done < ./mgame_loadfile.lst
	rm ./mgame_loadfile.lst
	
echo "�������ݽ���!"
exit 0

