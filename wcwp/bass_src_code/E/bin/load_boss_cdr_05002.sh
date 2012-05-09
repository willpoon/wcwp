#!/bin/ksh

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

#���ַ����滻����
ReplaceAllSubStr()
{
	echo $1 > sed$$.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
	rm sed$$.temp
	echo $sReturnString
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

#��shell
#1�����ݿ�������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

#������Ŀ¼
ROOT_PATH=/bassdb2/etl/E/boss/xfer
WORK_PATH=${ROOT_PATH}/lzytest
TOLOAD_PATH=${ROOT_PATH}/toload
ERROR_PATH=$ROOT_PATH/error

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}


cd $WORK_PATH
today=`date '+%Y%m%d'`
#sday=`yesterday $today`
#smonth=`echo "$sday"|cut -c1-6`
sday=20090124    
echo $sday
echo $smonth

if [ $# -eq 1 ] ; then
   sday=$1
   smonth=`echo "$sday"|cut -c1-6`  
fi

echo "���ڣ�${sday};�·�:${smonth};"

#Ϊ�˱�֤ά����һ���ԣ�����ά����©�ģ�����ͨ����ȡ���ñ��Щ
#�Ȼ�ȡtable_name_templet��task_name

	#���������ļ���ģʽ�ͺϲ���Ľӿ��ļ�
	task_id="A05002"
	prefixfile="DR_SMS_L_"
	echo $task_id
	echo $prefixfile
	filename="${prefixfile}${sday}.AVL"
	patternfilename="${prefixfile}${sday}*.dat.ok"
	echo "patternfilename:${patternfilename},filename:${filename}"
	
	DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet,task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
	sline=`DB2_SQL_EXEC | grep xxxxxx`
	table_name=`echo $sline| grep xxxxxx|awk '{print $2}'`
	task_name=`echo $sline| grep xxxxxx|awk '{print $3}'`


        #2008-11-03 ����������������޸Ĵ˲��ֳ����ڱ�BASS2.ETL_TASK_RUNNING��BASS2.ETL_TASK_log�����������ֶ�bass_tablename,avl_filename,chk_filename
	#2.3.2�������µ�����
	
        #ʵ�ʱ�������������
	control_code=`echo $task_id|cut -c2-6|awk '{print "TR1_L_"$1}'`
	table_name=`ReplaceAllSubStr $table_name 'YYYYMMDD' $sday`
	table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $smonth`

	#2.3����������ʼ
	#2.3.1��ɾ����������
	DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$sday'\""
	DB2_SQL_EXEC > /dev/null
	
	
	echo table_name=$table_name
	echo filename=$filename
	#echo ��У���ļ�
	DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,script_name,type,module,stime,status) values('$task_id','$sday','$table_name','$filename','$task_name',-1,'ETL',current timestamp,'A')\""
	DB2_SQL_EXEC > /dev/null
	#2008-11-03 �޸Ľ���
	
	if [ "$table_name" = "" ]; then
		#2.3.3.1��û���õ���ģ���
		echo "�ӿ�$task_idû�����������Ϣ��"
		DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='�ӿ�û�����������Ϣ��' where task_id='$task_id' and cycle_id='$sday'\""
		DB2_SQL_EXEC > /dev/null
		
		ALARM_CONTENT="�ӿ�$task_idû�����������Ϣ��"
	        DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(mobile_num,MESSAGE_CONTENT) select PHONE_ID,'${ALARM_CONTENT}' from BASS2.ETL_SEND_MESSAGE where module='LOAD'\""
	        DB2_SQL_EXEC > /dev/null

		cp $patternfilename $TOLOAD_PATH
		continue
	fi

	#ʵ�ʱ������������� ���˲��ֵ���������
	#control_code=`echo $task_id|cut -c2-6|awk '{print "TR1_L_"$1}'`
	#table_name=`ReplaceAllSubStr $table_name 'YYYYMMDD' $sday`
	#table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $smonth`
		
	echo "control_code:${control_code},table_name:${table_name}"
		
	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
	DB2_SQL_EXEC > /dev/null

	#2.3.3.6��������ݱ�
	DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
	DB2_SQL_EXEC > /dev/null

		
	#�ϲ���һ�������ļ�
	echo "CATING ..."	
	cat $patternfilename > $filename
	
	#2.3.3.5���˶�����
	rec_cnt=`wc -l $filename |awk '{print $1}'`
	rec_cnt=${rec_cnt:=0}
	echo "rec_cnt:$rec_cnt"
	
	#added by lizhanyong 2008-10-17	 ������������
	 file_time=`ls -trl $filename |awk '{print $8}` 
	 avl_byte=`ls -trl $filename |awk '{print $5}`            
	/bassdb2/etl/L/interface_control.sh $task_id $file_time $rec_cnt $avl_byte
	     
	#added end   

	
	#2.3.3.7����������
	echo "LOADING ..."
	load_cnt=0
	if [ $rec_cnt -gt 0 ]; then	
		  
		DB2_SQLCOMM="db2 \"load client from $WORK_PATH/$filename of del \
					modified by coldel; timestampformat=\\\"YYYYMMDDHHMMSS\\\" fastparse anyorder \
					method p(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37, \
					38,39,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74, \
					75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,93,94,95) \
					warningcount 1000 \
					messages /dev/null \
					replace into bass2.CDR_sms_test\"";
					
		echo ${DB2_SQLCOMM}				
		DB2_SQL_EXEC > /dev/null
		
	
		#2.3.3.8����¼�����¼������Ϣ
		sleep 10
		DB2_SQLCOMM="db2 \"select 'xxxxxx',sum(1) from bass2.CDR_sms_test\""
		load_cnt=`DB2_SQL_EXEC| grep xxxxxx|awk '{print $2}'`
		load_cnt=${load_cnt:=0}
	fi

	echo "load_cnt:$load_cnt"

	#2.3.3.9�������жϵ����Ƿ���ȷ��¼��Ӧ����־
	if [ ${rec_cnt:=0} = ${load_cnt:=0} ] ; then
        #2.3.3.9.1��������ȫ��ȷ
        error_cnt=0
        
        ALARM_CONTENT=""
        status="C"
        flag=0
    else
        let error_cnt=rec_cnt-load_cnt
        if [ $rec_cnt -ne 0 -a $load_cnt -eq 0 ]; then
        	#2.3.3.9.3������ȫ������
        	ALARM_CONTENT="�ӿ��ļ�:${filename}�����¼ȫ��ʧ��,ԭ��δ����"
			status="F"
			flag=-1
        else
        	#2.3.3.9.4�����벿�ֳ���(��ʱ���澯,�治Ӱ�����ִ��)
        	ALARM_CONTENT="�ӿ��ļ�:${filename}����ʱ����ʧ��,ʧ�ܼ�¼��Ϊ${error_cnt}!"
        	status="C"
        	flag=0
        fi
        
    	DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(mobile_num,MESSAGE_CONTENT) select PHONE_ID,'${ALARM_CONTENT}' from BASS2.ETL_SEND_MESSAGE where module='LOAD'\""
    	DB2_SQL_EXEC > /dev/null
        	
        cp $patternfilename $ERROR_PATH
    fi
    
	DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=${flag} where CONTROL_CODE='${control_code}'\""   
	DB2_SQL_EXEC  > /dev/null
	DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='$status',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$rec_cnt,load_sheet_cnt=$load_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$sday'\""
	DB2_SQL_EXEC > /dev/null
	DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$sday' and status='C'\""
	DB2_SQL_EXEC > /dev/null
	DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$sday' and status='C'\""
	DB2_SQL_EXEC > /dev/null

	#2.3.3.10��ɾ���ӿ��ļ�
	echo "�ӿ��ļ�${filename}������ϡ�"
	#rm $filename
	#rm $patternfilename

echo "��������������"
