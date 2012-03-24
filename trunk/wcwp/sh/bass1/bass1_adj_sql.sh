#~ test case:BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl
#########################################################################################
#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC(){
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM 
    #~ echo $DB2_SQLCOMM
	db2ret=$?
	#~ test ${db2ret} -ne 0 -o ${db2ret} -ne 1
	#~ testval=$?
		if [ ! ${db2ret} -eq 0 -a ! ${db2ret} -eq 1 ];then
		echo ����������"error occurs when running DB2_SQLCOMM!"
		db2 connect reset 
		echo ����������"DB2_SQLCOMM�쳣�˳�!"		
		exit 1
		fi
    db2 commit 
    db2 connect reset 
    return 0
}

#########################################################################################
#~ �澯�����Ѵ���
fnSetDeal(){

#~ �����������
ControlCode=$1

	if [ ${ControlCode} = "" ];then
	exit 1
	fi
	
#~ �����ѯ������
	iCount="
	select count(0) cnt,'xxx' from  app.sch_control_alarm \
	where alarmtime >=  current timestamp - 1 days \
	and flag = -1	\
	and control_code  = '${ControlCode}'	\
	with ur
	"
	DB2_SQLCOMM="db2 \"${iCount}\""
	DB2_SQL_EXEC>.tmp$$
	RetCount=`cat .tmp$$|egrep 'xxx'|head -1|awk '{print $1}'`
	if [ ${RetCount} -lt 1 ];then
	rm .tmp$$
	echo ����������û�пɸ��¸澯��
	return 1
	fi

#~ ִ�е��ȸ���
	SetDeal="
	update (
	select *from  app.sch_control_alarm 
	where alarmtime >=  current timestamp - 1 days
	and flag = -1
	and control_code  = '${ControlCode}'
	)t 
	set flag = 1
	"
    echo �������������¸澯��ϣ�
		
	DB2_SQLCOMM="db2 \"${SetDeal}\""
	DB2_SQL_EXEC>/dev/null
	return 0
}
#########################################################################################
ifRunLog(){
#~ �����������
ControlCode=$1

	if [ ${ControlCode} = "" ];then
	exit 1
	fi
	
#~ �����ѯrunlog������
	iCount="
	select count(0) cnt,'xxx' from  app.sch_control_runlog \
	where  control_code = '${ControlCode}' and flag = -1 \
	with ur
	"
	DB2_SQLCOMM="db2 \"${iCount}\""
	DB2_SQL_EXEC>.tmp$$
	RetCount=`cat .tmp$$|egrep 'xxx'|awk '{print $1}'`
	if [ ${RetCount} -ne 1 ];then
	     echo ����������û�пɸ��µ�runlog��¼��
		exit 2
	fi
	rm .tmp$$
	return ${RetCount}
}
#########################################################################################
#~ �����
fnSetDone(){
echo ��������������Ƿ��������ɣ�${ControlCode}...
	ControlCode=$1
	if [ ${ControlCode} = "" ];then
	exit 1
	fi
	ifRunLog ${ControlCode};
	retVal=$?
	if [ ${retVal} -ne 1 ];then
	echo ����������runlog��ѯ�õ��ļ�¼�������⣡
	exit 1
	fi	
	SetDone="
			update 
			(
				select *from  app.sch_control_runlog
				where   control_code = '${ControlCode}'
				and flag = -1
			) t
			set  flag = 0
			"
	DB2_SQLCOMM="db2 \"${SetDone}\""
	DB2_SQL_EXEC>/dev/null
echo ����������${ControlCode}�����OK��
return 0
}
#########################################################################################
#~  ������ 
fnSetReDo(){
echo ��������������Ƿ���� ������ ${ControlCode}...
	ControlCode=$1
	if [ ${ControlCode} = "" ];then
		exit 1
	fi
	
	ifRunLog ${ControlCode};
	retVal=$?
	if [ ${retVal} -ne 1 ];then
		echo ����������runlog��ѯ�õ��ļ�¼�������⣡
	exit 1
	fi	
	
echo ���������� ������ ${ControlCode}...
SetRedo="
update 
(
	select *from  app.sch_control_runlog
	where   control_code  = '${ControlCode}'
	and flag = -1
) t
set  flag = -2
"
DB2_SQLCOMM="db2 \"${SetRedo}\""
DB2_SQL_EXEC>/dev/null
echo ���������� ������ ${ControlCode} ok!

return 0
}


#~  ������ 2
fnSetReDo2(){
	ControlCode=$1
	if [ ${ControlCode} = "" ];then
		exit 1
	fi
	
	
echo ���������� ������ ${ControlCode}...���۵����Ƿ�澯��
SetRedo="
update 
(
	select *from  app.sch_control_runlog
	where   control_code  = '${ControlCode}'
	and begintime >=  current timestamp - 1 days
) t
set  flag = -2
"
DB2_SQLCOMM="db2 \"${SetRedo}\""
DB2_SQL_EXEC>/dev/null
echo ���������� ������ ${ControlCode} ok!

return 0
}

#########################################################################################

if [ $# -eq 1 ];then 
clear
echo "-----------------------------------------------------------"
echo "---sch_control_alarm---------------------------------------"
echo "-----------------------------------------------------------"
sql_str="select CONTROL_CODE,ALARMTIME,FLAG,substr(trim(CONTENT),1,100) CONTENT from  app.sch_control_alarm \
        where alarmtime >=  current timestamp - 1 days  \
        and flag = -1   \
        and control_code like 'BASS1%'  \
order by alarmtime desc with ur
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC
fi

#~ start here...
if [ $# -ne 2 ];then 
echo ����������$0 [ -r/d/or] [control code]
exit 1
fi
type=$1
ControlCode=$2

if [ ${type} = '-d' -a ${ControlCode} != "" ];then 
	fnSetDeal ${ControlCode}
	fnSetDone ${ControlCode}
elif [ ${type} = '-r' -a ${ControlCode} != "" ];then
		fnSetDeal ${ControlCode}
		fnSetReDo ${ControlCode}
elif [ ${type} = '-or' -a ${ControlCode} != "" ];then
		fnSetReDo2 ${ControlCode}		
else 
		echo ����������invalid argument!
fi

exit 0
