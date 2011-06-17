if [ $# -ne 2 ];then 
echo "$0 unit_code time_id"
exit
fi


#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM 
		if [ $? -ne 0 ];then 
		echo "error occurs when running DB2_SQLCOMM!"
		db2 connect reset 
		return 1
		fi
    db2 commit 
    db2 connect reset 
    return 0
}

sendalarmsms(){
	if [ $# -ne 1 ];then 
	echo sendalarmsms msg
	exit
	fi
	MESSAGE_CONTENT=$1
	sql_str="insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) \
	select  ('${MESSAGE_CONTENT}'),MOBILE_NUM from bass1.mon_user_mobile
	"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	DB2_SQL_EXEC > /dev/null
	
  if [ $? -ne 0 ];then 
	echo "error occurs while generating alert msg!"
	return 1
	fi	
	return 0
}

while [ true ]
do
v_unit_code=$1
v_time_id=$2
/bassapp/backapp/bin/bass1_report/bass1_report
	sql_str="select 'xxxxx',count(0) from app.g_runlog \
					 where \
					 unit_code = '${v_unit_code}' \
					 and time_id = ${v_time_id} \
					 and return_flag=1 \
					 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	reclvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
	echo $reclvl_ret_cnt
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	
	if [ ${reclvl_ret_cnt} -eq 1 ];then 
		sendalarmsms "${v_time_id}|${v_unit_code} return normally"
		echo normal exit
		exit
	fi
sleep 60
done
