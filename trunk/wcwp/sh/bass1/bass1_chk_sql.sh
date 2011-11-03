
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

clear
echo "-----------------------------------------------------------"
echo "---sch_control_alarm---------------------------------------"
echo "-----------------------------------------------------------"

sql_str="select *from  app.sch_control_alarm \
	where alarmtime >=  current timestamp - 1 days	\
	and flag = -1	\
	and control_code like 'BASS1%'	\
order by alarmtime desc with ur
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC

echo "-----------------------------------------------------------"
echo "---SMS_SEND_INFO---------------------------------------"
echo "-----------------------------------------------------------"
sql_str="select send_time,substr(trim(message_content),1,100) from   APP.SMS_SEND_INFO	\
where send_time is not null	\
and mobile_num = '15913269062'	\
and send_time >=  current timestamp - 1 days	\
and date(send_time) = char(current date )	\
order by send_time desc with ur
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC

echo "-----------------------------------------------------------"
echo "---chk_same---------------------------------------"
echo "-----------------------------------------------------------"

sql_str="select *from   table( bass1.chk_same(0) ) a order by 2
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC

echo "-----------------------------------------------------------"
echo "---G_FILE_REPORT---------------------------------------"
echo "-----------------------------------------------------------"

sql_str="select *from (	\
select  a.*,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
from APP.G_FILE_REPORT a	\
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'	\
) t where rn = 1
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC
