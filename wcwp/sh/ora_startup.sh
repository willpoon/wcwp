#startup database instance
check_stat=`ps -ef|grep ${ORACLE_SID}|grep pmon|wc -l`;
oracle_num=`expr $check_stat`
if [ $oracle_num -gt 0 ]
then
exit 0
else
sqlplus sys/oracle as sysdba <<eof             
startup;
eof
fi

