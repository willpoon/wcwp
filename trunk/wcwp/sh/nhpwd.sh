echo nohup sqlplus zhengfengmei/oracle123@gzdm @`$1`/dmin_run.sql>sqlout 2>&1 &
nohup sqlplus -s zhengfengmei/oracle123@gzdm @`$1`/dmin_run.sql>sqlout 2>&1 &
