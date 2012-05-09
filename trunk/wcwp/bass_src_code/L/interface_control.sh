#!/bin/ksh
#umask 000
#HOME=/bassapp/report
#export HOME
#. /bassapp/report/.profile
###===================获取日期=============================
#if [ $# -lt 1 ]
#then
#        echo "请在shell后输入日期参数：op_time"
#        echo "格式说明：interface_control.sh  YYYY-MM-DD"
#        exit 1
#fi

op_time=` date +"20%y-%m-%d" `
echo $op_time

_year=`echo  $op_time|awk '{printf("%s",substr($0,1,4))}'`
echo $_year
_month=`echo $op_time|awk '{printf("%s",substr($0,6,2))}'`
echo $_month
_day=`echo   $op_time|awk '{printf("%s",substr($0,9,2))}'`
echo $_day

   date_optime=`echo $op_time|awk '{printf("%s%s%s",substr($0,1,4),substr($0,6,2),substr($0,9,2))}'`
#===============date_optime=20050912====================
   echo  $date_optime

   #cd   /bassapp/report/tcl
   echo "<<<<<<开始判断接文件>>>>>>"
   
   task_id=$1
   time=$2
   rec_cnt=$3
   avl_byte=$4   

   echo task_id=$task_id
   echo time=$time
   echo rec_cnt=$rec_cnt
   echo avl_byte=$avl_byte

   DB2_SQL_EXEC()
   {
       #db2 connect to FM user fm using fm@123
       db2 connect to bassdb user fm using fm
       eval $DB2_SQLCOMM1
       eval $DB2_SQLCOMM2
       eval $DB2_SQLCOMM3
       eval $DB2_SQLCOMM4
       eval $DB2_SQLCOMM5
       db2 commit
       db2 terminate
   }
   echo "conn is ok"
   DB2_SQLCOMM1="db2 \"insert into FM.FM_FILE_INTERFACE_INFO values ('LOAD','$task_id','$_year$_month$_day','$time',1,$rec_cnt,$avl_byte)\""
   echo "$DB2_SQLCOMM1"
   DB2_SQLCOMM2="db2 \"insert into FM.FM_FILE_INTERFACE_INFO values ('EXTRACT','$task_id','$_year$_month$_day','$time',1,$rec_cnt,$avl_byte)\""
   DB2_SQLCOMM3="db2 \"insert into FM.FM_FILE_INTERFACE_INFO values ('FTP','$task_id','$_year$_month$_day','$time',1,$rec_cnt,$avl_byte)\""
   DB2_SQLCOMM4="db2 \"insert into FM.FM_FILE_INTERFACE_INFO values ('CLEAN','$task_id','$_year$_month$_day','$time',1,$rec_cnt,$avl_byte)\""
   DB2_SQLCOMM5="db2 \"insert into FM.FM_FILE_INTERFACE_INFO values ('TRANS','$task_id','$_year$_month$_day','$time',1,$rec_cnt,$avl_byte)\""
      DB2_SQL_EXEC > /dev/null
   echo "insert is ok"

#CREATE TABLE FM.FM_FILE_INTERFACE_INFOs
# (CHECK_TYPE      VARCHAR(32)     NOT NULL,
#  INTERFACE_NAME  VARCHAR(32)     NOT NULL,
#  CHECK_DATE      VARCHAR(32)     NOT NULL,
#  FILE_NUM        INTEGER,
#  FILE_LINE       INTEGER,
#  FILE_SIZE       INTEGER
# )
#  DATA CAPTURE NONE
#  IN TBS_FM;
