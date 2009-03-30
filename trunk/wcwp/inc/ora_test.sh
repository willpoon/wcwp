#!/usr/bin/ksh
#author : panzw 
. $PZW_INC/common.ksh

sql="select * from ref.tr_dir_typ;"
ora_exec_sql "$sql"
exit
