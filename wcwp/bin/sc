#######################################################################################################
#!/usr/bin/ksh
#author : panzw 
#purse : perform a quick dupliacte_checksum
. $PZW_INC/common.ksh
#######################################################################################################
if [  $# -eq 1 ] 
then 
count_field="usr_nbr"
else 
count_field=$2
fi
select_count="
select count(0) 
from $1
;
"
echo " "
echo " "
echo  "select count(0) from $1 ;"
ora_exec_sql "$select_count"

