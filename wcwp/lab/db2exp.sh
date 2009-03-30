#purpose : export db2 table to a flat file
. $PZW_INC/common.ksh
if [ $# -lt 2 ] ; then
echo "not enough args provided!"
echo 'usage:db2exp <tablename> <output_filename> <"column_delimiter">'
exit 1
fi

if [ $# -gt 2 ] ; then
exp_sql="
export to $2 of del
modified by coldel$3 nochardel striplzeros decplusblank 
select
*
from $1;
"
else 
exp_sql="
export to $2 of del
modified by  nochardel striplzeros decplusblank 
select
*
from $1;
"
fi
db2_exec_sql "$exp_sql"
db2_disconnect 
