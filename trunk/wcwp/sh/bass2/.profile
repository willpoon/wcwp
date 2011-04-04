export PATH=$PATH:/bassapp/bass2/panzw2/bin
base_dir=/bassapp/bass2/panzw2
alias lt='ls -alrt'
alias llog='$base_dir/ViewLoadLog_testdb.sh'
alias conn='db2 terminate;db2 connect to bassdb'
#alias term='db2 terminate'
alias tf='tail -f nohup.out'
alias gol='cd /bassapp/bass2/load/boss'
alias goif='cd /bassapp/bass2/ifboss'
alias vipr='vi /bassapp/bass2/panzw2/.profile'
alias pzh='cd /bassapp/bass2/panzw2'
#alias desc='db2 describe table '
alias gocrm='cd /bassapp/bass2/ifboss/crm_interface/bin/config/CRM'
alias gocfg='cd /bassapp/bass2/ifboss/crm_interface/bin/config/'
descf() { db2 describe table $1 | awk -F" " '{print $1}'
}

ifconn(){
db2 values 1 > /dev/null
if [ $? -ne 0 ];then
conn > /dev/null
echo now connected
else 
echo connected
fi
}


term(){
db2 terminate > /dev/null
}

desc(){
db2 describe table  $1
}

conn(){
term 
my_pass=`${base_dir}/decode  0312004131`
db2 connect to BASSDB user bass2 using ${my_pass} > /dev/null
}

conn56(){
term
my_pass=`${base_dir}/decode  0312004131`
db2 connect to BASSDB56 user bass2 using ${my_pass} > /dev/null
}

desf(){
ifconn
desc $1|\
awk -F" " '{printf "\t%-20s\t%-20s\n" ,$1,$3"("$4")"}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'   
}

dest(){
ifconn
desc $1|\
nawk -F" " '{ if( $3 ~ /CHAR/ ) 
{
 printf "\t%-20s\t%-20s\n" ,","$1,$3"("$4")";
} 
else 
if( $3 ~ /DECIMAL/ ) 
{printf "\t%-20s\t%-20s\n" ,","$1,$3"("$4","$5")";}
else
{ printf "\t%-20s\t%-20s\n" ,","$1,$3; } }'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
}

desf1(){
ifconn
desc $1|\
awk -F" " '{if( NR >= 6 ) {printf "\t%s\n" ,","$1} else {printf "\t%s\n" ," "$1}}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
}


if [ `echo $0` != "ksh" ] ; then 
ksh
fi

if [ `echo $0` = "ksh" -o  `echo $0` = "bash"  ];then 
set -o vi
fi

#########
#javaetl="java ETLMain 20101202 taskList_tmp_pzw.properties"


gb(){
target_table=$1
dimension_field=$2
echo "\n"
echo "select ${dimension_field} , count(0) \n--,  count(distinct ${dimension_field} ) \r
from ${target_table} \r
group by  ${dimension_field} \r
order by 1 "
echo "\n"
}


resset(){
unset i
unset res
unset rs_set
rs_set="puts \$f \"\$"
i=0
while [ $i -lt $1 ]
do 
if [ $i -lt 10 ];then 
res="result0$i"
else 
res="result$i"
fi
echo "    set $res [lindex \$p_row $i]"
rs_set=${rs_set}"{"${res}"}\\\$\$"
i=`expr $i + 1`
done 
echo ${rs_set}"\""
}


#子字符串替换函数
ReplaceAllSubStr()
{
	echo $1 > sed$$.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
	rm sed$$.temp
	echo $sReturnString
	return 1
}


crtab(){
#建表函数：
#1.只需要提供模板表和日期，不需要指定表空间，默认为模板表得表空间
#2.运行之前要先建立数据库连接
tablename=""
TABLE_NAME_TEMPLET=""
TBSPACE=""
INDEX_TBSPACE=""
PARTITIONKEY=""
TABLE_NAME_TEMPLET=$1
DT=$2

if [ $# -ne 2 ];then 
echo run script like :
echo crt.sh DWD_NG2_A02025_YYYYMMDD 20101231
return -1
fi

#TABLE_NAME_TEMPLET=DWD_NG2_A02025_YYYYMMDD
#DT=20101231

LEN=`echo $DT|awk '{print length($1)}'`


if [ $LEN -eq 8 ];then 
tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMMDD' $DT`
else 
 	if [ $LEN -eq 6 ];then 
	tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMM' $DT` 
	else 
	echo tablename not set!
	return -1
	fi
fi

TBTMPLET=""
db2 "select 'xxxxx',tabname from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBTMPLET

if [ -z $TBTMPLET ];then
echo TBTMPLET not set!
return -1
fi

TBSPACE=""
db2 "select 'xxxxx',TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBSPACE

if [ -z $TBSPACE ];then 
echo TBSPACE not set!
return -1 
fi


INDEX_TBSPACE=""
db2 "select 'xxxxx',INDEX_TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read INDEX_TBSPACE

if [ -z $INDEX_TBSPACE ];then 
echo INDEX_TBSPACE not set!
return -1
fi


PARTITIONKEY=""
db2 "select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname from syscat.columns \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' and partkeyseq >= 1 order by partkeyseq"|\
grep xxxxx|awk '{print $2}'|awk 'BEGIN{a=""}{for (i = 1; i <= NF; i++) a=a$1}END {print a}'|read PARTITIONKEY


if [ -z $PARTITIONKEY ];then 
echo PARTITIONKEY not set!
return -1
fi

sql="create table ${tablename} like ${TABLE_NAME_TEMPLET} in ${TBSPACE} index in ${INDEX_TBSPACE} partitioning key ( ${PARTITIONKEY} ) using hashing not logged initially "
echo $sql
#db2 $sql
}


netrc(){
machine 172.16.9.25
login bass2
password bass2
machine 172.16.5.44
login bass2
password bass2

machine 172.16.5.130
login bass
password 3jysjbx
}


ftp44(){
HOME=${base_dir}
export HOME
ftp -v 172.16.5.44
HOME=/bassapp/bass1
export HOME
}

ftp130(){
HOME=${base_dir}
export HOME
ftp -v 172.16.5.130
HOME=/bassapp/bass1
export HOME
}

