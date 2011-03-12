#!/usr/bin/ksh
#子字符串替换函数
ReplaceAllSubStr()
{
        echo $1 > sed$$.temp
        sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
        rm sed$$.temp
        echo $sReturnString
        return 1
}


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
exit
fi


LEN=`echo $DT|awk '{print length($1)}'`


if [ $LEN -eq 8 ];then
tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMMDD' $DT`
else
        if [ $LEN -eq 6 ];then
        tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMM' $DT`
        else
        echo tablename not set!
        exit
        fi
fi


TBSPACE=""
db2 "select 'xxxxx',TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBSPACE

if [ -z $TBSPACE ];then
echo TBSPACE not set!
exit
fi


INDEX_TBSPACE=""
db2 "select 'xxxxx',INDEX_TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read INDEX_TBSPACE

if [ -z $INDEX_TBSPACE ];then
echo INDEX_TBSPACE not set!
exit
fi


PARTITIONKEY=""
db2 "select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname from syscat.columns \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' and partkeyseq >= 1 order by partkeyseq"|\
grep xxxxx|awk '{print $2}'|awk 'BEGIN{a=""}{for (i = 1; i <= NF; i++) a=a$1}END {print a}'|read PARTITIONKEY


if [ -z $PARTITIONKEY ];then
echo PARTITIONKEY not set!
exit
fi

sql="create table ${tablename} like ${TABLE_NAME_TEMPLET} in ${TBSPACE} index in ${INDEX_TBSPACE} partitioning key ( ${PARTITIONKEY}
 ) using hashing not logged initially "
echo $sql
#db2 $sql

