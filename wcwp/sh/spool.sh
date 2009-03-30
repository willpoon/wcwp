if [ $# -lt 2 ] 
then
echo "usage: spool <output_condiction> <out_filenam>"
exit 1
else 
echo set colsep '|' >.tmp.sql
echo set pagesize 0 >>.tmp.sql
echo set heading off >>.tmp.sql
echo set echo off >>.tmp.sql
echo set feedback off >>.tmp.sql
echo set termout off >>.tmp.sql
echo set trimspool on >>.tmp.sql
echo set linesize 3000 >>.tmp.sql
echo "spool ./$2">>.tmp.sql
cat .tmp.sql $1>.tmp2.sql
echo spool off>>.tmp2.sql
echo quit >>.tmp2.sql
rm .tmp.sql
sqlplus -s zhengfengmei/oracle123@gzdm @.tmp2.sql
sed -e 's/ *//g' $2>rs_$2;
rm .tmp2.sql
fi
