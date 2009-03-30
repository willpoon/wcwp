. $PZW_INC/ora10g.cfg.inc
echo $1
sqlplus -s $user/$pass <<EOF
set echo off;
set feedback off;
set heading off;
alter session force parallel query parallel 16;
alter session force parallel dml parallel 16;
$1
quit;
EOF
