#purpose:a short-cut to get  table structure
. $cfgroot/inc/connect_db2.inc
db2 "describe table $1"|sed '/^$/d'|sed -e '1,3d;$d'|awk 'BEGIN { FS=" "}{ printf ",%-30s	%s\n",$1,$3"("$4")" }' 
