echo info file:interface_info.csv
ifnull(){
if [ -z $1 ];then
echo  interface_info.csv:index$i is null!check it!
exit
fi
}

value(){
if [ -z $1 ];then
echo  null
else
echo $1
fi
}

tochar(){
if [ -z $1 ];then
echo  null
else
echo \'$1\'
fi
}

i=0
#15¸ö×Ö¶ÎÐÅÏ¢
while [ i -lt 15 ]
do
info[$i]=$(cat interface_info.csv|nawk -F"\t" -v I=`expr $i + 1` '{print $I}')
#echo $i `value ${info[$i]}`
case $i in
0|5|7|8|9|12|13 ) ifnull ${info[i]} ;;
esac
i=`expr $i + 1`
done

INTERFACE_FILE_NAME=${info[0]}
BOSS_SCHEMA=${info[1]}
BOSS_TABLE_NAME=${info[2]}
SPLIT_RULE=${info[3]}
BASS_CLASS=${info[4]}
INTERFACE_CN_NAME=${info[5]}
BASS_MODULE_ID=${info[6]}
EXTRACT_CYCLE=${info[7]}
BASS_ODS_TABLE_NAME=${info[8]}
EXTRACT_HOST=${info[9]}
EXTRACT_TIMING=${info[10]}
NOTES=${info[11]}
ASSIGN_TBS=${info[12]}
PARTITION_KEY=${info[13]}
BOSS_DB_MODULE=${info[14]}


etl_load_table_map(){
interface_file_name=$1
interface_type=`echo ${interface_file_name} |awk '{print substr($1,1,1)}'`
interface_code=`echo ${interface_file_name} |awk '{print substr($1,2,5)}'`
interface_datetime=`echo ${interface_file_name} |awk '{print substr($1,7,8)}'`
task_id=${interface_type}${interface_code}
TABLE_NAME_TEMPLET=$2
TASK_NAME=$3
LOAD_METHOD=$4
BOSS_TABLE_NAME=$5
sql="INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) \n values( \
$(tochar ${task_id}),\
$(tochar ${TABLE_NAME_TEMPLET}),\
$(tochar ${TASK_NAME}),\
		 ${LOAD_METHOD},\
$(tochar ${BOSS_TABLE_NAME})\
)"
echo "\n"
echo ${sql}
echo "\n"

}


etl_load_table_map $INTERFACE_FILE_NAME $BASS_ODS_TABLE_NAME $INTERFACE_CN_NAME 0 $BOSS_TABLE_NAME

USYS_TABLE_MAINTAIN2(){
echo TABLE_ID	TABLE_NOTE	TABLE_NAME	SPLIT_TYPE	SPLIT_DATE	TABLE_LIFECYCLE	PARTITION_TYPE	EXTEND_TABLE	EXTEND_FIELD	SCHEMA_NAME	TEMPLATE_TABLE	TBLSPACE_NAME	INDSPACE_NAME	PARTITION_KEY	NEED_CREATE
}


ddl_create(){
TABLE_NAME_TEMPLET=$1	
ASSIGN_TBS=$2
PARTITION_KEY=$3
	sql=" --DROP TABLE ${TABLE_NAME_TEMPLET};\n
		CREATE TABLE ${TABLE_NAME_TEMPLET} (\n\n
	  	 )\n
	  	 DATA CAPTURE NONE \n
 		 IN ${ASSIGN_TBS}\n
 		 INDEX IN TBS_INDEX\n
  		 PARTITIONING KEY\n
   		 ( ${PARTITION_KEY} )\n
 		 USING HASHING;\n
	"
	echo $sql
}	

ddl_create $TABLE_NAME_TEMPLET $ASSIGN_TBS $PARTITION_KEY

	