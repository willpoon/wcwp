
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
sql="INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) \
values('${task_id}',${TABLE_NAME_TEMPLET},${TASK_NAME},${LOAD_METHOD},${BOSS_TABLE_NAME})"
echo ${sql}
}

etl_load_table_map $INTERFACE_FILE_NAME $BASS_ODS_TABLE_NAME $INTERFACE_CN_NAME 0 $(value $BOSS_TABLE_NAME)

