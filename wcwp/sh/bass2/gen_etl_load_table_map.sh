interface_file_name=$1

interface_type=`echo ${interface_file_name} |awk '{print substr($1,1,1)}'`
echo $interface_type

interface_code=`echo ${interface_file_name} |awk '{print substr($1,2,5)}'`
echo $interface_code

interface_datetime=`echo ${interface_file_name} |awk '{print substr($1,7,8)}'`
echo $interface_datetime


task_id=${interface_type}${interface_code}
echo ${task_id}

TABLE_NAME_TEMPLET=$2 
TASK_NAME=$3
LOAD_METHOD=$4
BOSS_TABLE_NAME=$5
sql="INSERT INTO etl_load_table_map SELECT ${task_id},${TABLE_NAME_TEMPLET},${TASK_NAME},${LOAD_METHOD},${BOSS_TABLE_NAME} FROM dual"
echo ${sql}

