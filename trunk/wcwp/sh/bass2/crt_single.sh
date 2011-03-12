TABLE_NAME_TEMPLET=$1
TBLSPACE_NAME=$2
PARTITIONING_KEK=$3
TIME_ID=$4
#day=${YYYYMM}`getday $YYYYMM`
#fday=$YYYYMM'01'


if [ `echo ${TIME_ID} |awk '{print length($1)}'` = 6 ] ; then 
TABLE_TEMPLET=${TABLE_NAME_TEMPLET}_YYYYMM
else
TABLE_TEMPLET=${TABLE_NAME_TEMPLET}_YYYYMMDD
fi


db2 connect to bassdb user bass2  using bass2;

sql="create table ${TABLE_NAME_TEMPLET}_${TIME_ID} like $TABLE_TEMPLET in ${TBLSPACE_NAME} index in tbs_index partitioning key ( ${PARTITIONING_KEK} ) using hashing not logged initially "
echo $sql
db2 $sql

db2 terminate;

