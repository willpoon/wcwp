#!/bin/sh

if [ ! $# -eq 4 ]
   then
           echo "Usage: $0 TABLE_NAME_TEMPLET TBLSPACE_NAME PARTITIONING_KEK YYYYMM"
           exit
fi



#得到某月的天数
getday()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`

        month=`expr $month + 0`

        case $month in
                1|3|5|7|8|10|12) day=31;;
                4|6|9|11) day=30;;
                2)
                        if [ `expr $year % 4` -eq 0 ]; then
                                if [ `expr $year % 400` -eq 0 ]; then
                                        day=29
                                elif [ `expr $year % 100` -eq 0 ]; then
                                        day=28
                                else
                                        day=29
                                fi
                        else
                                day=28
                        fi ;;
        esac

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $day
        return 1
}





TABLE_NAME_TEMPLET=$1
TBLSPACE_NAME=$2
PARTITIONING_KEK=$3
YYYYMM=$4
day=${YYYYMM}`getday $YYYYMM`
fday=$YYYYMM'01'


db2 connect to bassdb user bass2  using bass2;

while [ $fday -le $day ]
do

sql="create table ${TABLE_NAME_TEMPLET}_${fday} like ${TABLE_NAME_TEMPLET}_YYYYMMDD in ${TBLSPACE_NAME} index in tbs_index partitioning key ( ${PARTITIONING_KEK} ) using hashing not logged initially "
echo $sql
db2 $sql


fday=`expr $fday + 1`

done


db2 terminate;