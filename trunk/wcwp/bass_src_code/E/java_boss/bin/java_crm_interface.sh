#!/bin/ksh

#*******************************************************
#函数名：java_crm_interface.sh
#功  能：
#参  数：
#      
#      
#输  出: 
#注意事项：
#*******************************************************
#求取昨天的日期
yesterday()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=`echo "$1"|cut -c7-8`

        month=`expr $month + 0`
        day=`expr $day - 1`

        if [ $day -eq 0 ]; then
                month=`expr $month - 1`
                if [ $month -eq 0 ]; then
                        month=12
                        day=31
                        year=`expr $year - 1`
                else
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
                fi
        fi

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

today=`date '+%Y%m%d'`
sday=`yesterday $today`

echo $sday

LANG=zh_CN.GBK;         export NLS_LANG
LC_ALL=zh_CN.GBK; export LC_ALL
cd /bassdb1/etl/E/boss/java/crm_interface/bin
/usr/jdk/jdk1.5.0_19/bin/java ETLMain $sday > /bassdb1/etl/E/boss/java/crm_interface/bin/log/$sday.log
