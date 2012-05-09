#!/bin/ksh

#*******************************************************
#函数名：java_crm_interfaceMonth.sh
#功  能：
#参  数：
#      
#      
#输  出: 
#注意事项：
#*******************************************************
#求取上月日期
lastmonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=00
        
        month=`expr $month - 1`
        
        if [ $month -eq 0 ]; then
                month=12
                year=`expr $year - 1`
          else year=$year
               month=$month                     
        fi

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        
        echo $year$month$day
        return 1
}

thismonth=`date '+%Y%m'` 
smonth=`lastmonth $thismonth`

echo $smonth

LANG=zh_CN.GBK;         export NLS_LANG
LC_ALL=zh_CN.GBK; export LC_ALL
cd /bassdb1/etl/E/boss/java/crm_interface/bin
/usr/jdk/jdk1.5.0_19/bin/java ETLMain $smonth taskListMonth.properties > /bassdb1/etl/E/boss/java/crm_interface/bin/log/$smonth.log
