#!/bin/sh
#*******************************************************
#函数名：ftp_fetion.sh
#功  能：从主机10.233.26.55获取飞信接口数据
#参  数：
#      
#      
#输  出: 
#注意事项：
#*******************************************************


##求取昨天的日期
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
        
        echo $year$month
        return 1
}

thismonth=`date '+%Y%m'` 
yyyymm=`lastmonth $thismonth`
#yyyymm=200912

##ftp 连接配置
host_ip=10.233.26.55
user_id=xzgwjk
password=xzgwjk

##ftp目录配置
remote_dir=/data1/interface/fetion/data/$yyyymm
local_dir=/bassdb1/etl/L/fetion

rsh 10.233.26.55 -l xzgwjk /data1/interface/fetion/ftp_fetion.sh
cd $local_dir

echo "开始下载飞信数据文件,请稍侯..."
while read sfilename
do

ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $sfilename
bye
end
done < ./ftp.lst

#filename=M_FETION*_${yyyymm}00*
#
#ls -l $filename | awk '{print $9}' > ./file.lst
#while read sfilename
#do
#ftp -n $host_ip <<end
#prompt
#user $user_id $password
#cd $remote_dir	
#mdelete $sfilename
#bye
#end
#done < ./file.lst
#rm file.lst



echo "飞信接口单元的数据文件下载完毕!"





exit 0
