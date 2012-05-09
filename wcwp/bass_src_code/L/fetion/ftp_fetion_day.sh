#!/bin/sh
#*******************************************************
#函数名：ftp_fetion_day.sh
#功  能：从外网主机221.130.46.130获取飞信接口数据
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
#yyyymm=200911


today=`date '+%Y%m%d'`
yyyymmdd=`yesterday $today`


##创建目录

if [ -d /data1/interface/data/day ]
then
   echo "/data1/interface/data/day exists!"
else
   mkdir -p /data1/interface/data/day
fi

##ftp 连接配置
host_ip=221.130.46.130
port=21
user_id=IDP_XZ
password=Bmhhhe12

##ftp目录配置
remote_dir=/
local_dir=/data1/interface/data/day



filename=A_FETIONActiveUser_${yyyymmdd}_??????.Z
chkname=A_FETIONActiveUser_${yyyymmdd}.CHK


echo "ftp目录配置如下："
echo remote_dir_B=$remote_dir
echo local_dir=$local_dir
echo FILENAME=$filename


echo "开始下载飞信数据文件,请稍侯..."
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir
cd $remote_dir
mget $filename
mget $chkname
bye
end

echo "飞信接口单元的数据文件下载完毕!"

exit 0
