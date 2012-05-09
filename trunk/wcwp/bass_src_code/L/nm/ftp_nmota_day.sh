#!/bin/sh
#*******************************************************
#��������ftp_nmota_day.sh
#��  �ܣ������ؽ�nmota�ӿ������͵��Ϸ�����
#��  ����
#      
#      
#��  ��: 
#ע�����
#*******************************************************


##��ȡ���������
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

#��ȡ��������
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




##add by lizhanyong  ����ִ��ʱ�����������ڲ����Ĵ�������ȡ��ʷ����ʱ�Ͳ���ÿ�ζ������ļ��е�������
##ִ�и�ʽ�� ftp_nmota_day.sh yyyymmdd
if [ $# -eq 1 ] ; then
	today=$1
	yyyymmdd=`yesterday $today`
	yyyymmdd2=$today
else
  today=`date '+%Y%m%d'`
  yyyymmdd=`yesterday $today`
  yyyymmdd2=`yesterday $today`

fi


##ftp ��������
host_ip=10.233.23.60
port=21
user_id=load
password=load

##ftpĿ¼����

#modify by lizhanyong 2010-05-04 ,�޸Ļ�ȡ�ļ���·���������ļ�����nm_dataĿ¼�������Ϳ�����get_nm.sh�Զ�ȡ�ļ���

#remote_dir=/bassdb2/etl/L/nm/backup
#local_dir=/bassdb1/etl/L/nm



mkdir /bassdb1/etl/L/nm/nm_data/$today
mkdir /bassdb1/etl/L/nm/nm_data/$today/CHK
mkdir /bassdb1/etl/L/nm/nm_data/$today/DATA

remote_dir_C=/bassdb2/etl/L/nm/nm_data/$today/CHK
remote_dir_D=/bassdb2/etl/L/nm/nm_data/$today/DATA

local_dir_C=/bassdb1/etl/L/nm/nm_data/$today/CHK
local_dir_D=/bassdb1/etl/L/nm/nm_data/$today/DATA



filename=*${yyyymmdd}*.AVL
chkname=*${yyyymmdd}*.CHK


echo "ftpĿ¼�������£�"
echo remote_dir_C=$remote_dir_C
echo remote_dir_D=$remote_dir_D
echo local_dir_C=$local_dir_C
echo local_dir_D=$local_dir_D
echo DATA_FILENAME=$filename
echo CHK_FILENAME=$chkname


echo "��ʼ���������ļ�,���Ժ�..."
ftp -n $host_ip <<end
prompt
user $user_id $password
bin
lcd $local_dir_C
cd $remote_dir_C
mget $chkname
lcd $local_dir_D
cd $remote_dir_D
mget $filename
bye
end

echo "���ܽӿڵ�Ԫ�������ļ��������!"


##ftpĿ¼����
remote_dir=/bassdb2/etl/L/ota/backup
local_dir=/bassdb1/etl/L/ota

filename=*${yyyymmdd2}*.AVL
chkname=*${yyyymmdd2}*.CHK


echo "ftpĿ¼�������£�"
echo remote_dir_B=$remote_dir
echo local_dir=$local_dir
echo FILENAME=$filename


echo "��ʼ����OTA�ļ�,���Ժ�..."
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

echo "OTA�ӿڵ�Ԫ�������ļ��������!"

exit 0
