#!/bin/sh

#此脚本由crontab调用

#配置信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

EXEC_PATH=/bassdb1/etl/L/js
WORK_PATH=/bassdb1/etl/L/js
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
OBJ_PATH=/bassdb1/etl/L/boss

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}


#求取上月的日期
lastmonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        month=`expr $month - 1`
        if [ $month -eq 0 ]; then
             month=12
             year=`expr $year - 1`
        fi
        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        echo $year$month
        return 1
}

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

echo "生成结算数据开始!"

today=`date '+%Y%m%d'`
sday=`yesterday $today`
####sday=20120109
if [ $# -eq 1 ] ; then
   sday=$1
fi

cd $WORK_PATH

#./ftp_interface.sh  10.233.23.60 load load /bassdb1/etl/L/js  /jiesuan/mthuss/south MTHUS${sday}891.???

### 2010-06-29 南方基地BOSS系统割接，修改文件获取方式
## 2010-9-13  liuzhilong修改 因为boss有时只有压缩文件 改成将.gz文件一并取过来 再解压

./ftp_interface.sh  172.16.6.28 biftp biftp /bassdb1/etl/L/js  / MTHUS${sday}891.???
./ftp_interface.sh  172.16.6.28 biftp biftp /bassdb1/etl/L/js  / MTHUS${sday}891.???.gz
gzip -fd *gz

####注意：现在文件存放主机为 172.16.6.28 ，目录为 /data2/backup/center/back/format/sj/sp/month/  ，BOSS分配给经分的账号访问权限做了限制，biftp用户为只读，不能跳转到其它目录，不能登录主机。
####      用biftp用户ftp到172.16.6.28 ，默认是在/data2/backup/center/back/format/sj/sp/month/ 目录下的
####      目录下文件有两种 :   MTHI20100609891.000.gz,MTHUS20100412891.001    也就是压缩或没压缩的
###################################################################################################################################

cd $WORK_PATH

date_file_name="A01091${sday}000000.AVL" 
chk_file_name="A01091${sday}000000.CHK" 

if [ -f ${date_file_name} ] ; then
   rm ${date_file_name}
fi

if [ -f ${chk_file_name} ] ; then
   rm ${chk_file_name}
fi

if [ -f MTHUS${sday}891.000 ] ; then
       cat MTHUS${sday}891.??? > MTHUS${sday}891.temp
       sed 's/ /|/g' MTHUS${sday}891.temp > MTHUS${sday}891.temp_1
       rm MTHUS${sday}891.temp
fi 

while read sline
do
      mid_record_flag=`echo ${sline}|cut -c1-2` 
      busi_type=`echo ${sline}|cut -c3-12`   
      product_no=`echo ${sline}|cut -c13-27`  
      third_product_no=`echo ${sline}|cut -c28-42`  
      sp_code=`echo ${sline}|cut -c43-62`  
      busi_code=`echo ${sline}|cut -c63-82`  
      bill_type=`echo ${sline}|cut -c83-84`  
      mem_attribute=`echo ${sline}|cut -c85-85`  
      buy_channel=`echo ${sline}|cut -c86-87`  
      js_month=`echo ${sline}|cut -c88-93`  
      last_buy_time=`echo ${sline}|cut -c94-107` 
      standard_info_fee=`echo ${sline}|cut -c108-113`
      last_info_fee=`echo ${sline}|cut -c114-119`
      reserve=`echo ${sline}|cut -c120-139`
      if [ "${mid_record_flag}" = "20" ]  ; then 
         echo "${mid_record_flag}\$${busi_type}\$${product_no}\$${third_product_no}\$${sp_code}\$${busi_code}\$${bill_type}\$${mem_attribute}\$${buy_channel}\$${js_month}\$${last_buy_time}\$${standard_info_fee}\$${last_info_fee}\$${reserve}" |sed 's/|//g' >> ${date_file_name}
      fi
done< ./MTHUS${sday}891.temp_1

rm MTHUS${sday}891.temp_1

if [ -f MTHUS${sday}891.000 ] ; then
     if [ ! -f  ${date_file_name} ] ; then
             touch ${date_file_name}
     fi
fi 

if [ -f ${date_file_name} ]  ; then

     #压缩
     compress -f ${date_file_name}
     
     #取得大小,记录行数
     file_size=`ls -l ${date_file_name}.Z|awk '{print $5}'`
     rec_cnt=`zcat ${date_file_name}.Z|wc -l|awk '{print $1}'`
           
     
     #文件生成时间
     file_create_time=`date '+%Y%m%d%H%M%S'`
     echo "${date_file_name}.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
     
     rm MTHUS${sday}891.temp_1
     
     mv MTHUS${sday}891.???  ${BACKUP_PATH}
     
     mv ${date_file_name}.Z  ${OBJ_PATH}
     mv ${chk_file_name}     ${OBJ_PATH}

fi     

echo "生成数据结束!"
`
 
