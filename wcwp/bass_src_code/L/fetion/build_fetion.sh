#!/bin/ksh
#*************************************************************************************************************
#函数名：build_fetion.sh
#功  能：从10.233.26.55主机取数据文件，合并文件，替换分隔符，生成适合load_boss.sh加载的文件形式
#参  数：无
#      
#编写人: 
#编写时间 2009-08-21
#    
#输  出: 符合load_boss.sh加载的文件
#注意事项：
#*************************************************************************************************************


#配置信息

EXEC_PATH=/bassdb1/etl/L/fetion
WORK_PATH=/bassdb1/etl/L/fetion
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
OBJ_PATH=/bassdb1/etl/L/boss

#求取上月的日期
lastmonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        month=`expr $month - 1`
        day=00
        if [ $month -eq 0 ]; then
             month=12
             year=`expr $year - 1`
        fi
        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        echo $year$month$day
        return 1
}


#求取本月的日期
thismonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=01
        echo $year$month$day
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

echo "处理数据开始!"

today=`date '+%Y%m%d'`
sday=`lastmonth $today`
thisday=`thismonth $today`
#sday=20090719
yyyymm=`echo "$sday"|cut -c1-6`
lastmonth=`lastmonth $today`
lastmonth=`lastmonth $today`

if [ $# -eq 1 ] ; then
   sday=$1
fi


#切换回工作目录 
cd $WORK_PATH

#########合并文件,替换分隔符'&&'为'$',生成AVL和CHK文件

#M_FETIONMonthlyActivityUser_20090700_000001.txt

   if [ -f M_FETIONMonthlyActivityUser_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40008${lastmonth}000000.AVL
   	     chk_file_name=M40008${lastmonth}000000.CHK
   	     
          cp M_FETIONMonthlyActivityUser_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONMonthlyActivityUser_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40008${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
          echo "校验文件生成"
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONMonthlyActivityUser_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONMonthlyActivityUser_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi 
   
   
   if [ -f M_FETIONCtnHushUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40009${lastmonth}000000.AVL
   	     chk_file_name=M40009${lastmonth}000000.CHK
   	     
          cp M_FETIONCtnHushUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONCtnHushUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40009${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONCtnHushUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONCtnHushUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   if [ -f M_FETIONActHushUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40010${lastmonth}000000.AVL
   	     chk_file_name=M40010${lastmonth}000000.CHK
   	     
          cp M_FETIONActHushUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONActHushUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40010${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONActHushUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONActHushUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   
   if [ -f M_FETIONTasteUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40011${lastmonth}000000.AVL
   	     chk_file_name=M40011${lastmonth}000000.CHK
   	     
          cp M_FETIONTasteUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONTasteUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40011${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONTasteUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONTasteUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   if [ -f M_FETIONCmlUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40012${lastmonth}000000.AVL
   	     chk_file_name=M40012${lastmonth}000000.CHK
   	     
          cp M_FETIONCmlUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONCmlUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40012${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONCmlUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONCmlUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   if [ -f M_FETIONCustomUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40013${lastmonth}000000.AVL
   	     chk_file_name=M40013${lastmonth}000000.CHK
   	     
          cp M_FETIONCustomUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONCustomUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40013${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONCustomUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONCustomUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   if [ -f M_FETIONFaithUserInfo_${lastmonth}_000001.txt ] ; then
   	
   	     data_file_name=M40014${lastmonth}000000.AVL
   	     chk_file_name=M40014${lastmonth}000000.CHK
   	     
          cp M_FETIONFaithUserInfo_${lastmonth}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #压缩                        
          compress -f $data_file_name
   
        
          #取得大小,记录行数
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' M_FETIONFaithUserInfo_${lastmonth}.CHK`         
          
          #文件生成时间
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #生成校验文件       
          echo "M40014${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
	#将AVL和CHK文件移动至加载目录
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#将原始文件移到backup目录备份
	mv M_FETIONFaithUserInfo_${lastmonth}_000001.txt $BACKUP_PATH/
	mv M_FETIONFaithUserInfo_${lastmonth}.CHK $BACKUP_PATH/
                    
   fi
   
   
echo "生成数据结束!"
exit 0
 
