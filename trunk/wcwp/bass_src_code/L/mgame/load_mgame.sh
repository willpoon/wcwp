#!/bin/sh
#*************************************************************************************************************
#函数名：load_mgame.sh
#功  能：从10.233.26.55主机mgame目录取数据文件,加载入库
#参  数：无
#
#编写人: lizhanyong
#编写时间 2010-04-30
#
#输  出: 接口加载入库
#注意事项：此shell由crontab调用,程序中调用了ftp_mgame.sh
#
#执行方式：程序每天由crontab调度执行，也可手工执行。手工执行可以跟一个参数，也可以跟两个参数，执行方式如下：
#          执行方式:load_mgame.sh yyyymmdd ,为取特定某一天的接口，如./load_mgame.sh 20100401 为取2010年4月1号的全部接口文件
#          执行方式:load_mgame.sh yyyymmdd 接口编号 ,为取特定某一天的某个接口，如./load_mgame.sh 20100401 I43001 为取 2010年4月1号接口I43001的文件
#
#其他说明：接口原文件中是以'\t'（制表符）分割的,以下部分是测试制表符的获取方式
#     	   a=a
#     	   b=b
#     	   c=c
#     	   echo $a["\t"]$b[" "]$c
#     	   替换制表符的语句，sed 's/	/$/g' ，注意此处不能用'\t',而要用一个TAB键
#     	   如db2 load 操作可以指定分割符为制表符，则不需要对文件中的制表符进行替换
#*************************************************************************************************************

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

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
echo `date` >> /bassdb1/etl/L/mgame/db2_connect.log
echo $DB2_SQLCOMM >> /bassdb1/etl/L/mgame/db2_connect.log

conn_cnt=0
while [ $conn_cnt -lt 3 ]
do
	db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
	if [ $? != 0 ] ; then
		echo "数据库连接失败！" >> /bassdb1/etl/L/mgame/db2_connect.log
		conn_cnt=`expr ${conn_cnt} + 1`
		echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/mgame/db2_connect.log
	else
		echo "数据库连接成功！" >> /bassdb1/etl/L/mgame/db2_connect.log
		break
	fi
done
eval $DB2_SQLCOMM

db2 commit
db2 terminate
}

#字符串替换函数
ReplaceAllSubStr()
{
     echo $1 > sed$$.temp
     sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
     rm sed$$.temp
     echo $sReturnString
     return 1
}

#接口号与接口文件名匹配函数
MatchFileName()
{
	if [ "$1"="I43001" ] ; then
	  INTERFILE_NAME="KPI_FEEUSER"	  
  else if [ "$1"="I43002" ] ; then
  	INTERFILE_NAME="ORDER"
  else if [ "$1"="I43003" ] ; then
    INTERFILE_NAME="SERVICE"
  else if [ "$1"="I43004" ] ; then
    INTERFILE_NAME="USER"
  else if [ "$1"="I43004" ] ; then
  	INTERFILE_NAME="USER_TST"   
  fi
  fi
  fi
  fi
  fi 	 	
}

#接口号对应文件信息
   #I43001：KPI付费用户清单        
   #I43002：游戏业务套餐订购清单
   #I43003：手机游戏业务清单
   #I43004：游戏业务使用用户清单
   #I43005：公测游戏业务使用用户清单 

#配置信息

PROGRAM_NAME="load_mgame.sh"
WORK_PATH=/bassdb1/etl/L/mgame
DATA_PATH=/bassdb1/etl/L/mgame/data
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload


#数据库连接信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

#解密
DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`

#切换回工作目录
cd $WORK_PATH

echo "处理手机游戏接口数据开始 ..."
date

   
   
#手工执行，跟一个参数时，替换日期
if [ $# -eq 1 ] ; then
   sday=$1 
   yyyymm=`echo "$sday"|cut -c1-6`
   
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame KPI_FEEUSER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ORDER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame SERVICE${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER_TST${sday}891.???.dat  
   
#手工执行，跟两个参数时，替换日期和接口编码，并且仅传输接口编码对应的文件
 else if [ $# -eq 2 ] ; then 
	 sday=$1  
	 yyyymm=`echo "$sday"|cut -c1-6`    
	 INTERFACE_CODE=$2
	 	
	 	#获取接口号对应的接口文件名 
	 MatchFileName $INTERFACE_CODE
	 	 
	 echo "开始传输接口号${INTERFACE_CODE}对应的文件${INTERFILE_NAME}${sday}891.???.dat ...	"
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ${INTERFILE_NAME}${sday}891.???.dat	
   
else 
  #不跟参数时，按日期批量传输
  echo "开始批量传输接口文件 ..."   
  
   today=`date '+%Y%m%d'`
   sday=`yesterday $today`
   yyyymm=`echo "$sday"|cut -c1-6`
  
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame KPI_FEEUSER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame ORDER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame SERVICE${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER${sday}891.???.dat
  ./ftp_mgame.sh  10.233.26.55 xzgwjk xzgwjk /data1/mgame/game /bassdb1/etl/L/mgame USER_TST${sday}891.???.dat
  
fi
fi

##########处理KPI_FEEUSER${sday}891.???.dat文件，生成AVL和CHK文件

  
     if [ -f KPI_FEEUSER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43001${sday}000000.AVL
     	     chk_file_name=I43001${sday}000000.CHK
     	     data_file_tmp=I43001${sday}000000.tmp
       	                 
           #因加载时用$做分隔符，所以先将文件中的$剔除掉，并将制表符替换为$符
            sed 's/\$//g' KPI_FEEUSER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' KPI_FEEUSER${sday}891.???.dat > $data_file_name
            #cp KPI_FEEUSER${sday}891.???.dat  $data_file_name
            mv KPI_FEEUSER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #取得大小,记录行数
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #文件生成时间
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #生成校验文件
            echo "I43001${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
###########处理ORDER${sday}891.???.dat文件，生成AVL和CHK文件
  
     if [ -f ORDER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43002${sday}000000.AVL
     	     chk_file_name=I43002${sday}000000.CHK
     	     data_file_tmp=I43002${sday}000000.tmp
  
           #因加载时用$做分隔符，所以先将文件中的$剔除掉，并将制表符替换为$符
            sed 's/\$//g' ORDER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp ORDER${sday}891.???.dat  $data_file_name
            mv ORDER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #取得大小,记录行数
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #文件生成时间
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #生成校验文件
            echo "I43002${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
###########处理SERVICE${sday}891.???.dat文件，生成AVL和CHK文件
  
     if [ -f SERVICE${sday}891.???.dat ] ; then
  
     	     data_file_name=I43003${sday}000000.AVL
     	     chk_file_name=I43003${sday}000000.CHK
           data_file_tmp=I43003${sday}000000.tmp
            
            #因加载时用$做分隔符，所以先将文件中的$剔除掉，并将制表符替换为$符
     	      sed 's/\$//g' SERVICE${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp SERVICE${sday}891.???.dat  $data_file_name
            mv SERVICE${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #取得大小,记录行数
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #文件生成时间
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #生成校验文件
            echo "I43003${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
##########处理USER${sday}891.???.dat文件，生成AVL和CHK文件
  
     if [ -f USER${sday}891.???.dat ] ; then
  
     	     data_file_name=I43004${sday}000000.AVL
     	     chk_file_name=I43004${sday}000000.CHK
     	     data_file_tmp=I43004${sday}000000.tmp
  
            #因加载时用$做分隔符，所以先将文件中的$剔除掉，并将制表符替换为$符
     	      sed 's/\$//g' USER${sday}891.???.dat > $data_file_tmp
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            cp $data_file_name $data_file_tmp
            sed 's///g' $data_file_tmp > $data_file_name
            #cp USER${sday}891.???.dat  $data_file_name
            mv USER${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #取得大小,记录行数
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #文件生成时间
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #生成校验文件
            echo "I43004${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
###########处理USER_TST${sday}891.???.dat文件，生成AVL和CHK文件
  
     if [ -f USER_TST${sday}891.???.dat ] ; then
  
     	     data_file_name=I43005${sday}000000.AVL
     	     chk_file_name=I43005${sday}000000.CHK  
     	     data_file_tmp=I43005${sday}000000.tmp
            
            #因加载时用$做分隔符，所以先将文件中的$剔除掉，并将制表符替换为$符
            sed 's/\$//g' USER_TST${sday}891.???.dat > $data_file_tmp 
            sed 's/	/$/g' $data_file_tmp > $data_file_name
            #cp USER_TST${sday}891.???.dat  $data_file_name
            mv USER_TST${sday}891.???.dat  ${BACKUP_PATH}
            rm ${data_file_tmp}
  
            #取得大小,记录行数
            file_size=`wc -c $data_file_name|awk '{print $1}'`
            avl_cnt=`wc -l $data_file_name|awk '{print $1}'`
  
            #文件生成时间
            file_create_time=`date '+%Y%m%d%H%M%S'`
  
            #生成校验文件
            echo "I43005${sday}000000.AVL\$${file_size}\$${avl_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
  
     fi
  
################################以下部分开始进行数据加载######################################################
  
   ls -l *.AVL |awk '{print $9}' > ./mgame_loadfile.lst
   echo `ls -l *.AVL |awk '{print $9}'`
  
   while read sfilename
     do
  
    #1、如果没有需要加载的文件，直接退出
     if [ "${sfilename}" = "" ] ; then
         break
     fi
  
    echo "===============================加载接口文件${sfilename}开始============================================"
  
    #2、获取接口代码和周期、调度名称
  
       #获取接口号、数据文件名、校验文件名
  	  task_id=`echo "$sfilename"|cut -c1-6`
  	  avl_filename=${sfilename}
  	  pure_filename=`echo "$sfilename"|cut -c1-20`
  	  chk_filename=`echo "$pure_filename".CHK`
  
  	  #接口加载调度名称
  	  control_code=`echo $task_id|cut -c1-6`
  	  control_code=`echo TR1_L_$control_code`
  
  	  #接口加载文件上的日月信息
  	   month_id=`echo ${sfilename}|cut -c7-12`
       day_id=`echo ${sfilename}|cut -c7-14`
  
      #获取接口文件的数据周期
       sfrequency=`echo "$sfilename"|cut -c1-1`
       if [ $sfrequency = "M" ]; then
  	    	scycleid=`echo $month_id`
  	    else
  	      scycleid=`echo $day_id`
  	   fi
  
  	 echo task_id=$task_id
  	 echo avl_filename=$avl_filename
  	 echo chk_filename=$chk_filename
  	 echo scycleid=$scycleid
  	 echo control_code=$control_code
  
  
  	#3、判断对应的校验文件是否存在,若不存在则跳出本次循环，进行下一接口处理
     if [ ! -f ${WORK_PATH}/${chk_filename} ]  ; then
       echo "接口加载告警信息：数据文件${sfilename}对应的校验文件不存在!"
  	   sleep 30
       continue
  
  	 else
      #统计数据文件记录数和文件大小，记录校验文件中记录数
  
  		chk_cnt=`grep AVL $chk_filename |awk -F$ '{print $3}'`
  		avl_cnt=`wc -l $avl_filename |awk '{print $1}'`
  		avl_byte=`ls -trl $avl_filename |awk '{print $5}'`
  		file_time=`ls -trl $avl_filename |awk '{print $8}'`
  
  		echo chk_cnt=$chk_cnt
  		echo avl_cnt=$avl_cnt
  
     fi
  
    #4、判断接口文件记录数与校验文件记录是否一致
  		if [ $chk_cnt -ne $avl_cnt ]; then
  			ALARM_CONTENT=`echo "接口加载告警信息：接口名称为${task_name}，接口文件${avl_filename}和${chk_filename}文件记录行数不符！"`  			
  		  #插入调度告警表
  			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  			DB2_SQL_EXEC
  			echo ALARM_CONTENT=$ALARM_CONTENT				
  
  
  		  #将文件移到ERROR_PATH
  			mv $sfilename $ERROR_PATH
  		  mv $chk_filename $ERROR_PATH
  			continue
  		fi
  
  
    #5、获取导入模板表名、接口名称、入库方式
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
  		load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo table_name_templet=$table_name_templet
      echo task_name=$task_name
      echo load_method=$load_method
  
  
  	#6、判断接口导入模板表是否配置
  	 if [ "$table_name_templet" = "" ]; then
  		#若没配置导入模板表，则直接告警
  		ALARM_CONTENT=`echo "接口加载告警信息：接口$task_id没有配置入库信息到表ETL_LOAD_TABLE_MAP！"`  		
  		DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  	  DB2_SQL_EXEC
  	  echo ALARM_CONTENT=$ALARM_CONTENT		  
  
  		#接口文件移至toload目录，进行下一接口处理
  		mv $avl_filename $TOLOAD_PATH
  		mv $chk_filename $TOLOAD_PATH
  		continue
  
    else
     #处理得到接口表名称
  	   tablename=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $day_id`
  	   tablename=`ReplaceAllSubStr $tablename 'YYYYMM' $month_id`
  
       echo tablename=$tablename
       echo avl_filename=$avl_filename
       echo chk_filename=$chk_filename
  
     fi
  
    #7、判断接口加载目标表是否存在
      DB2_SQLCOMM="db2 \"select 'xxxxxx',tabname from syscat.tables where tabname=upper('$tablename')\""
  		table_name_target=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
  		echo table_name_target=$table_name_target
  		if [ "${table_name_target}" = "" ]; then
  			ALARM_CONTENT=`echo "接口加载告警信息：接口$task_id加载目标表$tablename不存在！"`        
  			DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
  			DB2_SQL_EXEC
  			echo ALARM_CONTENT=$ALARM_CONTENT
        
  
  			#接口文件移至toload目录，进行下一接口处理
  			mv $avl_filename $TOLOAD_PATH
  			mv $chk_filename $TOLOAD_PATH
  			continue
  	  fi
  
  
  
   #8、导入接口数据任务开始
  
  	 #8.1、设置加载任务
  	  #删除已有任务
  	  echo "删除已有任务 ..."
  		DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	  #增加新的任务
  	  echo "增加新的任务 ..."
  		DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$tablename','$avl_filename','$chk_filename','$task_name',-1,'ETL',current timestamp,'A')\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	 #8.2、更新调度日志表，flag=1表示正在执行
  		echo "更新调度日志表运行时间，状态信息 ..."
  		DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
  		DB2_SQL_EXEC > /dev/null
  
  
  	 #8.3、导入数据:load_method="0"就用LOAD，load_method=1就用import
  			if [ $load_method = "0" ]; then
  				echo "LOADING ..."
  				DB2_SQLCOMM="db2 \"load client from $WORK_PATH/$avl_filename of del modified by coldel$ fastparse anyorder warningcount 1000 messages /dev/null replace into $tablename\""
  				echo ${DB2_SQLCOMM}
  				DB2_SQL_EXEC
  
  			else
  				echo "IMPORT ..."
  				DB2_SQLCOMM="db2 \"import from $WORK_PATH/$load_avl_filename of del modified by coldel$ replace into $tablename\""
  				echo ${DB2_SQLCOMM}
  				DB2_SQL_EXEC
  
  			fi


		 #8.4、读取导入记录条数信息
		  echo "GET LOAD RECORD ..."
			DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from bass2.$tablename\""
			loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
			echo loaded_cnt=$loaded_cnt

		 #8.5、判断导入是否正确，记录相应的日志(在此需要用户字符串比较方式,否则查询失败用数字比较将出错)
			if [ "${avl_cnt}" = "${loaded_cnt}" ] ; then
			  #8.5.1、导入完全正确
			  
			  error_cnt=`expr $avl_cnt - $loaded_cnt`
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$chk_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG(task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt) select task_id,cycle_id,bass_tablename,avl_filename,chk_filename,step,parent_script_id,script_id,script_name,type,module,run_param,script_type,run_script1,run_script2,run_script3,plan_time,stime,etime,status,error_msg,priority,flag,spid,exact_sheet_cnt,load_sheet_cnt,error_sheet_cnt from BASS2.ETL_TASK_RUNNING a where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0  where CONTROL_CODE='${control_code}'\""
				DB2_SQL_EXEC > /dev/null 	  				
				echo "接口加载校验：数据加载完全正确！"
				
				#数据加载完全正确，调用数据质量管理处理程序
	       /bassdb1/etl/L/interface_control.sh $task_id $file_time $avl_cnt $avl_byte
	   
	      #数据加载完全正确，备份接口文件至data目录
	       cd $WORK_PATH
			   mv $avl_filename $DATA_PATH
			   mv $chk_filename $DATA_PATH
									      			
      else
      if [ "${loaded_cnt}" = "" ] ; then
        #8.5.2、导入出错(从表中查询记录数失败,一般是表异常,因此可以确定导入也失败了)
        ALARM_CONTENT=`echo 接口加载校验："文件${sfilename},查询导入记录数失败！"`       	
				DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$chk_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
				DB2_SQL_EXEC > /dev/null
				DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
				DB2_SQL_EXEC > /dev/null				
				mv $avl_filename $ERROR_PATH
			  mv $chk_filename $ERROR_PATH
			  echo ALARM_CONTENT=$ALARM_CONTENT
			  		  
      else
        error_cnt=`expr $avl_cnt - $loaded_cnt`
        if [ ${error_cnt} -ne 0 ]; then
			    #8.5.3、导入出错
			    ALARM_CONTENT=`echo "接口加载校验：接口文件$sfilename导入记录失败,失败记录数为${error_cnt}！"`			    	
			    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='${ALARM_CONTENT}',exact_sheet_cnt=$chk_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
			    DB2_SQL_EXEC > /dev/null
			    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1  where CONTROL_CODE='${control_code}'\""
			    DB2_SQL_EXEC > /dev/null
			    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','${PROGRAM_NAME}',1,'${ALARM_CONTENT}',current timestamp,-1)\""
			    DB2_SQL_EXEC > /dev/null
			    mv $avl_filename $ERROR_PATH
			    mv $chk_filename $ERROR_PATH
			    echo ALARM_CONTENT=$ALARM_CONTENT			    	    
		    fi
      fi                 			
		fi
		
		echo "处理接口文件$sfilename完毕！"
		
	done < ./mgame_loadfile.lst
	rm ./mgame_loadfile.lst
	
echo "生成数据结束!"
exit 0

