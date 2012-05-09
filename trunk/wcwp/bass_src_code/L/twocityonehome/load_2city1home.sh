#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

EXEC_PATH=/bassdb1/etl/L/twocityonehome
WORK_PATH=/bassdb1/etl/L/twocityonehome
BACKUP_PATH=${WORK_PATH}/data/backup
ERROR_PATH=${WORK_PATH}/error
TOLOAD_PATH=${WORK_PATH}/toload

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

start_interface_code=99901
end_interface_code=99903

#接口定长信息
INTERFACE_LEN_99901="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"
INTERFACE_LEN_99902="1 18,19 33,34 37,38 40,41 43,44 57,58 71,72 85,86 87,88 103,104 104"


#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM
    db2 commit
}

#子字符串替换函数
ReplaceAllSubStr()
{
     echo $1 > sed$$.temp
     sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
     rm sed$$.temp
     echo $sReturnString
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

#取得加载进程个数,并删除停止文件标志
if_run=`ps -ef|grep load_2city1home.sh |wc -l`
if [ -f ${EXEC_PATH}/load_2city1home_stop ] ; then
   rm ${EXEC_PATH}/load_2city1home_stop
fi

#参数判断
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "load_2city1home已经在运行,不能复复执行!"
            exit
        fi
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "stop" > ${EXEC_PATH}/load_2city1home_stop
            echo "已生成文件停止标志,load_2city1home程序执行完当前任务后即退出,请等待!"
            exit
        else
            echo "没有load_2city1home在运行!"
            exit
        fi
   else
        echo "错误的运行方式,正确的执行方式为:load_2city1home [start|stop]"
   fi
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 2 ] ; then
            echo "load_2city1home已经在运行,不能复复执行!"
            exit
   fi
else
   echo "错误的运行方式,正确的执行方式为:load_2city1home [start|stop]"
   exit
fi

#主SHELL

        #若停止标志文件存在，则程序退出
        if [ -f ${EXEC_PATH}/load_2city1home_stop ]; then
            rm ${EXEC_PATH}/load_2city1home_stop
            break
        fi

        #取得的昨天,上月的日期
        today=`date '+%Y%m%d'`
        #sday1=`yesterday $today`
        #sday2=`yesterday $sday1`
        #sday=`yesterday $sday2`
        sday=20091111
        echo sday=$sday
        smonth=`lastmonth $today`
        echo smonth=$smonth
        echo `date`

        #取接口文件
        
#        2009年11月重新启动两城一家，boss不再上发文件到集团，接口这边直接从boss取文件
#        主机：10.233.30.50
#        ftp口令：opencrm/opencrm
#        文件目录：/data1/opencrm/zhout/twocityonehome/cfile
#        日增量文件格式：CYYYYMMDDNNN.ZZZ.TTT  
#        月全量文件格式：CYYYYMM999.ZZZ.TTT

       ./ftp_interface.sh  10.233.30.50 opencrm opencrm ${WORK_PATH}/data  /data1/opencrm/zhout/twocityonehome/cfile C${sday}???.891.???
       ./ftp_interface.sh  10.233.30.50 opencrm opencrm ${WORK_PATH}/data  /data1/opencrm/zhout/twocityonehome/cfile C${smonth}???.891.???
        
#       ./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${sday}???.???.891
#       ./ftp_interface.sh  135.64.22.29 mcb3tran mcB3!891 ${WORK_PATH}/data  /opt/mcb/pcs/data/incoming C${smonth}???.???.891

        if [ -f ${WORK_PATH}/data/C${sday}???.891.??? -o ${WORK_PATH}/data/C${smonth}???.891.??? ] ; then
              cd   $WORK_PATH/data
              cp   C${sday}???.891.??? ${WORK_PATH}/backup
              cp   C${smonth}???.891.??? ${WORK_PATH}/backup
              echo "文件存在!"

         ## 以下进行剔除文件头记录和尾记录 
         echo "current_path=`pwd`"
         mark1=1
         mark2=3
         mark=$mark1
         echo $mark
       while [ ${mark} -lt ${mark2} ]
          do 
             if [  "${mark}" = "${mark1}"  ] ; then          
                 ls -l C${sday}???.891.??? |awk '{print $9}' > file.list
                 echo `ls -l C${sday}???.891.??? |awk '{print $9}'`
              
               else 
                 ls -l C${smonth}???.891.??? |awk '{print $9}' > file.list
                 echo `ls -l C${smonth}???.891.??? |awk '{print $9}'`
             fi 
                          
             while read sfilename
	            do
	              echo "${sfilename}文件剔除头记录和尾记录开始!"
	              sfilename=`echo ${sfilename}`
	              rec_cnt=`wc -l $sfilename|awk '{print $1}'`
	              rec_cnt1=`expr $rec_cnt - 1`
	              rec_cnt2=`expr $rec_cnt1 - 1`

	              echo $rec_cnt
	              echo $rec_cnt1
	              echo $rec_cnt2
	              #awk '{if(NR < `aaa=expr $rec_cnt`) print NR}' $sfilename >$sfilename.AVL

	              #echo "awk '{if(NR < $rec_cnt) print NR}' $sfilename >$sfilename.AVL.2"
	              head -$rec_cnt1 $sfilename > $sfilename.tmp
	              tail -$rec_cnt2 $sfilename.tmp > $sfilename.AVK
	              #rm $filename

	            done < ./file.list
	        
	         rm ./*.tmp
	         rm ./file.list
	         mark=`expr $mark + 1`
	         echo mark=$mark
          
        done  
	        	       	       
	        echo "剔除文件头记录和尾记录结束!"

        else
              echo "文件不存在!"
        fi

         #数据文件合并并判断合并是否成功
              day_datafile=A99901${sday}000000.AVL
              day_chkfile=A99901${sday}000000.CHK
              month_datafile=M99902${smonth}000000.AVL 
              month_chkfile=M99902${smonth}000000.CHK
              
              echo day_datafile=$day_datafile
              echo day_chkfile=$day_chkfile
              echo month_datafile=$month_datafile
              echo month_chkfile=$month_chkfile 
             
             #合并日数据文件
              cat C${sday}???.891.???.AVK > ${day_datafile}              
              
              if [ $? -eq 0 ]
              then
               rm ./C${sday}???.891.???.AVK
               rm ./C${sday}???.891.???
    	         echo "合并文件成功!"

    	       #文件合并成功，生成校验文件
    	         day_datafile_byte=`ls -l $day_datafile|awk '{print $5}'`
    	         day_datafile_count=`wc -l $day_datafile|awk '{print $1}'`
    	         day_datafile_date=`date +%Y%m%d%H%M%S`
    	             	         
    	         echo ${day_datafile}\$${day_datafile_byte}\$${day_datafile_count}\$${sday}\$${day_datafile_date} > ./$day_chkfile
    	         
    	         else
                 rm ${day_datafile}
    	           echo "合并文件失败!"
    	           continue
              fi
    	         
    	         #合并月数据文件
    	         cat C${smonth}???.891.???.AVK > ${month_datafile}
              
               if [ $? -eq 0 ]
               then
                 rm ./C${sday}???.891.???.AVK
                 rm ./C${sday}???.891.???
    	           echo "合并文件成功!"
    	         
    	         month_datafile_byte=`ls -l $month_datafile|awk '{print $5}'`
    	         month_datafile_count=`wc -l $month_datafile|awk '{print $1}'`
    	         month_datafile_date=`date +%Y%m%d%H%M%S`
    	         echo ${month_datafile}\$${month_datafile_byte}\$${month_datafile_count}\$${smonth}\$${month_datafile_date} > ./$month_chkfile

              else
                 rm ${month_datafile}
    	           echo "合并文件失败!"
    	           continue
              fi

        #取得文件,将数据文件和校验文件移动到数据加载目录

        current_interface_code=${start_interface_code}
        while [ ${current_interface_code} -lt ${end_interface_code} ]
        do
                    mv A${current_interface_code}${sday}*.CHK   ${WORK_PATH}
                    mv A${current_interface_code}${sday}*.AVL   ${WORK_PATH}

                    mv M${current_interface_code}${smonth}}*.CHK   ${WORK_PATH}
                    mv M${current_interface_code}${smonth}}*.AVL   ${WORK_PATH}
                    
                    echo "文件移动至加载目录成功！"
             current_interface_code=`expr $current_interface_code + 1`
             echo  current_interface_code=$current_interface_code
        done

         #主流程
         cd $WORK_PATH
         ls -l *.AVL |awk '{print $9}' > ./file.lst
         echo `ls -l *.AVL |awk '{print $9}'`

         while read sfilename
         do

               if [ "${sfilename}" = "" ] ; then
               	    break
               fi

               #若停止标志文件存在，则程序退出
               if [ -f ${EXEC_PATH}/load_dsmp_stop ]; then
                   rm ${EXEC_PATH}/load_dsmp_stop
                   break
               fi

               #解析文件名,生成接口单元名,月份,粒度标志,调度代码等
               datafilename=${sfilename}
               pure_filename=`echo "$sfilename"|cut -c1-20`
		           chkfilename=`echo "$pure_filename".CHK`
               task_id=`echo ${sfilename}|cut -c1-6`
               month_id=`echo ${sfilename}|cut -c7-12`
               time_id=`echo ${sfilename}|cut -c7-14`
               flag=`echo ${sfilename}|cut -c1-1`
               interface_code=`echo ${sfilename}|cut -c2-6`
               control_code="TR1_L_${interface_code}"
               if [ "$flag" = "M" ]; then
               	  scycleid=`echo $month_id`
               else
                  scycleid=`echo $time_id`
               fi

               #获取倒入模板表名、接口名称、入库方式
               DB2_SQLCOMM="db2 \"select 'xxxxxx',table_name_templet from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
               table_name_templet=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
               DB2_SQLCOMM="db2 \"select 'xxxxxx',task_name from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
               task_name=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
               DB2_SQLCOMM="db2 \"select 'xxxxxx',load_method from bass2.ETL_LOAD_TABLE_MAP where task_id='$task_id'\""
               load_method=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`

               #通过模板表名生成对应的表名
               if [ "$table_name_templet" = "" ]; then
                     mv ${datafilename} ${TOLOAD_PATH}
               	     mv ${chkfilename}  ${TOLOAD_PATH}
         	           ALARM_CONTENT="接口${task_id}没有配置入库信息!"
                     echo ${ALARM_CONTENT}
         	     continue
               else
         	     table_name=`ReplaceAllSubStr $table_name_templet 'YYYYMMDD' $time_id`
         	     table_name=`ReplaceAllSubStr $table_name 'YYYYMM' $month_id`
               fi

               #判断校验文件是否存在
               if [  ! -f ${chkfilename} ] ; then
                   mv ${datafilename}  ${ERROR_PATH}
                   echo "${datafilename}的校验文件不存在!"
                   continue
               fi

               #取得文件合并后的记录行数及文件大小
               avl_size=`ls -l ${datafilename} |awk '{print $5}'`
               avl_cnt=`wc -l ${datafilename}|awk '{print $1}`

               #取得此文件的校验文件的记录数和大小
               chk_size=`grep AVL ${chkfilename} |awk -F$ '{print $2}'`
               chk_cnt=`grep AVL ${chkfilename} |awk -F$ '{print $3}'`


               #added by lizhanyong 2008-10-16	 数据质量管理

 	              file_time=`ls -trl $datafilename |awk '{print $8}`
 	              /bassdb1/etl/L/interface_control.sh $task_id $file_time $avl_cnt $avl_size
 	              cd $WORK_PATH
 	             #added end

               #加载文件${sfilename},并更新日志为开始
               DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""
               DB2_SQL_EXEC > /dev/null

               #判断文件记录和大小是否与校验文件中的一致
               if [  "${avl_size}" = "${chk_size}" -a "${avl_cnt}" = "${chk_cnt}" ] ; then
                    echo "接口${task_id}文件大小、记录数和校验文件中的一致!"
               else
                    mv ${datafilename} ${ERROR_PATH}
               	    mv ${chkfilename}  ${ERROR_PATH}
                    ALARM_CONTENT="接口${task_id}文件大小或记录数和校验文件中的不一致!"

                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
                    echo ${ALARM_CONTENT}
         	     continue
         	    fi


              #取得接口的定长信息
              len_val=`grep "INTERFACE_LEN_${interface_code}" ${EXEC_PATH}/load_2city1home.sh |cut -d= -f2`

	            #防止重复导入,删除日期数据
             # if [ ${flag} = "P" -o ${flag} = "M" -o ${flag} = "I" -o ${flag} = "A" ] ; then
              DB2_SQLCOMM="db2 \"alter table $table_name activate not logged initially with empty table\""
              echo $DB2_SQLCOMM
	            DB2_SQL_EXEC > /dev/null
            #  else
            #        DB2_SQLCOMM="db2 \"delete from ${table_name} where bill_date='${time_id}'\""
            #        DB2_SQL_EXEC > /dev/null
            #  fi

	      #2008-11-03 配合数据质量管理，修改此部分程序，在表BASS2.ETL_TASK_RUNNING和BASS2.ETL_TASK_log中增加三个字段bass_tablename,avl_filename,chk_filename
	      #记录ETL日志
	      echo table_name=$table_name
              DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_RUNNING (task_id,cycle_id,bass_tablename,avl_filename,chk_filename,script_name,type,module,stime,status) values('$task_id','$scycleid','$table_name','$datafilename','$chkfilename','$task_name',-1,'ETL',current timestamp,'A')\""
	            DB2_SQL_EXEC > /dev/null
	      #2008-11-03 修改结束

	      echo "加载${datafilename}数据开始!"

              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\"  timeformat=\\\"HHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""
              echo $DB2_SQLCOMM
              DB2_SQL_EXEC
              echo ${DB2_SQLCOMM}

        echo "加载${datafilename}数据完成!"

              #判断加载是否成功
              #if [ "${flag}" = "P" -o "${flag}" = "M" -o "${flag}" = "I" -o "${flag}" = "A" ] ; then
                   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
                   loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              #else
              #     DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} where bill_date='${time_id}' \""
              #     loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              #fi

              if [ "${loaded_cnt}" = "${avl_cnt}" ] ; then
              	    echo "${datafilename}加载成功,记录数为:${avl_cnt},并更新日志为完成"
              	    error_cnt=`expr $avl_cnt - $loaded_cnt`
              	    mv ${datafilename} ${BACKUP_PATH}
              	    mv ${chkfilename}  ${BACKUP_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='C',etime=current timestamp,exact_sheet_cnt=$avl_cnt,load_sheet_cnt=$loaded_cnt,error_sheet_cnt=$error_cnt where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"insert into BASS2.ETL_TASK_LOG select* from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"delete from BASS2.ETL_TASK_RUNNING where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
              else
                    ALARM_CONTENT="${datafilename}加载失败,加载记录数:${loaded_cnt},文本记录数:${avl_cnt},更新日志为失败,并告警"
                    echo ${ALARM_CONTENT}
                    mv ${datafilename} ${ERROR_PATH}
              	    mv ${chkfilename}  ${ERROR_PATH}
              	    DB2_SQLCOMM="db2 \"update BASS2.ETL_TASK_RUNNING set status='F',etime=current timestamp,ERROR_MSG='$ALARM_CONTENT' where task_id='$task_id' and cycle_id='$scycleid'\""
                    DB2_SQL_EXEC > /dev/null
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_dsmp.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC  > /dev/null
             fi
             rm ${WORK_PATH}/${datafilename}
             rm ${WORK_PATH}/${chkfilename}

       done < ./file.lst

       rm ./file.lst
       
       echo "程序运行完成！" 
        
exit