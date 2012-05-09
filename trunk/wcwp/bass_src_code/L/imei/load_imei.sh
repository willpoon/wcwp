#!/bin/sh

#配置信息
DB2_OSS_DB="BASSDB"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
EXEC_PATH=/bassdb1/etl/L/imei
WORK_PATH=${EXEC_PATH}/work
BAK_PATH=${EXEC_PATH}/backup

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 connect reset
}

#上个月
lastmonth()
{
        today=`date '+%Y%m%d'`
        year=`echo "${today}"|cut -c1-4`
        month=`echo "${today}"|cut -c5-6`
        month=`expr $month - 1`
        if [ $month -eq 0 ]; then
             month=12
             year=`expr $year - 1`
        fi
        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        echo "$year$month"
        return 1
}

#取得加载进程个数,并删除停止文件标志
if_run=`ps -ef|grep load_imei.sh |wc -l`
if [ -f ${EXEC_PATH}/load_imei_stop ] ; then
   rm ${EXEC_PATH}/load_imei_stop       
fi

#参数判断
if [ $# -eq 1 ] ; then
   if   [  "$1" = "start"  ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "load_imei已经在运行,不能复复执行!"
            exit        
        fi      
   elif [  "$1" = "stop"   ] ; then
        if [ ${if_run} -gt 2 ] ; then
            echo "stop" > ${EXEC_PATH}/load_imei_stop
            echo "已生成文件停止标志,load_imei_stop程序执行完当前任务后即退出,请等待!"
            exit
        else    
            echo "没有load_imei在运行!"
            exit        
        fi 
   else
        echo "错误的运行方式,正确的执行方式为:load_imei [start|stop]"
   fi    
elif [ $# -eq 0 ] ; then
   if [ ${if_run} -gt 2 ] ; then
            echo "load_imei已经在运行,不能复复执行!"
            exit        
   fi           
else   
   echo "错误的运行方式,正确的执行方式为:load_imei [start|stop]"
   exit
fi

INTERFACE_CODE="91003"

#主SHELL
while [ true ]
do       
        
        #若停止标志文件存在，则程序退出
        if [ -f ${EXEC_PATH}/load_imei_stop ]; then
            rm ${EXEC_PATH}/load_imei_stop
            break
        fi
        
        #变换加载接口[91003对应91003,91004,91005,91006,91007]
        if [ "${INTERFACE_CODE}" = "91003" ] ; then
            INTERFACE_CODE="91002"            
        else
            INTERFACE_CODE="91003"              
        fi      
        
        echo "FTP数据,并判断数据文件是否存在"
        #TIME_ID=`lastmonth`
        TIME_ID=201204
        ${EXEC_PATH}/ftp_imei.sh ${TIME_ID} ${INTERFACE_CODE}
        if [ ${INTERFACE_CODE} -eq 91002 ] ; then
            if [ ! -f ${WORK_PATH}/i_30000_${TIME_ID}_${INTERFACE_CODE}_001.dat ] ; then 
                 echo "数据文件不存在[i_30000_${TIME_ID}_${INTERFACE_CODE}_001.dat]"
                 continue
            fi  
        fi      
        if [ ${INTERFACE_CODE} -eq 91003 ] ; then
            if [ ! -f ${WORK_PATH}/i_30000_${TIME_ID}_${INTERFACE_CODE}.xml ] ; then 
                 echo "数据文件不存在[i_30000_${TIME_ID}_${INTERFACE_CODE}.xml]"
                 sleep 300
                 continue
            fi  
        fi      
        
        echo "格式转换,将文件的各字段转换成以$字符分隔"
        if [ ${INTERFACE_CODE} -eq 91002 ] ; then
                if [ -f ${WORK_PATH}/BASS2.DIM_TACNUM_DEVID.del ] ; then
                     rm ${WORK_PATH}/BASS2.DIM_TACNUM_DEVID.del
                fi
                while read line_str
                do 
                    TAC_NUM=`echo "${line_str}" | cut -c9-18`
                    DEV_ID=`echo "${line_str}" | cut -c19-34`
                    echo ${TIME_ID}"$"${TAC_NUM}"$""${DEV_ID}" >> ${WORK_PATH}/BASS2.DIM_TACNUM_DEVID.del  
                done < ${WORK_PATH}/i_30000_${TIME_ID}_${INTERFACE_CODE}_001.dat        
        fi
        if [ ${INTERFACE_CODE} -eq 91003 ] ; then
                java -jar -server -Xmx512m ${EXEC_PATH}/XMLER/XMLER.jar i_30000_${TIME_ID}_${INTERFACE_CODE}.xml ${WORK_PATH}
                if [ ! -f ${WORK_PATH}/${TIME_ID}/CONTROL_INFO.del ] ; then
                    echo "解析文件不存在[${WORK_PATH}/${TIME_ID}/CONTROL_INFO.del]"     
                    exit        
                fi
                if [ ! -f ${WORK_PATH}/${TIME_ID}/DEVICE_INFO.del ] ; then
                    echo "解析文件不存在[${WORK_PATH}/${TIME_ID}/DEVICE_INFO.del]"      
                    exit        
                fi
                if [ ! -f ${WORK_PATH}/${TIME_ID}/DEVICE_PROFILE.del ] ; then
                    echo "解析文件不存在[${WORK_PATH}/${TIME_ID}/DEVICE_PROFILE.del]"   
                    exit        
                fi
                if [ ! -f ${WORK_PATH}/${TIME_ID}/PROPERTY_INFO.del ] ; then
                    echo "解析文件不存在[${WORK_PATH}/${TIME_ID}/PROPERTY_INFO.del]"    
                    exit        
                fi
                if [ ! -f ${WORK_PATH}/${TIME_ID}/PROPERTY_VALUE_RANGE.del ] ; then
                    echo "解析文件不存在[${WORK_PATH}/${TIME_ID}/PROPERTY_VALUE_RANGE.del]"     
                    exit        
                fi
                if [ -f ${WORK_PATH}/DIM_CONTROL_INFO.del ] ; then
                     rm ${WORK_PATH}/DIM_CONTROL_INFO.del       
                fi              
                if [ -f ${WORK_PATH}/DIM_DEVICE_INFO.del  ] ; then
                     rm ${WORK_PATH}/DIM_DEVICE_INFO.del        
                fi          
                if [ -f ${WORK_PATH}/DIM_DEVICE_PROFILE.del ] ; then
                     rm ${WORK_PATH}/DIM_DEVICE_PROFILE.del     
                fi        
                if [ -f ${WORK_PATH}/DIM_PROPERTY_INFO.del ] ; then
                     rm ${WORK_PATH}/DIM_PROPERTY_INFO.del
                fi        
                if [ -f ${WORK_PATH}/DIM_PROPERTY_VALUE_RANGE.del ] ; then
                     rm ${WORK_PATH}/DIM_PROPERTY_VALUE_RANGE.del       
                fi
                sed 's/,/$/g' ${WORK_PATH}/${TIME_ID}/CONTROL_INFO.del         >  ${WORK_PATH}/BASS2.DIM_CONTROL_INFO.del
                sed 's/,/$/g' ${WORK_PATH}/${TIME_ID}/DEVICE_INFO.del          >  ${WORK_PATH}/BASS2.DIM_DEVICE_INFO.del
                sed 's/,/$/g' ${WORK_PATH}/${TIME_ID}/DEVICE_PROFILE.del       >  ${WORK_PATH}/BASS2.DIM_DEVICE_PROFILE.del
                sed 's/,/$/g' ${WORK_PATH}/${TIME_ID}/PROPERTY_INFO.del        >  ${WORK_PATH}/BASS2.DIM_PROPERTY_INFO.del
                sed 's/,/$/g' ${WORK_PATH}/${TIME_ID}/PROPERTY_VALUE_RANGE.del >  ${WORK_PATH}/BASS2.DIM_PROPERTY_VALUE_RANGE.del
        fi       
        
        echo "文本格式转换结束,入库开始"
        cd ${WORK_PATH}
        ls -l *.del |awk '{print $9}' > ./file.lst
        while read sfilename
        do
              #生成对应的调度代码
              if [ "${sfilename}" = "BASS2.DIM_TACNUM_DEVID.del" ] ; then
                  control_code="TR1_L_91002"
              fi        
               
              if [ "${sfilename}" = "BASS2.DIM_CONTROL_INFO.del" ] ; then
                  control_code="TR1_L_91003"
              fi 
         
              if [ "${sfilename}" = "BASS2.DIM_DEVICE_INFO.del" ] ; then
                  control_code="TR1_L_91004"
              fi 
        
              if [ "${sfilename}" = "BASS2.DIM_DEVICE_PROFILE.del" ] ; then
                  control_code="TR1_L_91005"
              fi 
              
              if [ "${sfilename}" = "BASS2.DIM_PROPERTY_INFO.del" ] ; then
                  control_code="TR1_L_91006"
              fi
              
              if [ "${sfilename}" = "BASS2.DIM_PROPERTY_VALUE_RANGE.del" ] ; then
                  control_code="TR1_L_91007"
              fi  
        
              #解析文件名,取对应的表名
              creator=`echo ${sfilename}|cut -d. -f1`     
              name=`echo ${sfilename}|cut -d. -f2`
              table_name="${creator}.${name}"
               
              #防止重复加载 
              DB2_SQLCOMM="db2 \"delete from ${table_name} \""
              DB2_SQL_EXEC > /dev/null   
        
              #更新日志为开始
              DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set begintime=current timestamp,endtime=NULL,flag=1,runtime=NULL where CONTROL_CODE='${control_code}'\""   
              DB2_SQL_EXEC > /dev/null
              
              #加载数据 
              echo "LOADING[${table_name}]..."
              DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${sfilename} of del modified by coldel$ timestampformat=\\\"YYYYMMDDHHMMSS\\\" fastparse anyorder warningcount 1000  replace into ${table_name}\""
              #DB2_SQL_EXEC  > /dev/null 
              DB2_SQL_EXEC
              echo "LOADED"
              
              #判断加载是否成功
              DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from ${table_name} \""
              loaded_cnt=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
              rec_cnt=`wc -l ${sfilename} |awk '{print $1}'`
              
        #added by lizhanyong 2008-10-17  数据质量管理
         task_id='M'`echo $control_code|cut -c7-11`
         file_time=`ls -trl ${sfilename} |awk '{print $8}` 
         avl_byte=`ls -trl ${sfilename} |awk '{print $5}`            
        /bassdb1/etl/L/interface_control.sh $task_id $file_time $rec_cnt $avl_byte
             
        #added end 
               
              if [ "${rec_cnt}" = "${loaded_cnt}" ] ; then
                    echo "${sfilename}加载成功,记录数为:${rec_cnt},并更新日志为完成"
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=0 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
              else
                    ALARM_CONTENT="${sfilename}加载失败,加载记录数:${loaded_cnt},文本记录数:${rec_cnt},并更新日志为失败,并告警" 
                    DB2_SQLCOMM="db2 \"update APP.SCH_CONTROL_RUNLOG set endtime=current timestamp,runtime=timestampdiff(2,char(current timestamp-begintime)),flag=-1 where CONTROL_CODE='${control_code}'\""   
                    DB2_SQL_EXEC  > /dev/null
                    DB2_SQLCOMM="db2 \"insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,FLAG) values('${control_code}','load_imei.sh',1,'${ALARM_CONTENT}',current timestamp,-1)\""
                    DB2_SQL_EXEC  > /dev/null
                    echo ${ALARM_CONTENT}
              fi      
               
        done < ./file.lst 
        
        #删除临时生成文件,备份正式文件
        #rm *.del
        mkdir /bassdb1/etl/L/imei/test/${TIME_ID}
        mv ${WORK_PATH}/*.del  /bassdb1/etl/L/imei/test/${TIME_ID}
        
        rm -r ${WORK_PATH}/${TIME_ID}
        if [ ${INTERFACE_CODE} -eq 91002 ] ; then
            mv  ${WORK_PATH}/i_30000_${TIME_ID}_${INTERFACE_CODE}_001.dat ${BAK_PATH}
        fi      
        if [ ${INTERFACE_CODE} -eq 91003 ] ; then
            mv ${WORK_PATH}/i_30000_${TIME_ID}_${INTERFACE_CODE}.xml ${BAK_PATH}
        fi      
        
        echo "文本入库结束"
        
        if [ ${INTERFACE_CODE} -eq 91003 ] ; then        	
            break 
        fi       
        
done

echo "load_imei.sh程序正常退出!"
exit
