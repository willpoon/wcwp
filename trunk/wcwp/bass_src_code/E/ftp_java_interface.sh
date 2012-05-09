#!/bin/ksh

#*******************************************************
#函数名：ftp_java_interface.sh
#功  能：自动将接口文件送到43加载目录
#参  数：
#      
#      
#输  出: 
#注意事项：
#*******************************************************


host_ip=172.16.5.43
user_id=load
password=load
obj_dir=/bassdb1/etl/L/boss
src_dir=/bassdb2/etl/E/boss/jyfx/tocrm
EXEC_PATH=/bassdb2/etl/E/boss/bin



function main
{
	while [ true ] 
	do
	       #若停止标志文件存在，则程序退出
	       if [ -f ${EXEC_PATH}/stop_ftp ] ; then
	           echo "发现停止标志,程序退出!"
	           break
	       fi
	       
	       cd $src_dir
	       
		ls -l *.CHK | awk '{print substr($9,1,length($9)-4)}' > ${EXEC_PATH}/ftp_file.lst
		
		echo `date`
		while read filename
		do 
			
			if [ -f ${filename}.AVL.Z -a -f ${filename}.CHK ] ; then 
				echo "ftp ${filename}"
				ftp -n $host_ip <<EOF
					prompt
					user $user_id $password
					bin
					tcpwindow 900000
					cd $obj_dir
					lcd $src_dir
					
					
					put ${filename}.AVL.Z
					put ${filename}.CHK
					bye
EOF
				cd $src_dir
				rm ${filename}*
			fi
			
		done<${EXEC_PATH}/ftp_file.lst 
		
		sleep 60
	
	done
}
current_day=`date '+%Y%m%d'`
main > ./log/ftpdata.log
