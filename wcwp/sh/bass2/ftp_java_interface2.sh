#!/bin/ksh

#*******************************************************
#函数名：ftp_java_interface.sh
#功  能：自动将接口文件送到60加载目录
#参  数：
#      
#      
#输  出: 
#注意事项：
#*******************************************************


host_ip=172.16.9.25
user_id=bass2
password=bass2
obj_dir=/bassapp/bass2/load/boss
src_dir=/bassapp/bass2/ifboss/data
EXEC_PATH=/bassapp/bass2/ifboss/crm_interface/bin

#把接口传到正式机供加载
ftp_interface(){
host_ip=172.16.5.43
user_id=load
password=load
obj_dir=/bassdb1/etl/L/mtload/backup/mtbackup
src_dir=/bassapp/bass2/ifboss/mtdata
	
				ftp -n $host_ip <<EOF
					prompt
					user $user_id $password
					bin
					cd $obj_dir
					lcd $src_dir
					put  ${filename}.AVL.Z
					put  ${filename}.CHK
					bye
EOF
}

#*******************************************************
#函数名：backup_mtdata
#功  能：针对所抽中测接口备份到/bassapp/bass2/ifboss/mtdata,防止竞争
#参  数：$filename：接口文件名称
#      
#      
#输  出: 
#注意事项：中测完成后只需移除本函数体及backup_mtdata ${filename} 指令即可。
#*******************************************************
#2010-12-28 10:09:30 by panzw 针对所抽中测接口备份到/bassapp/bass2/ifboss/mtdata
backup_mtdata(){
	PROPERTIES_PATH=/bassapp/bass2/ifboss/crm_interface/bin/config/CRM
	INTERFACE_CODE=`echo ${filename}| awk '{print substr($1,1,6)}'`
	MT_SRC_DIR=/bassapp/bass2/ifboss/mtdata
	filename=$1
	cd $src_dir
	#taskList_mt0.properties 未所有中测必测接口列表
	cat ${PROPERTIES_PATH}/taskList_mt0.properties | grep ${INTERFACE_CODE} > /dev/null
	if [ $? = 0 ] ; then 
		cp ${filename}* ${MT_SRC_DIR}
		ftp_interface
	fi
}





main()
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
					cd $obj_dir
					lcd $src_dir
					put  ${filename}.AVL.Z
					put  ${filename}.CHK
					bye
EOF
				cd $src_dir
				#2010-12-28 10:11:35 by panzw 针对所抽中测接口备份到/bassapp/bass2/ifboss/mtdata
				backup_mtdata ${filename}
				rm  ${filename}*
			fi
			
		done<${EXEC_PATH}/ftp_file.lst 
		
		sleep 60
	
	done
}
current_day=`date '+%Y%m%d'`
main > ./log/ftpdata.log
