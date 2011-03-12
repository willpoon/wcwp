#!/bin/ksh

#*******************************************************
#��������ftp_java_interface.sh
#��  �ܣ��Զ����ӿ��ļ��͵�60����Ŀ¼
#��  ����
#      
#      
#��  ��: 
#ע�����
#*******************************************************


host_ip=172.16.9.25
user_id=bass2
password=bass2
obj_dir=/bassapp/bass2/load/boss
src_dir=/bassapp/bass2/ifboss/data
EXEC_PATH=/bassapp/bass2/ifboss/crm_interface/bin

#�ѽӿڴ�����ʽ��������
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
#��������backup_mtdata
#��  �ܣ���������в�ӿڱ��ݵ�/bassapp/bass2/ifboss/mtdata,��ֹ����
#��  ����$filename���ӿ��ļ�����
#      
#      
#��  ��: 
#ע������в���ɺ�ֻ���Ƴ��������弰backup_mtdata ${filename} ָ��ɡ�
#*******************************************************
#2010-12-28 10:09:30 by panzw ��������в�ӿڱ��ݵ�/bassapp/bass2/ifboss/mtdata
backup_mtdata(){
	PROPERTIES_PATH=/bassapp/bass2/ifboss/crm_interface/bin/config/CRM
	INTERFACE_CODE=`echo ${filename}| awk '{print substr($1,1,6)}'`
	MT_SRC_DIR=/bassapp/bass2/ifboss/mtdata
	filename=$1
	cd $src_dir
	#taskList_mt0.properties δ�����в�ز�ӿ��б�
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
	       #��ֹͣ��־�ļ����ڣ�������˳�
	       if [ -f ${EXEC_PATH}/stop_ftp ] ; then
	           echo "����ֹͣ��־,�����˳�!"
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
				#2010-12-28 10:11:35 by panzw ��������в�ӿڱ��ݵ�/bassapp/bass2/ifboss/mtdata
				backup_mtdata ${filename}
				rm  ${filename}*
			fi
			
		done<${EXEC_PATH}/ftp_file.lst 
		
		sleep 60
	
	done
}
current_day=`date '+%Y%m%d'`
main > ./log/ftpdata.log
