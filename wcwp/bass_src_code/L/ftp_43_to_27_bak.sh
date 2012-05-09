#!/bin/ksh

#*******************************************************
#��������ftp_43_to_27.sh
#��  �ܣ��Զ����ӿ��ļ��͵�27����Ŀ¼
#��  ����
#   Useage:  ./ftp_43_to_27.sh  
#      
#��  ��: 
#ע�����
#*******************************************************

#���͵�Ŀ������
host_ip=172.16.9.27
user_id=load
password=load


function main
{

#ȡ��һ������
PreDay=`TZ=${TZ}+24 date +%Y%m%d`

#CRM��boss·���������ż�����
for src_lst in boss ca mr
do
		src_path=${HOME}/${src_lst}/backup
		obj_path=/bassdb/etl/L/$src_lst
	  cd $src_path
	  
	  #�������Ŀ¼���ڣ���˵���Ѿ������ļ���Ŀ¼���ദ���޸�Դ·��
	  if [ -d $PreDay ]; then
	  src_path=${src_path}/$PreDay
	  fi
	  echo $src_path
	  cd $src_path
	 
	  ls -l *$PreDay* > ${HOME}/$src_lst/backup/ftp_to_27_lst/ftp_file_${PreDay}.lst
	  if [ $? -gt 0 ]; then
	 	 echo "No file to put"
	  fi 

    date "+%Y-%m-%d %H:%M:%S"
    
    #crm��ȡ
	  if [ $src_lst="boss" ]; then
	  			echo $src_lst
	  			ftp -n $host_ip <<EOF
					prompt
					user $user_id $password
					bin
					tcpwindow 900000
					cd $obj_path
					lcd $src_path
					mput *$PreDay*.AVL.Z
					mput *$PreDay*.CHK
					bye    
EOF
    fi
    
    #���ų�ȡ      
		if [ $src_lst="ca" ]; then
					echo $src_lst
					ftp -n $host_ip <<EOF
					prompt
					user $user_id $password
					bin
					tcpwindow 900000
					cd $obj_path
					lcd $src_path
					mput *$PreDay*.dat
					mput *$PreDay*.verf
					bye
EOF
    fi 
         				
		#�����ȡ  	
		if [ $src_lst="mr" ]; then 	
					echo $src_lst
					ftp -n $host_ip <<EOF
					prompt
					user $user_id $password
					bin
					tcpwindow 900000
					cd $obj_path
					lcd $src_path
					mput *$PreDay*.AVL
					mput *$PreDay*.SUM
					mput *$PreDay*.CHK
					bye
EOF
		fi
done
}
current_day=`date '+%Y%m%d'`
main >> ./log/ftpdata_27.log 2>&1
