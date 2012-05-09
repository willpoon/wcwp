#!/bin/ksh

#*******************************************************
#函数名：ftp_43_to_27.sh
#功  能：自动将接口文件送到27加载目录
#参  数：
#   Useage:  ./ftp_43_to_27.sh  
#      
#输  出: 
#注意事项：
#*******************************************************

#传送的目标主机
host_ip=172.16.9.27
user_id=load
password=load


function main
{

#取上一天日期
PreDay=`TZ=${TZ}+24 date +%Y%m%d`

#CRM（boss路径）、飞信及彩铃
for src_lst in boss ca mr
do
		src_path=${HOME}/${src_lst}/backup
		obj_path=/bassdb/etl/L/$src_lst
	  cd $src_path
	  
	  #若昨天的目录存在，则说明已经作了文件按目录分类处理，修改源路径
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
    
    #crm抽取
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
    
    #飞信抽取      
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
         				
		#彩铃抽取  	
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
