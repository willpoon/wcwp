#!/bin/sh
#*************************************************************************************************************
#函数名：modify_filename.sh
#功  能：批量修改接口文件名称     
#编写人: lizhanyong 
#编写时间 2009-10-27    
#输  出: 日志nohup.out

#目前功能实现了已知文件目录进行批量修改文件；后面可以继续完善，在未知文件目录的情况下，先找到文件再修改名称
#考虑用find . -name 'P99641*' 这样的命令
#*************************************************************************************************************


  source_filename="P99641"
  obj_filename="P09641"		
 
 
  WORK_PATH=/bassdb2/etl/L/boss/backup/
  cd $WORK_PATH
  
  ###################################################
  #此部分是更改/bassdb2/etl/L/boss/backup/目录下的文件名称
  
  ls -l *${source_filename}* |awk '{print $9}' > ./interface.list	
	echo `ls -l *${source_filename}* |awk '{print $9}'`
	
	while read sfilename
	do
		
##生成目标文件名	
		temp_filename=`echo "$sfilename"|cut -c7-100`
		object_filename=${obj_filename}${temp_filename}			
		echo object_filename=${object_filename}

##生成目标文件	
    
	  mv $sfilename $object_filename		
	  echo "mv $sfilename $object_filename"
	  
	done < ./interface.list
	     rm ./interface.list
	     
  ######################################################### 
  #此部分是更改/bassdb2/etl/L/boss/backup/目录的下级目录下的文件名称，如/bassdb2/etl/L/boss/backup/20091001
  
  #ls -l 结果以d开头的为目录
  ls -l |grep '^d' |awk '{print $9}' >./directory.list
  echo 	`ls -l |grep '^d' |awk '{print $9}'` 
  
  while read directory
  	do 
    cd $WORK_PATH$directory
    pwd
    
	ls -l *${source_filename}* |awk '{print $9}' > ./interface.list	
	echo `ls -l *${source_filename}* |awk '{print $9}'`
	
	while read sfilename
	do
		
##生成目标文件名	
		temp_filename=`echo "$sfilename"|cut -c7-100`
		object_filename=${obj_filename}${temp_filename}			
		echo object_filename=${object_filename}

##生成目标文件	
    
	  mv $sfilename $object_filename		
	  echo "mv $sfilename $object_filename"
	  
	done < ./interface.list
	     rm ./interface.list	
	     
	done < ./directory.list      
	     rm ./directory.list 
	
	######################################################     

echo "程序正常结束!"
