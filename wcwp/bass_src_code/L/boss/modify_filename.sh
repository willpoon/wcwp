#!/bin/sh
#*************************************************************************************************************
#��������modify_filename.sh
#��  �ܣ������޸Ľӿ��ļ�����     
#��д��: lizhanyong 
#��дʱ�� 2009-10-27    
#��  ��: ��־nohup.out

#Ŀǰ����ʵ������֪�ļ�Ŀ¼���������޸��ļ���������Լ������ƣ���δ֪�ļ�Ŀ¼������£����ҵ��ļ����޸�����
#������find . -name 'P99641*' ����������
#*************************************************************************************************************


  source_filename="P99641"
  obj_filename="P09641"		
 
 
  WORK_PATH=/bassdb2/etl/L/boss/backup/
  cd $WORK_PATH
  
  ###################################################
  #�˲����Ǹ���/bassdb2/etl/L/boss/backup/Ŀ¼�µ��ļ�����
  
  ls -l *${source_filename}* |awk '{print $9}' > ./interface.list	
	echo `ls -l *${source_filename}* |awk '{print $9}'`
	
	while read sfilename
	do
		
##����Ŀ���ļ���	
		temp_filename=`echo "$sfilename"|cut -c7-100`
		object_filename=${obj_filename}${temp_filename}			
		echo object_filename=${object_filename}

##����Ŀ���ļ�	
    
	  mv $sfilename $object_filename		
	  echo "mv $sfilename $object_filename"
	  
	done < ./interface.list
	     rm ./interface.list
	     
  ######################################################### 
  #�˲����Ǹ���/bassdb2/etl/L/boss/backup/Ŀ¼���¼�Ŀ¼�µ��ļ����ƣ���/bassdb2/etl/L/boss/backup/20091001
  
  #ls -l �����d��ͷ��ΪĿ¼
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
		
##����Ŀ���ļ���	
		temp_filename=`echo "$sfilename"|cut -c7-100`
		object_filename=${obj_filename}${temp_filename}			
		echo object_filename=${object_filename}

##����Ŀ���ļ�	
    
	  mv $sfilename $object_filename		
	  echo "mv $sfilename $object_filename"
	  
	done < ./interface.list
	     rm ./interface.list	
	     
	done < ./directory.list      
	     rm ./directory.list 
	
	######################################################     

echo "������������!"
