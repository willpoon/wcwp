#!/bin/sh
#*************************************************************************************************************
#��������build_fetion.sh
#��  �ܣ���10.233.26.55����ȡ�����ļ����ϲ��ļ����滻�ָ����������ʺ�load_boss.sh���ص��ļ���ʽ
#��  ������
#      
#��д��: 
#��дʱ�� 2009-08-21
#    
#��  ��: ����load_boss.sh���ص��ļ�
#ע�����
#*************************************************************************************************************


#������Ϣ

EXEC_PATH=/bassdb1/etl/L/fetion
WORK_PATH=/bassdb1/etl/L/fetion
BACKUP_PATH=${WORK_PATH}/backup
ERROR_PATH=${WORK_PATH}/error
OBJ_PATH=/bassdb1/etl/L/boss

#��ȡ���µ�����
lastmonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        month=`expr $month + 0`
        month=`expr $month - 1`
        day=00
        if [ $month -eq 0 ]; then
             month=12
             year=`expr $year - 1`
        fi
        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi
        echo $year$month$day
        return 1
}


#��ȡ���µ�����
thismonth()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=01
        echo $year$month$day
        return 1
}

#��ȡ���������
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

echo "�������ݿ�ʼ!"

today=`date '+%Y%m%d'`
sday=`lastmonth $today`

echo $sday



thisday=`thismonth $today`

#sday=20090719
yyyymm=`echo "$sday"|cut -c1-6`
yyyymmdd=`yesterday $today`



if [ $# -eq 1 ] ; then
   sday=$1
fi



while [ true ]
do


#����
if [ ! -f ${BACKUP_PATH}/A_FETIONActiveUser_${yyyymmdd}_000001.txt ] ; then
	
	if [  ! -f ${WORK_PATH}/A_FETIONActiveUser_${yyyymmdd}_000001.Z ] ; then
		
		rsh 10.233.26.55 -l xzgwjk /data1/interface/ftp_fetion_day.sh
		#ȡ���Žӿ��ļ�
		./ftp_cb_interface.sh  10.233.26.55 xzgwjk xzgwjk /data1/interface/data/day /bassdb1/etl/L/fetion A_FETIONActiveUser_${yyyymmdd}_000001.Z
		./ftp_cb_interface.sh  10.233.26.55 xzgwjk xzgwjk /data1/interface/data/day /bassdb1/etl/L/fetion A_FETIONActiveUser_${yyyymmdd}.CHK
		
	fi
fi


#�л��ع���Ŀ¼ 
cd $WORK_PATH

#########�ϲ��ļ�,�滻�ָ���'&&'Ϊ'$',����AVL��CHK�ļ�

#M_FETIONMonthlyActivityUser_20090700_000001.txt

   if [ -f A_FETIONActiveUser_${yyyymmdd}_000001.txt ] ; then
   	
   	     data_file_name=I40015${yyyymmdd}000000.AVL
   	     chk_file_name=I40015${yyyymmdd}000000.CHK
   	     
          cp A_FETIONActiveUser_${yyyymmdd}_000001.txt $data_file_name
          
          sed 's/&&/$/g' $data_file_name > ${data_file_name}.temp
          
          mv ${data_file_name}.temp $data_file_name
          
          #ѹ��                        
          compress -f $data_file_name
   
        
          #ȡ�ô�С,��¼����
          file_size=`ls -l $data_file_name.Z|awk '{print $5}'`
          rec_cnt=`awk -F'|' '{print $2}' A_FETIONActiveUser_${yyyymmdd}.CHK`         
          
          #�ļ�����ʱ��
          file_create_time=`date '+%Y%m%d%H%M%S'`
          
          #����У���ļ�       
          echo "I40015${sday}000000.AVL.Z\$${file_size}\$${rec_cnt}\$${sday}\$${file_create_time}" > ${chk_file_name}
          
          echo "У���ļ�����"
          
	#��AVL��CHK�ļ��ƶ�������Ŀ¼
	mv ${data_file_name}.Z $OBJ_PATH 
	mv ${chk_file_name} $OBJ_PATH
	
	#��ԭʼ�ļ��Ƶ�backupĿ¼����
	mv A_FETIONActiveUser_${yyyymmdd}_000001.txt $BACKUP_PATH/
	mv A_FETIONActiveUser_${yyyymmdd}.CHK $BACKUP_PATH/
                    
   fi 
   
if [ ! -f ${BACKUP_PATH}/A_FETIONActiveUser_${yyyymmdd}_000001.txt ] ; then
	sleep 1200
else
	break
fi

done
   
echo "�������ݽ���!"
exit 0
 
