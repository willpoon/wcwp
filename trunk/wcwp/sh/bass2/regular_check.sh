#�˽ű�������app������
#�����ϴ�8��9��ӿ�
#�ϴ���verf �� ִ�� bass1_rpt 
#��֤�ű�����ִ�в�Ӱ������
#���սӿ�

secchk(){
	#������ȫ���
	this_script=`pwd`/$0
	pattern='rm|del|mv'
	except_pattern1="grep -iv 'pattern.*m.*l.*v'"
	except_pattern2="grep -iv 'alarm'"
	except_pattern3="grep -iv 'terminate'"
	egrep -in ${pattern} ${this_script}|grep -iv 'pattern.*m.*l.*v'|grep -iv 'alarm'|grep -iv 'terminate'
	ret_code=$?
	if [ ${ret_code} -eq 0 ] ; then 
	echo ">>>>>>�ű����ڲ���ȫ���أ���"
	#echo ">>>>>>egrep -in '${pattern}' `pwd`/$0|$except_pattern1|$except_pattern2|$except_pattern3"
	return 1
	else 
		if [ ${ret_code} -ne 1 ]; then 
		echo ">>>>>>grep������"	
	  return 1
	  fi
	fi
	return 0
}

chkputtime(){
	CURR_HHMM=`date "+%H%M"`
	MAX_HHMM="0900"
	if [ ${CURR_HHMM} -gt ${MAX_HHMM} ] ; then 
	echo ">>>>>>�������ϴ�ʱ��!!��Ҫǿ���ϴ������� $0 force ��ִ��!!"
	return 1
	fi
	return 0
}



yesterday()
{
	#usage:yesterday yyyymmdd
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

getmtime(){
		#����ϴ��ļ���ϸ�޸�����
		if [ $# -ne 1 ];then
		echo "getmtime filename"
		exit
		fi
		in_file=$1
		if [ ! -f ${in_file} ];then 
		echo "getmtime ���������ļ� ${in_file} ������!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh`
		export tcltimestamp
		file_mtime=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
		echo ${file_mtime}
		return 0
}


#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
#echo ${DB2_OSS_PASSWD}	
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM
    db2 commit
    db2 connect reset
}


ifalarm(){
	#������ڸ澯δ��������1
	sql_str="select 'xxxxx',count(0) from  app.sch_control_alarm where alarmtime >=  timestamp('"${today}"'||'000000') and flag = -1 and control_code like 'BASS1%'"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	echo ${DB2_SQLCOMM}
	alarm_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`
	if [ -z ${alarm_cnt} ];then 
		echo ">>>>>>����ȡ��alarm_cnt �� (����ȡ��alarm_cnt = ${alarm_cnt} )"	
	return 1
	fi
		echo ">>>>>>alarm_cnt = ${alarm_cnt}"
	if [ ${alarm_cnt} -gt 0 ];then
		echo ">>>>>>���и澯δ����!!�����ϴ���!!"
		return 1
	fi
}

chkemptyfile(){
		empty_file_cnt=`ls -l ${exp_dir}/*.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l|awk '{print $1}'`
		echo ">>>>>>empty_file_cnt  ���ļ���: ${empty_file_cnt}"
		return 0
	}

putdatfile(){
		#ʵ��dat�ļ��ϴ�
		HOME=/bassapp/bihome/panzw/config
		export HOME
		echo ">>>>>>Ŀ������ : ${FTPHOST}"
		echo ">>>>>>��ҵʱ�� : `date`"		
		echo ">>>>>>PID\$\$ $$"
		echo ">>>>>>�޸� HOME : ${HOME}"
		
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		#��鴫�����
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
		echo ">>>>>>dat file ֮ǰ�Ѿ��ϴ�,��ȷ���Ƿ���Ҫ�����ϴ���\n>>>>>>�����ش�����ɾ��${FTPHOST}�ϵ�'*.dat',��ɾ��${ftped_file_list}  !!!"
		echo ">>>>>>���һ���ϴ���${ftped_dat_cnt}���ӿ�!ʱ����:${upload_dat_time}"
		return 1
		fi
		
		#����ftp�����ļ�
		echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_dat_file}
		echo "lcd ${LOCAL_DIR}" >> ${ftp_mac_put_dat_file}
		echo "bin" >> ${ftp_mac_put_dat_file}
		echo "prompt off" >> ${ftp_mac_put_dat_file}
		echo "mput *.dat" >> ${ftp_mac_put_dat_file}
		#��Ϊδ��verfʱdat�������ߣ����Կ�����dir������ϴ��б�
		echo "dir *.dat ${ftped_file_list}" >> ${ftp_mac_put_dat_file}
		
		if [ ! -f ${ftp_mac_put_dat_file} ];then 
		echo ">>>>>>ftp macro �ļ�δ���� !!"
		return 1
		fi

		ftp_mac_put_dat_file_cnt=`wc -l ${ftp_mac_put_dat_file}|awk '{print $1}'`		
		if [ ${ftp_mac_put_dat_file_cnt} -ne 6 ];then 
		echo ">>>>>>ftp macro д������ !!"
		return 1
		fi
				
		
		#�ϴ�
		echo ">>>>>>ִ��verf�ļ��ϴ�>>>>>>>>>>>>>>>>>>>>>>"
		ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}
		#�ָ�$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>�ָ� HOME : ${HOME}"
		
		#��ӡ�ϴ����
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
		echo ">>>>>>�ո��ϴ���${ftped_dat_cnt}���ӿ�!ʱ����:${upload_dat_time} ,��־·��:${ftped_file_list}"
		else 
		echo ">>>>>>FTP dir �����ȡ�ϴ��ļ��б�ʧ�ܣ������飡��"
		return 1
		fi		
		
		return 0
}

putverffile(){
		#ʵ��verf�ļ��ϴ�
		HOME=/bassapp/bihome/panzw/config
		export HOME
		echo ">>>>>>Ŀ������ : ${FTPHOST}"
		echo ">>>>>>��ҵʱ�� : `date`"		
		echo ">>>>>>PID\$\$ $$"
		echo ">>>>>>�޸� HOME : ${HOME}"
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftp_mac_put_verf_file=${HOME}/put_verf.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		#���ǰ���ļ����
		if [ ! -f ${ftp_mac_put_dat_file} ];then 
		echo ">>>>>>ftp mput macro �ļ�δ���� !!"
		return 1
		fi
				
		if [ ! -f ${ftped_file_list} ];then 
		echo ">>>>>>δ�ҵ�dat�ϴ���־�ļ�,��ȷ��dat�Ƿ����ϴ�!!"		
		return 1
		else 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
				if [ ${ftped_dat_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
					echo ">>>>>>dat�ϴ�����������${DAY_INTERFACE_CNT},��ȷ��dat�Ƿ�����ȷ�ϴ�!!"		
					return 1
				fi
		fi
		
		#����ftp�����ļ�
		echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_verf_file}
		echo "lcd ${LOCAL_DIR}" >> ${ftp_mac_put_verf_file}
		echo "bin" >> ${ftp_mac_put_verf_file}
		echo "prompt off" >> ${ftp_mac_put_verf_file}
		echo "mput *.verf" >> ${ftp_mac_put_verf_file}
		
		if [ ! -f ${ftp_mac_put_verf_file} ];then 
		echo ">>>>>>ftp macro �ļ�δ���� !!"
		return 1
		fi

		ftp_mac_put_verf_file_cnt=`wc -l ${ftp_mac_put_verf_file}|awk '{print $1}'`		
		if [ ${ftp_mac_put_verf_file_cnt} -ne 5 ];then 
		echo ">>>>>>ftp macro д������ !!"
		return 1
		fi

		#�ϴ�
		echo ">>>>>>ִ��verf�ļ��ϴ�>>>>>>>>>>>>>>>>>>>>>>"
		ftp -v ${FTPHOST} < ${ftp_mac_put_verf_file}
		#�ָ�$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>�ָ� HOME : ${HOME}"
		
		return 0	
}
###################################main program##########################

#1. security check 
secchk
if [ $? -eq 1 ] ; then 
exit
fi


#2.var define
today=`date '+%Y%m%d'`
deal_date=`yesterday ${today}`
DAY_INTERFACE_CNT=56
dat_file_cnt=
verf_file_cnt=
file_rpt_cnt=
difference_cnt=
record_rpt_cnt=
exp_dir="/bassapp/backapp/data/bass1/export/export_${deal_date}"
rpt_dir="/bassapp/backapp/data/bass1/report/report_${deal_date}"
#ftp config >>
FTPHOST=172.16.9.25
REMOTE_DIR=/bassapp/bass2/panzw2/data
LOCAL_DIR=${exp_dir}

#1.1 alarm check 

#ifalarm
if [ $? -eq 1 ] ; then 
exit
fi

#ftp config <<		
#3.print *.dat 
test -d ${exp_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>���ݵ���Ŀ¼�����ڣ������飡��"
exit
fi
ls -lrt ${exp_dir}|grep dat

echo "\n"
echo ">>>>>>PID\$\$ $$"
#4.print datetime & basic info 
echo ">>>>>>today           ��ǰ���� :${today}"
echo ">>>>>>deal_date   ������������ :${deal_date}"

echo ">>>>>>exp_dir     ���ݵ���Ŀ¼ :  ${exp_dir}"

first_exp_file=`ls -1rt ${exp_dir}/*.dat|head -1`
last_exp_file=`ls -1rt ${exp_dir}/*.dat|tail -1`

echo ">>>>>>first_exp_file  �������� :`getmtime ${first_exp_file}`"
echo ">>>>>> last_exp_file  �������� :`getmtime ${last_exp_file}`"


dat_file_cnt=`ls -lrt ${exp_dir}/*.dat | wc -l|awk '{print $1}'`
echo ">>>>>>dat_file_cnt  �����ļ��� :${dat_file_cnt}"


if [ ${dat_file_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>dat_file_cnt  �����ļ��� ������ ${DAY_INTERFACE_CNT} ,���ȴ���"
exit
fi


verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l|awk '{print $1}'`
echo ">>>>>>verf_file_cnt У���ļ��� :${verf_file_cnt}"


if [ ${verf_file_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>verf_file_cnt  У���ļ��� ������ ${DAY_INTERFACE_CNT} ,���ȴ���"
exit
fi
#ͳ�ƿ��ļ���
chkemptyfile


#echo ">>>>>>>>>>>>>>>>>>>>>>>>ִ��dat�ļ��ϴ�>>>>>>>>>>>>>>>>>>>>>>"
#2. ����������
if [ $# -eq 0 ];then 
#����ϴ�ʱ��
chkputtime
	if [ $? -eq 0 ];then 
	#ִ���ϴ�
		#�緵��У�飬��Ӧ���ϴ�
		test -d ${rpt_dir}
		if [ $? -eq 0 ] ; then 
		echo ">>>>>>�ѷ����ļ���У�鱨�棬�����ظ��ϴ�!!"
		exit
		fi
		 putdatfile
		ret_code=$?
		if [ ${ret_code} -eq 0 ];then 
		sleep 5
		 putverffile
		fi 
	else 
	#echo ">>>>>>���ڲ��������ϴ�ʱ�Σ�ֻ���򵥼�� !!"
	echo "\n"
	fi
else 
	#ǿ���ϴ�
	if [ $# -eq 1 -a $1 = "force" ];then 
	#ִ���ϴ�
		#test -d ${rpt_dir}
		if [ $? -eq 0 ] ; then 
		echo ">>>>>>�ѷ����ļ���У�鱨�棬�����ظ��ϴ�!!"
		exit
		fi	
	  putdatfile
		ret_code=$?
		if [ ${ret_code} -eq 0 ];then 
		 sleep 5
		 putverffile
		fi 	
	fi
fi
###>>>>>>>>>>>>>>>>>>>>>>>>���dat�ļ��ϴ�>>>>>>>>>>>>>>>>>>>>>>



###>>>>>>>>>>>>>>>>>>>>>>>>�鿴���淵�����>>>>>>>>>>>>>>>>>>>>>>

test -d ${rpt_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>>>>>>>>>>>>>>>>>>>��ʼ��ȡ�ļ�����¼��У��>>>>>>>>>>>>>>>>>>>>>>"
echo /bassapp/backapp/bin/bass1_report/bass1_report
echo ">>>>>>>>>>>>>>>>>>>>>>>>������ȡ�ļ�����¼��У��>>>>>>>>>>>>>>>>>>>>>>"
		test -d ${rpt_dir}
		if [ $? -eq 1 ] ; then 
		echo ">>>>>>���淵��Ŀ¼�����ڣ������飡��"
		exit
		fi
fi

echo ">>>>>>rpt_dir     ���淵��Ŀ¼ :  ${rpt_dir}"

#ͳ���ļ�������
test -f ${rpt_dir}/f*
if [ $? -eq 1 ] ; then 
file_rpt_cnt=0
echo ">>>>>>�ļ���У����δ����!!"
else
file_rpt_cnt=`ls -lrt ${rpt_dir}/f* | wc -l|awk '{print $1}'`
fi
echo ">>>>>>file_rpt_cnt�ļ��������� :${file_rpt_cnt}"
#�ļ�������ͳ��
if [ ${file_rpt_cnt} -ne ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${file_rpt_cnt}`
echo ">>>>>>�ļ���У����δ������ȫ!!!���� ${difference_cnt} ��!!!"
echo ">>>>>>�ļ���У��Ӧ�ܿ췵����ȫ�������������ϴ��Ƿ���©����"
difference_cnt=""
exit
else
echo ">>>>>>�ļ���У�鷵����ȫ����"
fi

#ͳ�Ƽ�¼������
test -f ${rpt_dir}/r*
if [ $? -eq 1 ] ; then 
record_rpt_cnt=0
echo ">>>>>>��¼��У����δ����!!"
else 
record_rpt_cnt=`ls -lrt ${rpt_dir}/r* | wc -l|awk '{print $1}'`
fi
echo ">>>>>>record_rpt_cnt��¼������ :${record_rpt_cnt}"
#ͳ�Ƽ�¼������
if [ ${record_rpt_cnt} -gt 0 -a  ${record_rpt_cnt} -lt ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${record_rpt_cnt}`
echo ">>>>>>��¼��У����δ������ȫ!!!���� ${difference_cnt} ��!!!"
fi

if [ ${record_rpt_cnt} -eq ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>��¼��У�鷵����ȫ����"
echo "\n"
fi

###>>>>>>>>>>>>>>>>>>>>>>>>�����鿴���淵�����>>>>>>>>>>>>>>>>>>>>>>
