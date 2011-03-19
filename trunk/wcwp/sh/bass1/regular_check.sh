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
		if [ $# -ne 1 ];then
		echo "getmtime filename"
		exit
		fi
		in_file=$1
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh`
		export tcltimestamp
		file_mtime=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
		echo ${file_mtime}
}

secchk(){
	this_script=`pwd`/$0
	pattern='rm|del|mv'
	egrep -i ${pattern} ${this_script}|grep -v 'pattern.*m.*l.*v' >>/dev/null
	if [ $? -eq 0 ] ; then 
	echo ">>>>>>�ű����ڲ���ȫ���أ���"
	echo ">>>>>>egrep -in '${pattern}' `pwd`/$0"
  exit
	fi
}

secchk

###
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
###
ls -lrt ${exp_dir}|grep dat

echo "\n"
echo ">>>>>>today           ��ǰ���� :${today}"
echo ">>>>>>deal_date   ������������ :${deal_date}"

test -d ${exp_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>���ݵ���Ŀ¼�����ڣ������飡��"
exit
fi

echo ">>>>>>exp_dir     ���ݵ���Ŀ¼ :  /bassapp/backapp/data/bass1/export/export_${deal_date}"

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



test -d ${rpt_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>���淵��Ŀ¼�����ڣ������飡��"
exit
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

if [ ${record_rpt_cnt} -gt 0 -a  ${record_rpt_cnt} -lt ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${record_rpt_cnt}`
echo ">>>>>>��¼��У����δ������ȫ!!!���� ${difference_cnt} ��!!!"
fi

if [ ${record_rpt_cnt} -eq ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>��¼��У�鷵����ȫ����"
echo "\n"
fi

putdatfile(){
		FTPHOST=172.16.9.25
		REMOTE_DIR=/bassapp/bass2/panzw2/data
		LOCAL_DIR=${exp_dir}
		HOME=/bassapp/bihome/panzw
		export HOME
		echo ">>>>>>Ŀ������ : ${FTPHOST}"
		echo ">>>>>>��ҵʱ�� : `date`"		
		echo ">>>>>>HOME : ${HOME}"
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		tcltimestamp=`echo "puts [file mtime ${ftped_file_list}]"|tclsh`
		export tcltimestamp
		upload_dat_time=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
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
		
		
		#�ϴ�
		#ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		#if [ ${ftped_dat_cnt} -gt 0 ];then 
		#return 1
		#else 
		ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}
		
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		tcltimestamp=`echo "puts [file mtime ${ftped_file_list}]"|tclsh`
		export tcltimestamp
		upload_dat_time=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
		echo ">>>>>>�ո��ϴ���${ftped_dat_cnt}���ӿ�!ʱ����:${upload_dat_time} ,��־·��:${ftped_file_list}"
		else "dirȡ���ϴ��ļ��б�ʧ�ܣ������飡��"
		fi		
		
		#fi
		#�뱾��У�飺�ļ���|�ļ���|�ļ���С
		#�ָ�$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>HOME : ${HOME}"
		echo ">>>>>>PID\$\$ $$"
}

echo ">>>>>>PID\$\$ $$"

echo ">>>>>>>>>>>>>>>>>>>>>>>>ִ��dat�ļ��ϴ�>>>>>>>>>>>>>>>>>>>>>>"

putdatfile
