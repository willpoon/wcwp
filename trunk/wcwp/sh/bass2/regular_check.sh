#此脚本必须在app下运行
#优先上传8个9点接口
#上传完verf 后 执行 bass1_rpt 
#保证脚本重新执行不影响数据
#检查空接口

secchk(){
	#操作安全检查
	this_script=`pwd`/$0
	pattern='rm|del|mv'
	except_pattern1="grep -iv 'pattern.*m.*l.*v'"
	except_pattern2="grep -iv 'alarm'"
	except_pattern3="grep -iv 'terminate'"
	egrep -in ${pattern} ${this_script}|grep -iv 'pattern.*m.*l.*v'|grep -iv 'alarm'|grep -iv 'terminate'
	ret_code=$?
	if [ ${ret_code} -eq 0 ] ; then 
	echo ">>>>>>脚本存在不安全因素！！"
	#echo ">>>>>>egrep -in '${pattern}' `pwd`/$0|$except_pattern1|$except_pattern2|$except_pattern3"
	return 1
	else 
		if [ ${ret_code} -ne 1 ]; then 
		echo ">>>>>>grep出错！！"	
	  return 1
	  fi
	fi
	return 0
}

chkputtime(){
	CURR_HHMM=`date "+%H%M"`
	MAX_HHMM="0900"
	if [ ${CURR_HHMM} -gt ${MAX_HHMM} ] ; then 
	echo ">>>>>>非正常上传时段!!如要强制上传，请用 $0 force 来执行!!"
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
		#获得上次文件详细修改日期
		if [ $# -ne 1 ];then
		echo "getmtime filename"
		exit
		fi
		in_file=$1
		if [ ! -f ${in_file} ];then 
		echo "getmtime 所操作的文件 ${in_file} 不存在!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh`
		export tcltimestamp
		file_mtime=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
		echo ${file_mtime}
		return 0
}


#在DB2数据库中执行SQL
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
	#如果存在告警未处理，返回1
	sql_str="select 'xxxxx',count(0) from  app.sch_control_alarm where alarmtime >=  timestamp('"${today}"'||'000000') and flag = -1 and control_code like 'BASS1%'"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	echo ${DB2_SQLCOMM}
	alarm_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`
	if [ -z ${alarm_cnt} ];then 
		echo ">>>>>>不能取得alarm_cnt ！ (不能取得alarm_cnt = ${alarm_cnt} )"	
	return 1
	fi
		echo ">>>>>>alarm_cnt = ${alarm_cnt}"
	if [ ${alarm_cnt} -gt 0 ];then
		echo ">>>>>>尚有告警未处理!!请马上处理!!"
		return 1
	fi
}

chkemptyfile(){
		empty_file_cnt=`ls -l ${exp_dir}/*.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l|awk '{print $1}'`
		echo ">>>>>>empty_file_cnt  空文件数: ${empty_file_cnt}"
		return 0
	}

putdatfile(){
		#实现dat文件上传
		HOME=/bassapp/bihome/panzw/config
		export HOME
		echo ">>>>>>目标主机 : ${FTPHOST}"
		echo ">>>>>>作业时间 : `date`"		
		echo ">>>>>>PID\$\$ $$"
		echo ">>>>>>修改 HOME : ${HOME}"
		
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		#检查传输情况
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
		echo ">>>>>>dat file 之前已经上传,请确认是否需要重新上传！\n>>>>>>如需重传，先删除${FTPHOST}上的'*.dat',再删除${ftped_file_list}  !!!"
		echo ">>>>>>最近一次上传了${ftped_dat_cnt}个接口!时间是:${upload_dat_time}"
		return 1
		fi
		
		#生成ftp命令文件
		echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_dat_file}
		echo "lcd ${LOCAL_DIR}" >> ${ftp_mac_put_dat_file}
		echo "bin" >> ${ftp_mac_put_dat_file}
		echo "prompt off" >> ${ftp_mac_put_dat_file}
		echo "mput *.dat" >> ${ftp_mac_put_dat_file}
		#因为未传verf时dat不会移走，所以可以用dir来获得上传列表
		echo "dir *.dat ${ftped_file_list}" >> ${ftp_mac_put_dat_file}
		
		if [ ! -f ${ftp_mac_put_dat_file} ];then 
		echo ">>>>>>ftp macro 文件未生成 !!"
		return 1
		fi

		ftp_mac_put_dat_file_cnt=`wc -l ${ftp_mac_put_dat_file}|awk '{print $1}'`		
		if [ ${ftp_mac_put_dat_file_cnt} -ne 6 ];then 
		echo ">>>>>>ftp macro 写入有误 !!"
		return 1
		fi
				
		
		#上传
		echo ">>>>>>执行verf文件上传>>>>>>>>>>>>>>>>>>>>>>"
		ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}
		#恢复$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>恢复 HOME : ${HOME}"
		
		#打印上传结果
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
		echo ">>>>>>刚刚上传了${ftped_dat_cnt}个接口!时间是:${upload_dat_time} ,日志路径:${ftped_file_list}"
		else 
		echo ">>>>>>FTP dir 命令获取上传文件列表失败！！请检查！！"
		return 1
		fi		
		
		return 0
}

putverffile(){
		#实现verf文件上传
		HOME=/bassapp/bihome/panzw/config
		export HOME
		echo ">>>>>>目标主机 : ${FTPHOST}"
		echo ">>>>>>作业时间 : `date`"		
		echo ">>>>>>PID\$\$ $$"
		echo ">>>>>>修改 HOME : ${HOME}"
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftp_mac_put_verf_file=${HOME}/put_verf.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		#检查前置文件情况
		if [ ! -f ${ftp_mac_put_dat_file} ];then 
		echo ">>>>>>ftp mput macro 文件未生成 !!"
		return 1
		fi
				
		if [ ! -f ${ftped_file_list} ];then 
		echo ">>>>>>未找到dat上传日志文件,请确认dat是否已上传!!"		
		return 1
		else 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		upload_dat_time=`getmtime ${ftped_file_list}`
				if [ ${ftped_dat_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
					echo ">>>>>>dat上传数量不等于${DAY_INTERFACE_CNT},请确认dat是否已正确上传!!"		
					return 1
				fi
		fi
		
		#生成ftp命令文件
		echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_verf_file}
		echo "lcd ${LOCAL_DIR}" >> ${ftp_mac_put_verf_file}
		echo "bin" >> ${ftp_mac_put_verf_file}
		echo "prompt off" >> ${ftp_mac_put_verf_file}
		echo "mput *.verf" >> ${ftp_mac_put_verf_file}
		
		if [ ! -f ${ftp_mac_put_verf_file} ];then 
		echo ">>>>>>ftp macro 文件未生成 !!"
		return 1
		fi

		ftp_mac_put_verf_file_cnt=`wc -l ${ftp_mac_put_verf_file}|awk '{print $1}'`		
		if [ ${ftp_mac_put_verf_file_cnt} -ne 5 ];then 
		echo ">>>>>>ftp macro 写入有误 !!"
		return 1
		fi

		#上传
		echo ">>>>>>执行verf文件上传>>>>>>>>>>>>>>>>>>>>>>"
		ftp -v ${FTPHOST} < ${ftp_mac_put_verf_file}
		#恢复$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>恢复 HOME : ${HOME}"
		
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
echo ">>>>>>数据导出目录不存在！！请检查！！"
exit
fi
ls -lrt ${exp_dir}|grep dat

echo "\n"
echo ">>>>>>PID\$\$ $$"
#4.print datetime & basic info 
echo ">>>>>>today           当前日期 :${today}"
echo ">>>>>>deal_date   处理数据日期 :${deal_date}"

echo ">>>>>>exp_dir     数据导出目录 :  ${exp_dir}"

first_exp_file=`ls -1rt ${exp_dir}/*.dat|head -1`
last_exp_file=`ls -1rt ${exp_dir}/*.dat|tail -1`

echo ">>>>>>first_exp_file  最早生成 :`getmtime ${first_exp_file}`"
echo ">>>>>> last_exp_file  最晚生成 :`getmtime ${last_exp_file}`"


dat_file_cnt=`ls -lrt ${exp_dir}/*.dat | wc -l|awk '{print $1}'`
echo ">>>>>>dat_file_cnt  数据文件数 :${dat_file_cnt}"


if [ ${dat_file_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>dat_file_cnt  数据文件数 不等于 ${DAY_INTERFACE_CNT} ,请先处理！"
exit
fi


verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l|awk '{print $1}'`
echo ">>>>>>verf_file_cnt 校验文件数 :${verf_file_cnt}"


if [ ${verf_file_cnt} -ne ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>verf_file_cnt  校验文件数 不等于 ${DAY_INTERFACE_CNT} ,请先处理！"
exit
fi
#统计空文件数
chkemptyfile


#echo ">>>>>>>>>>>>>>>>>>>>>>>>执行dat文件上传>>>>>>>>>>>>>>>>>>>>>>"
#2. 输入参数检查
if [ $# -eq 0 ];then 
#检查上传时间
chkputtime
	if [ $? -eq 0 ];then 
	#执行上传
		#如返回校验，不应再上传
		test -d ${rpt_dir}
		if [ $? -eq 0 ] ; then 
		echo ">>>>>>已返回文件级校验报告，不能重复上传!!"
		exit
		fi
		 putdatfile
		ret_code=$?
		if [ ${ret_code} -eq 0 ];then 
		sleep 5
		 putverffile
		fi 
	else 
	#echo ">>>>>>由于不是正常上传时段，只做简单检查 !!"
	echo "\n"
	fi
else 
	#强制上传
	if [ $# -eq 1 -a $1 = "force" ];then 
	#执行上传
		#test -d ${rpt_dir}
		if [ $? -eq 0 ] ; then 
		echo ">>>>>>已返回文件级校验报告，不能重复上传!!"
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
###>>>>>>>>>>>>>>>>>>>>>>>>完成dat文件上传>>>>>>>>>>>>>>>>>>>>>>



###>>>>>>>>>>>>>>>>>>>>>>>>查看报告返回情况>>>>>>>>>>>>>>>>>>>>>>

test -d ${rpt_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>>>>>>>>>>>>>>>>>>>开始获取文件、记录级校验>>>>>>>>>>>>>>>>>>>>>>"
echo /bassapp/backapp/bin/bass1_report/bass1_report
echo ">>>>>>>>>>>>>>>>>>>>>>>>结束获取文件、记录级校验>>>>>>>>>>>>>>>>>>>>>>"
		test -d ${rpt_dir}
		if [ $? -eq 1 ] ; then 
		echo ">>>>>>报告返回目录不存在！！请检查！！"
		exit
		fi
fi

echo ">>>>>>rpt_dir     报告返回目录 :  ${rpt_dir}"

#统计文件级返回
test -f ${rpt_dir}/f*
if [ $? -eq 1 ] ; then 
file_rpt_cnt=0
echo ">>>>>>文件级校验尚未返回!!"
else
file_rpt_cnt=`ls -lrt ${rpt_dir}/f* | wc -l|awk '{print $1}'`
fi
echo ">>>>>>file_rpt_cnt文件级返回数 :${file_rpt_cnt}"
#文件级返回统计
if [ ${file_rpt_cnt} -ne ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${file_rpt_cnt}`
echo ">>>>>>文件级校验尚未返回完全!!!还差 ${difference_cnt} 个!!!"
echo ">>>>>>文件级校验应很快返回完全！！否则请检查上传是否遗漏！！"
difference_cnt=""
exit
else
echo ">>>>>>文件级校验返回完全！！"
fi

#统计记录级返回
test -f ${rpt_dir}/r*
if [ $? -eq 1 ] ; then 
record_rpt_cnt=0
echo ">>>>>>记录级校验尚未返回!!"
else 
record_rpt_cnt=`ls -lrt ${rpt_dir}/r* | wc -l|awk '{print $1}'`
fi
echo ">>>>>>record_rpt_cnt记录级返回 :${record_rpt_cnt}"
#统计记录级返回
if [ ${record_rpt_cnt} -gt 0 -a  ${record_rpt_cnt} -lt ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${record_rpt_cnt}`
echo ">>>>>>记录级校验尚未返回完全!!!还差 ${difference_cnt} 个!!!"
fi

if [ ${record_rpt_cnt} -eq ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>记录级校验返回完全！！"
echo "\n"
fi

###>>>>>>>>>>>>>>>>>>>>>>>>结束查看报告返回情况>>>>>>>>>>>>>>>>>>>>>>
