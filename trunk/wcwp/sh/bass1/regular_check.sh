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
	echo ">>>>>>脚本存在不安全因素！！"
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
echo ">>>>>>today           当前日期 :${today}"
echo ">>>>>>deal_date   处理数据日期 :${deal_date}"

test -d ${exp_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>数据导出目录不存在！！请检查！！"
exit
fi

echo ">>>>>>exp_dir     数据导出目录 :  /bassapp/backapp/data/bass1/export/export_${deal_date}"

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



test -d ${rpt_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>报告返回目录不存在！！请检查！！"
exit
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

if [ ${record_rpt_cnt} -gt 0 -a  ${record_rpt_cnt} -lt ${DAY_INTERFACE_CNT}  ] ; then 
difference_cnt=`expr ${DAY_INTERFACE_CNT} - ${record_rpt_cnt}`
echo ">>>>>>记录级校验尚未返回完全!!!还差 ${difference_cnt} 个!!!"
fi

if [ ${record_rpt_cnt} -eq ${DAY_INTERFACE_CNT} ];then 
echo ">>>>>>记录级校验返回完全！！"
echo "\n"
fi

putdatfile(){
		FTPHOST=172.16.9.25
		REMOTE_DIR=/bassapp/bass2/panzw2/data
		LOCAL_DIR=${exp_dir}
		HOME=/bassapp/bihome/panzw
		export HOME
		echo ">>>>>>目标主机 : ${FTPHOST}"
		echo ">>>>>>作业时间 : `date`"		
		echo ">>>>>>HOME : ${HOME}"
		ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
		ftped_file_list=${HOME}/bass1_put_log/ftped_dat_day_${deal_date}.lst
		
		if [ -f ${ftped_file_list} ];then 
		ftped_dat_cnt=`wc -l ${ftped_file_list}|awk '{print $1}'`
		tcltimestamp=`echo "puts [file mtime ${ftped_file_list}]"|tclsh`
		export tcltimestamp
		upload_dat_time=`perl -MPOSIX -e '$x=$ENV{"tcltimestamp"}; @y=strftime("%Y-%m-%d %H:%M:%S", localtime($x)); print "@y\n"'`
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
		
		
		#上传
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
		echo ">>>>>>刚刚上传了${ftped_dat_cnt}个接口!时间是:${upload_dat_time} ,日志路径:${ftped_file_list}"
		else "dir取得上传文件列表失败！！请检查！！"
		fi		
		
		#fi
		#与本地校验：文件数|文件名|文件大小
		#恢复$HOME
		HOME=/bassapp/bass1
		export HOME
		echo ">>>>>>HOME : ${HOME}"
		echo ">>>>>>PID\$\$ $$"
}

echo ">>>>>>PID\$\$ $$"

echo ">>>>>>>>>>>>>>>>>>>>>>>>执行dat文件上传>>>>>>>>>>>>>>>>>>>>>>"

putdatfile
