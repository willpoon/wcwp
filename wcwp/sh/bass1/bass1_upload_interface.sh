#~ functions:
#~ 1.从设定的起始日期开始，自动上传后续日期，重复调用程序可以自动跳过已上传的日期。
#~ notes:
#~ 1.替换日期区间
#~ 2.替换接口编号
#~ 3.辅助脚本： find /bassapp/backapp/data/bass1/export -name  "*20120[6]*06035_01*" -exec cp {} /bassapp/bihome/panzw/tmp/06035 \;

if [ $# -ne 3 ];then
	echo $0 interface_code batch_no start_date
	exit
fi
interface_code=$1
batch_no=$2
start_date=$3

#~ nohup sh /bassapp/bihome/panzw/tmp/${interface_code}/bass1_upload_interface.sh \
#~ > /bassapp/bihome/panzw/tmp/${interface_code}/sh.out 2>&1 &

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

putinterface(){
if [ $# -ne 2 ];then
	echo take 2 parameters
return 1
fi
time_id=$1
interface=$2
FTPHOST=172.16.5.130
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw/tmp/${interface_code}
HOME=/bassapp/bihome/panzw/config
export HOME

ftp_mac_file=/bassapp/bihome/panzw/tmp/put_verf.mac.ftp


#生成ftp命令文件
echo "cd ${REMOTE_DIR}" > ${ftp_mac_file}
echo "lcd ${LOCAL_DIR}" >> ${ftp_mac_file}
echo "bin" >> ${ftp_mac_file}
echo "prompt off" >> ${ftp_mac_file}
echo "put *${time_id}*${interface}*dat" >> ${ftp_mac_file}
echo "put *${time_id}*${interface}*verf" >> ${ftp_mac_file}
cat ${ftp_mac_file}
ftp -v ${FTPHOST} <  ${ftp_mac_file}
#恢复$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}


#~ begin:


for dt in  \
20120701	20120702	20120703	20120704	20120705	\
20120706	20120707	20120708	20120709	20120710	\
20120711	20120712	20120713	20120714	20120715	\
20120716	20120717	20120718	20120719	20120720	\
20120721	20120722	20120723	20120724	20120725	\
20120726	20120727	20120728	20120729	20120730	\
20120731
do

	#~ upload the first date of a series interfaces
	
	if [ $dt -eq $start_date ];then	
		if [ -f /bassapp/backapp/data/bass1/report/report_${dt}/f*${interface_code}_${batch_no}*verf ];then
		echo ${dt} have put!
		else 
		putinterface ${dt} ${interface_code}
		fi
	fi

	while [ true ]
	do
	yestoday=`yesterday ${dt}`
	
	
	if [ $yestoday -lt $start_date ];then
		break
	fi
	
	cat /bassapp/backapp/data/bass1/report/report_${yestoday}/r*${interface_code}_${batch_no}*verf 2>/dev/null|grep "${yestoday}.*${interface_code}.*00000000" 
	ret=$?
	if [ $ret -eq 0 ];then
		echo now next: ${dt} ...
		if [ -f /bassapp/backapp/data/bass1/report/report_${dt}/f*${interface_code}_${batch_no}*verf ];then
		echo ${dt} have put!
		else 
		putinterface ${dt} ${interface_code}
		fi
		break
	else
		echo ${yestoday} not return 
		/bassapp/backapp/bin/bass1_report/bass1_report>/dev/null 2>&1
	fi
	sleep 10
	done
done
