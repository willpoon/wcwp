#~ nohup sh /bassapp/bihome/panzw/tmp/06035/bass1_upload_interface.sh > /bassapp/bihome/panzw/tmp/06035/sh.out 2>&1 &

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
LOCAL_DIR=/bassapp/bihome/panzw/tmp/06035
HOME=/bassapp/bihome/panzw/config
export HOME

ftp_mac_file=/bassapp/bihome/panzw/tmp/put_verf.mac.ftp


#生成ftp命令文件
echo "cd ${REMOTE_DIR}" > ${ftp_mac_file}
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


for dt in  20120610	\
20120611	20120612	20120613	20120614	20120615	20120616	\
20120617	20120618	20120619	20120620
do
	while [ true ]
	do
	yestoday=`yesterday ${dt}`
	cat /bassapp/backapp/data/bass1/report/report_${yestoday}/r*06035_01*verf|grep "${yestoday}.*06035.*00000000" 
	ret=$?
	if [ $ret -eq 0 ];then
		echo now next: ${dt} ...
		if [ -f /bassapp/backapp/data/bass1/report/report_${dt}/f*06035_01*verf ];then
		echo ${dt} have put!
		else 
		putinterface ${dt} 06035
		fi
		break
	else
	echo ${yestoday} not return 
	/bassapp/backapp/bin/bass1_report/bass1_report>/dev/null 2>&1
	fi
	sleep 10
	done
done