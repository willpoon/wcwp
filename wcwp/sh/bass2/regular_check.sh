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


#

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

echo ">>>>>>today           当前日期 :${today}"
echo ">>>>>>deal_date   处理数据日期 :${deal_date}"

test -d ${exp_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>数据导出目录不存在！！请检查！！"
exit
fi

echo ">>>>>>exp_dir     数据导出目录 :  /bassapp/backapp/data/bass1/export/export_${deal_date}"


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
fi

