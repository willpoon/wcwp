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
	echo ">>>>>>�ű����ڲ���ȫ���أ���"
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

echo ">>>>>>today           ��ǰ���� :${today}"
echo ">>>>>>deal_date   ������������ :${deal_date}"

test -d ${exp_dir}
if [ $? -eq 1 ] ; then 
echo ">>>>>>���ݵ���Ŀ¼�����ڣ������飡��"
exit
fi

echo ">>>>>>exp_dir     ���ݵ���Ŀ¼ :  /bassapp/backapp/data/bass1/export/export_${deal_date}"


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
fi

