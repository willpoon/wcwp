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

today=`date '+%Y%m%d'`
echo "===today    :${today}"
deal_date=`yesterday ${today}`
echo "===process  :${deal_date}"
exp_dir=/bassapp/backapp/data/bass1/export/export_${deal_date}
echo "===work path:  /bassapp/backapp/data/bass1/export/export_${deal_date}"
dat_file_cnt=`ls -lrt ${exp_dir}/*.dat | wc -l`
echo "===dat   cnt:${dat_file_cnt}"
verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l`
echo "===verf  cnt:${verf_file_cnt}"
