#!/bin/sh
#自动同步BOSS接口
#求取昨天的日期
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

	today=`date '+%Y%m%d'`
        sday=`yesterday $today`


cd /bassdb2/etl/L/boss/backup
        #分发月接口
        ls M*.END M*.AVL.Z|cut -c7-12|sort -u|awk '{print "mkdir "$1"01;mv M*"$1"*.* "$1"01"}'|sh
        #分发日接口
        ls *.END *.AVL.Z|cut -c7-14|sort -u|awk '{print "mkdir "$1";mv *"$1"*.* "$1""}'|sh
	
	touch ${sday}.tar.success

	#打包数据
	tar -cvf ${sday}.tar ./${sday}
	
				ftp -n 172.16.5.43 <<EOF
					prompt
					user load load
					bin
					cd /bassdb1/etl/L/boss/backup
					lcd /bassdb2/etl/L/boss/backup
					put ${sday}.tar
					put ${sday}.tar.success
					bye
EOF
	rm ${sday}.tar
	rm ${sday}.tar.success



