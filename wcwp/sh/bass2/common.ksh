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

#得到某月最后一天
getlastday()
{
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`

        month=`expr $month + 0`

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

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

#子字符串替换函数
ReplaceAllSubStr()
{
	echo $1 > sed$$.temp
	sReturnString=`sed 's/'$2'/'$3'/g' sed$$.temp`
	rm sed$$.temp
	echo $sReturnString
	return 1
}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
echo `date` >> /bassdb1/etl/L/boss/db2_connect.log
echo $DB2_SQLCOMM >> /bassdb1/etl/L/boss/db2_connect.log

conn_cnt=0
while [ $conn_cnt -lt 3 ] 
do
	db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
	if [ $? != 0 ] ; then
		echo "数据库连接失败！" >> /bassdb1/etl/L/boss/db2_connect.log
		conn_cnt=`expr ${conn_cnt} + 1`
		echo "conn_cnt=${conn_cnt}" >> /bassdb1/etl/L/boss/db2_connect.log
	else
		echo "数据库连接成功！" >> /bassdb1/etl/L/boss/db2_connect.log
		break
	fi
done
eval $DB2_SQLCOMM

db2 commit
db2 terminate
}

#生成加载结果文件
build_result_file()
{
    file_name=$1
    flag=`echo "$file_name"|cut -c1-1`
    
    if [ $flag = "M" ] ; then
        file_date=`echo "$file_name"|cut -c7-12`
    else
        file_date=`echo "$file_name"|cut -c7-14`
    fi 
    
    if [ -f ${WORK_PATH}/log/${file_date}.log ] ; then
         echo "$1,$2" >> ${WORK_PATH}/log/${file_date}.log
    else 
         echo "$1,$2" >  ${WORK_PATH}/log/${file_date}.log
    fi
}


##!/usr/bin/ksh
getxlsdata(){
				if [ $# -ne 2 ];then
				echo getxlsdata [inputfile] [yyyymm]
				return 2
				fi
				inputfile=$1
				tmp_voice_outputfile=fmt_voice_${data_mon}.tmp
				tmp_sms_outputfile=fmt_sms_${data_mon}.tmp
				voice_outputfile=load_voice_${data_mon}.txt
				sms_outputfile=load_sms_${data_mon}.txt
				data_mon=$2
				#step1:copy 28*p  data
				#step2:format data 0.000,save as voice_yyyymm.csv
				#step3:upload to unix
				#step4:run the following program!
				#step5:re-paste -d "," to excel (do some checksum)
				#step6:load data into tmp-table
				#step7:insert data into g_s_05001_month/g_s_05002_month
				#voice
				nawk 'BEGIN{FS=",";OFS=","}{print $1,$2,$4*1000,$3*1000}'  $inputfile >$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $5,$6,$8*1000,$7*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $9,$10,$12*1000,$11*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6,$7}' dim.txt > dim_voice.tmp
				paste -d "," dim_voice.tmp $tmp_voice_outputfile > $voice_outputfile
				#sms
				nawk 'BEGIN{FS=",";OFS=","}{if($14 != 0){print $14,$13,$16*1000,$15*1000}}'  $inputfile >$tmp_sms_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6}' dim_sms.txt > dim_sms.tmp
				paste -d "," dim_sms.tmp $tmp_sms_outputfile > $sms_outputfile
				#rm tmp files
				rm *.tmp
				#print result
				cat $voice_outputfile
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				cat $sms_outputfile
				db2 terminate
				db2 connect to BASSDB56 user bass2 using bass2
				echo db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				echo db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 connect reset
				db2 terminate
}



cdz(){
#compare data size
#run at /bassapp/bihome/panzw
#no parameter needed
today=`date '+%Y%m%d'`
deal_date=`yesterday ${today}`
prev_date=`yesterday ${deal_date}`
prev_data_path=/bassapp/backapp/data/bass1/export/export_${prev_date}
this_data_path=/bassapp/backapp/data/bass1/export/export_${deal_date}
ls -lrt ${this_data_path}/*.dat  | \
awk '{print $9,$5}'|\
awk 'BEGIN{FS="_";OFS="_"}{print $5,$6,$7}' |\
sort > /bassapp/bihome/panzw/tmp/.tmp1$$
#
ls -lrt ${prev_data_path}/*.dat  | \
awk '{print $9,$5}'|\
awk 'BEGIN{FS="_";OFS="_"}{print $5,$6,$7}' |\
sort > /bassapp/bihome/panzw/tmp/.tmp2$$
#-v deal_date=${deal_date} -v prev_date=${prev_date}
echo |nawk -v deal_date=${deal_date} -v prev_date=${prev_date} '{printf "%-30s%-20s%-20s\n", "filename","date:"deal_date,"date:"prev_date}'
paste -d " " /bassapp/bihome/panzw/tmp/.tmp1$$ /bassapp/bihome/panzw/tmp/.tmp2$$|awk '{printf "%-30s%-20s%-20s\n", $1,$2,$4}'
wc -l /bassapp/bihome/panzw/tmp/.tmp1$$
wc -l /bassapp/bihome/panzw/tmp/.tmp2$$
rm /bassapp/bihome/panzw/tmp/.tmp1$$
rm /bassapp/bihome/panzw/tmp/.tmp2$$
}



cds(){
#alias of cdz2	
#compare data size
#run at /bassapp/bihome/panzw
#no parameter needed
today=`date '+%Y%m%d'`
deal_date=`yesterday ${today}`
prev_date=`yesterday ${deal_date}`
prev_data_path=/bassapp/backapp/data/bass1/export/export_${prev_date}
this_data_path=/bassapp/backapp/data/bass1/export/export_${deal_date}
ls -lrt ${this_data_path}/*.dat  | \
awk '{print $9,$5}'|\
awk 'BEGIN{FS="/"}{print $8}' |\
awk 'BEGIN{FS="_";OFS="_"}{print $4,$3,$1,$6}' |\
sort > /bassapp/bihome/panzw/tmp/.tmp1$$

ls -lrt ${prev_data_path}/*.dat  | \
awk '{print $9,$5}'|\
awk 'BEGIN{FS="/"}{print $8}' |\
awk 'BEGIN{FS="_";OFS="_"}{print $4,$3,$1,$6}' |\
sort > /bassapp/bihome/panzw/tmp/.tmp2$$
echo |nawk -v deal_date=${deal_date} -v prev_date=${prev_date} '{printf "%-30s%-20s%-20s\n", "filename","date:"deal_date,"date:"prev_date}'
paste -d " " /bassapp/bihome/panzw/tmp/.tmp1$$ /bassapp/bihome/panzw/tmp/.tmp2$$ |\
awk '{printf "%-30s%-20s%-20s\n", $1,$2,$4}'
wc -l /bassapp/bihome/panzw/tmp/.tmp1$$
wc -l /bassapp/bihome/panzw/tmp/.tmp2$$
#
rm /bassapp/bihome/panzw/tmp/.tmp1$$
rm /bassapp/bihome/panzw/tmp/.tmp2$$
}



splfile(){
#function:split large file
#author:panzhiwei
#run:splfile [filename_nosuf] [linecount]
#example:splfile s_13100_201102_21003_00_001 7500000
if [ $# -ne 2 ];then 
echo splfile [filename] [linecount]
echo example:splfile s_13100_201102_21003_00_001 7500000
return 2
fi
#接口文件名作为参数(不带后缀)
filename=$1
linecount=$2
#去掉最后的顺序号
file_name_=`echo ${filename} |awk '{print substr($1,1,length($1)-1)}'`
#backup
if [ -f ${filename}.dat ];then 
echo ...backup ${filename}.dat to ${filename}.bak...
mv ${filename}.dat ${filename}.bak
else
echo no such file : ${filename}.dat
return 2
fi
#分割
echo ...spliting...
split -$linecount ${filename}.bak
echo ...split complete!
seq_no=1
#rename 
for split_unit in `ls xa*`
	do
	echo ...rename $split_unit to $file_name_$seq_no.dat
	mv $split_unit $file_name_$seq_no.dat
	seq_no=`expr $seq_no + 1`
	if [ $seq_no -gt 9 ];then
		echo ...is the file that large? check it!
		return 2
	fi
done
ls -lrt $file_name_*dat
echo ...all  complete!
}



getlist(){
if [ $# -ne 1 ];then
echo $0 yyyymm
return
fi
data_time=$1
data_path="/bassapp/backapp/data/bass1/export/export_${data_time}"
echo >/bassapp/bihome/panzw/tmp/export_cmp_${data_time}.txt
echo >/bassapp/bihome/panzw/tmp/getlist_log.txt
while read line
do
unit_code=`echo  "$line"|nawk -F"_" '{print $4}'`
grep $unit_code /bassapp/bihome/panzw/tmp/export_cmp_${data_time}.txt >/dev/null 2>&1
ret=$?
if [ $ret -eq 0 ];then
echo "`date` $unit_code already in the list!" >>/bassapp/bihome/panzw/tmp/getlist_log.txt
else 
echo $line|awk 'BEGIN{FS="_";OFS="_"}{print $4" "$4,$3,$1,$6}'  >>/bassapp/bihome/panzw/tmp/export_cmp_${data_time}.txt
fi
done<<!
`ls -lt ${data_path}/*.dat|awk '{print $9,$5}' |nawk -F"/" '{print $NF}'|sort`
!

cat /bassapp/bihome/panzw/tmp/export_cmp_${data_time}.txt|sort 
cat /bassapp/bihome/panzw/tmp/getlist_log.txt
rm /bassapp/bihome/panzw/tmp/export_cmp_${data_time}.txt
}


cms(){
if [ $# -ne 2 ];then
echo $0 this_data_month pre_data_month
return
fi
this_data_month=$1
pre_data_month=$2
join_file1=/bassapp/bihome/panzw/tmp/j1.txt
join_file2=/bassapp/bihome/panzw/tmp/j2.txt
getlist ${this_data_month} |grep dat>${join_file1}
getlist ${pre_data_month}  |grep dat>${join_file2}
join -a 1 -o 1.2 1.3 2.3 ${join_file1} ${join_file2} |\
awk '{printf "%-30s%-20s%-20s\n", $1,$2,$3}'
return 0
}
