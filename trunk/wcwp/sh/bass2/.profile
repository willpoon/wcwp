#	This is the default standard profile provided to a user.
#	They are expected to edit it to meet their own needs.

MAIL=/usr/mail/${LOGNAME:?}

# The following three lines have been added by UDB DB2.
if [ -f /db2home/db2inst1/sqllib/db2profile ]; then
    . /db2home/db2inst1/sqllib/db2profile
fi

stty erase '^H'

# Config Tcl env
DATABASE=DB2
DB_USER=BASS1
AIOMNIVISION=/bassapp/tcl/aiomnivision
HOME=/bassapp/bass1
PATH=$PATH:$AIOMNIVISION/aitools/bin:$HOME/tcl
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$AIOMNIVISION/aitools/lib
AITOOLSPATH=$AIOMNIVISION/aitools
BASS1TRACEDIR=$HOME/trace
BASS1LOGDIR=$HOME/log
BASS1FILEDIR=$HOME/data
BASS1DIR=$HOME

export DATABASE DB_USER AIOMNIVISION PATH LD_LIBRARY_PATH AITOOLSPATH BASS1FILEDIR BASS1DIR BASS1LOGDIR BASS1TRACEDIR

#############################################
#########  TOP  Software  ###################
if [ -d /usr/local/bin ]
then
        PATH=${PATH}:/usr/local/bin
fi
#############################################

LANG=zh_CN.GBK
export LANG
#let system prompt in english
#LANG=C 
#############################################
alias l1='ls -1'
alias ll='ls -l'
alias lt='ls -lrt'
alias pzh='cd /bassapp/bihome/panzw'
alias cdrpt='cd /bassapp/backapp/data/bass1/report'
alias cdexp='cd /bassapp/backapp/data/bass1/export'
alias cdtcl='cd /bassapp/bass1/tcl'
alias cdtcl2='cd /bassapp/bass2/tcl'
alias runrpt='cd /bassapp/backapp/bin/bass1_report'
alias runlog='cd /bassapp/bihome/panzw/tmp/tclrunlog'
nowpts=`ps |awk '{print $2}' |grep pts|uniq`
USER=`who |grep $nowpts|awk '{print $1}'`
export USER
echo $USER



ftp25(){
HOME=/bassapp/bihome/panzw/config
export HOME
ftp -v 172.16.9.25
HOME=/bassapp/bass1
export HOME
}

ftp130(){
HOME=/bassapp/bihome/panzw/config
export HOME
ftp -v 172.16.5.130
HOME=/bassapp/bass1
export HOME
}


ifconn(){
db2 values 1 > /dev/null
if [ $? -ne 0 ];then
conn > /dev/null
echo now connected
else
echo connected
fi
}


term(){
db2 terminate > /dev/null
}

desc(){
db2 describe table  $1
}

conn2(){
term
#my_pass=`${base_dir}/decode  0312004131`
my_pass=bass2
db2 connect to BASSDB user bass2 using ${my_pass} > /dev/null
}

conn1(){
term
db2 connect to BASSDB user bass1 using bass1 > /dev/null
}

gb(){
target_table=$1
dimension_field=$2
echo "\n"
echo "select ${dimension_field} , count(0) \n--,  count(distinct ${dimension_field} ) \r
from ${target_table} \r
group by  ${dimension_field} \r
order by 1 "
echo "\n"
}


desf1(){
ifconn
desc $1|\
awk -F" " '{if( NR >= 6 ) {printf "\t%s\n" ,","$1} else {printf "\t%s\n" ," "$1}}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
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


desf(){
ifconn
desc $1|\
awk -F" " '{printf "\t%-20s\t%-20s\n" ,$1,$3"("$4")"}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
}

dest(){
ifconn
desc $1|\
nawk -F" " '{ if( $3 ~ /CHAR/ )
{
 printf "\t%-20s\t%-20s\n" ,","$1,$3"("$4")";
}
else
if( $3 ~ /DECIMAL/ )
{printf "\t%-20s\t%-20s\n" ,","$1,$3"("$4","$5")";}
else
{ printf "\t%-20s\t%-20s\n" ,","$1,$3; } }'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
}

desf1(){
ifconn
desc $1|\
awk -F" " '{if( NR >= 6 ) {printf "\t%s\n" ,","$1} else {printf "\t%s\n" ," "$1}}'|\
sed -e '1,4d'|sed -e '$d'|sed -e '$d'|sed -e '$d'
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
echo |nawk -v deal_date=${deal_date} -v prev_date=${prev_date} '{printf "%-30s%-20s%-20s%-10s\n" \
,"filename","date:"deal_date,"date:"prev_date,"WAVE"}'

paste -d " " /bassapp/bihome/panzw/tmp/.tmp1$$ /bassapp/bihome/panzw/tmp/.tmp2$$ |\
awk '{
	if( int($4) > 0 && int($2) > 0 ) printf "%-30s%-20s%-20s% -10.2f%%\n", $1,$2,$4,(int($2)-int($4))*1.00/int($4)*100
	else 
	if ( int($4) == 0 && int($4) == 0 ) printf "%-30s%-20s%-20s% -10dBOTH0\n", $1,$2,$4,(int($2)-int($4))
	else
	if ( int($4) == 0 ) printf "%-30s%-20s%-20s% -10dPREV0\n", $1,$2,$4,(int($2)-int($4))
	else 
	if ( int($2) == 0 ) printf "%-30s%-20s%-20s% -10dTHIS0\n", $1,$2,$4,(int($2)-int($4))
	else printf "%-30s%-20s%-20s% -10.2f%%\n", $1,$2,$4,(int($2)-int($4))*1.00/int($4)*100
	
}'
wc -l /bassapp/bihome/panzw/tmp/.tmp1$$
wc -l /bassapp/bihome/panzw/tmp/.tmp2$$
#
rm /bassapp/bihome/panzw/tmp/.tmp1$$
rm /bassapp/bihome/panzw/tmp/.tmp2$$
}

ints(){
date +%Y%m%d%H%M%S
logfile="/bassapp/bihome/panzw/tmp/tclrunlog/$1.`date +%Y%m%d%H%M%S`"
time int -s $1 |tee $logfile
date +%Y%m%d%H%M%S
}


looktb(){
if [ $# -ne 2 ];then 
echo "$0 <skm> <tb>"
return 1
fi
case $USER in
app)  PWD=app;;
bass1)  PWD=bass1;;
bass2)  PWD=bass2;;
panzw)  USER=bass1;export USER;PWD=bass1;;
esac
db2look -d bassdb -e -i $USER -w $PWD -z $1 -t $2
}


d_at09_cnt=8
d_at11_cnt=32
d_at13_cnt=15
d_at15_cnt=6
d_all_cnt=61
#
m_on03_cnt=8
m_on05_cnt=19
m_on08_cnt=30
m_on10_cnt=17
m_on15_cnt=10
m_all_cnt=84

#subroutine for cms  cmd
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


getxlsdata(){
				if [ $# -ne 2 ];then
				echo "getxlsdata [inputfile] [yyyymm]															"
				echo "step1:copy 28*p (实际 1-28,a-p) data,to ue,-》csv           "
				echo "step2:format data 0.000,save as voice_yyyymm.csv            "
				echo "step3:upload to unix /bassapp/bihome/panzw/tmp              "
				echo "step4:run the following program!                            "
				echo "step5:re-paste -d "," to excel (do some checksum)           "
				echo "step6:load data into tmp-table                              "
				echo "step7:insert data into g_s_05001_month/g_s_05002_month      "
				echo "voice                                                       "
				echo "r1 change dir to /bassapp/bihome/panzw/tmp                  "
				echo "r2 from 去话时长 to 应收金额								                "
				return 2
				fi
				inputfile=$1
				data_mon=$2
				tmp_voice_outputfile=fmt_voice_${data_mon}.tmp
				tmp_sms_outputfile=fmt_sms_${data_mon}.tmp
				voice_outputfile=load_voice_${data_mon}.txt
				sms_outputfile=load_sms_${data_mon}.txt
				#step1:copy 28*p (实际 1-28,a-p) data,to ue,->csv
				#step2:format data 0.000,save as voice_yyyymm.csv
				#step3:upload to unix /bassapp/bihome/panzw/tmp
				#step4:run the following program!
				#step5:re-paste -d "," to excel (do some checksum)
				#step6:load data into tmp-table
				#step7:insert data into g_s_05001_month/g_s_05002_month
				#voice
				#r1 change dir to /bassapp/bihome/panzw/tmp
				#r2 from 去话时长 to 应收金额				
				nawk 'BEGIN{FS=",";OFS=","}{printf "%s,%s,%15d,%15d\n", $1,$2,$4*1000,$3*1000}'  $inputfile >$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{printf "%s,%s,%15d,%15d\n", $5,$6,$8*1000,$7*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{printf "%s,%s,%15d,%15d\n", $9,$10,$12*1000,$11*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6,$7}' dim.txt > dim_voice.tmp
				paste -d "," dim_voice.tmp $tmp_voice_outputfile > $voice_outputfile
				#sms
				nawk 'BEGIN{FS=",";OFS=","}{if($14 != 0){printf "%s,%s,%15d,%15d\n", $14,$13,$16*1000,$15*1000}}'  $inputfile >$tmp_sms_outputfile
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
				grep ",,,"  $sms_outputfile>/dev/null
				if [ $? -eq 0 ];then 
				echo "generate data file with errors!"
				return 1
				fi
				db2 terminate
				db2 connect to BASSDB user bass2 using bass2
				echo db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bihome/panzw/tmp/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				db2 "load client from /bassapp/bihome/panzw/tmp/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				echo db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bihome/panzw/tmp/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 "load client from /bassapp/bihome/panzw/tmp/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 connect reset
				db2 terminate
}

###########################
#fmt:将excel字段 格式化成文本代码
###########################
fmttbs(){
if [ $# -ne 0 ];then
	echo "fmt fmt.txt"
	return
fi
nawk -F"\t" '{ printf "\t,%-20s\t%-20s--%-20s\n", $1,$2,$3 }' fmt.txt
}

###########################
#fmt:将excel字段 格式化成文本代码
###########################
tbsu(){
	today=`date '+%Y%m%d'`
	deal_date=`yesterday ${today}`
	logFile09="/bassapp/bass2/check_db/check_bassdb_day_${today}09.log"
	logFile17="/bassapp/bass2/check_db/check_bassdb_day_${today}17.log"
	if [ -f ${logFile09} -o -f ${logFile17} ];then
		cat ${logFile09} ${logFile17} |grep -i bass1|grep NORMAL
	else
		logFile09="/bassapp/bass2/check_db/check_bassdb_day_${deal_date}09.log"
		logFile17="/bassapp/bass2/check_db/check_bassdb_day_${deal_date}17.log"
		cat ${logFile09} ${logFile17} |grep -i bass1|grep NORMAL
	fi
}

goexp2(){
	today=`date '+%Y%m%d'`
	deal_date=`yesterday ${today}`
	exp_dir="/bassapp/backapp/data/bass1/export/export_${deal_date}"
	cd ${exp_dir}
}

day9(){
	cd ${exp_dir}
	ls -arlth \
	*01002*dat \
	*01004*dat \
	*02004*dat \
	*02008*dat \
	*02011*dat \
	*02053*dat \
	*06031*dat \
	*06032*dat
	cd $HOME
}

###########################
#viewlog:find a log segment for  a large logfile
###########################

viewlog(){
	
	if [ $# -ne 3 ];then
		echo "usage:viewlog Mode[g/a] logFile Arg3[ContentPatern/FileNum]"
		return 1
	fi

	mode=$1
	logFile=$2
	if [ ! -f $logFile ]; then
		echo $2 not exist!
		return 1
	fi	
	##Arg3 can be ContentPatern or FileNum
	Arg3=$3 

	if [ $mode = "g" ];then
		echo "grep -in $Arg3 $logFile |more "
		grep -in $Arg3 $logFile |more 
		return 0
	else
	##http://www.unix.com/shell-programming-scripting/8820-check-if-argument-passed-integers.html
		echo $Arg3 | egrep '^(\+|-)?[0-9]+$'
		retcode=$?
		if [ $retcode -ne 0 ];then
			echo "$3 is invalid !"
			return 1
		fi	
		echo "nawk -v vFileNum=$Arg3 '{ if ( FNR > $vFileNum ) print }' $logFile|more"
		nawk -v vFileNum=$Arg3 '{ if ( FNR >= vFileNum ) print FNR,$0 }' $logFile|more
		return 0
	fi

}