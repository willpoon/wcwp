
splfile(){
#function:split large file
#author:panzhiwei
#run:splfile [filename] [linecount]
#example:splfile s_13100_201102_21003_00_001 7500000
if [ $# -ne 2 ];then 
echo splfile [filename] [linecount]
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


###################################################################################
export PATH=$PATH:/bassapp/bass2/panzw2/bin
base_dir=/bassapp/bass2/panzw2
alias lt='ls -alrt'
alias llog='$base_dir/ViewLoadLog_testdb.sh'
alias conn='db2 terminate;db2 connect to bassdb'
#alias term='db2 terminate'
alias tf='tail -f nohup.out'
alias gol='cd /bassapp/bass2/load/boss'
alias goif='cd /bassapp/bass2/ifboss'
alias vipr='vi /bassapp/bass2/panzw2/.profile'
alias pzh='cd /bassapp/bass2/panzw2'
#alias desc='db2 describe table '
alias gocrm='cd /bassapp/bass2/ifboss/crm_interface/bin/config/CRM'
alias gocfg='cd /bassapp/bass2/ifboss/crm_interface/bin/config/'
LANG=zh_CN.GBK
export LANG
#       This is the default standard profile provided to a user.
#       They are expected to edit it to meet their own needs.

MAIL=/usr/mail/${LOGNAME:?}
PATH=$PATH:/usr/local/bin:/usr/ccs/bin/:.
export PATH

# The following three lines have been added by UDB DB2.
if [ -f /db2home/db2inst1/sqllib/db2profile ]; then
    . /db2home/db2inst1/sqllib/db2profile
fi
stty erase '^H'

# Config Tcl env
DATABASE=db2
DB_USER=BASS2
AIOMNIVISION=/bassapp/tcl/aiomnivision
PATH=$PATH:$AIOMNIVISION/aitools/bin:.
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$AIOMNIVISION/aitools/lib
AITOOLSPATH=$AIOMNIVISION/aitools
AGENTTRACEDIR=$HOME/trace
AGENTLOGDIR=$HOME/log

CLASSPATH=/bassapp/bass2/ifboss/crm_interface/lib/ojdbc14.jar

export DATABASE DB_USER AIOMNIVISION PATH LD_LIBRARY_PATH AITOOLSPATH AGENTTRACEDIR AGENTLOGDIR CLASSPATH

#表空间环境变量设置
TBS_VOICE=TBS_CDR_VOICE
TBS_DATA=TBS_CDR_DATA
TBS_3H=TBS_3H
TBS_BILL=TBS_BILL
TBS_ODS_OTHER=TBS_ODS_OTHER
TBS_DIM=TBS_DIM
TBS_INDEX=TBS_INDEX
TBS_APP_OTHER=TBS_APP_OTHER
TBS_PSO=TBS_PSO
TBS_USER_TEMP=TBS_USER_TEMP
TBS_WEB=TBS_BASS_WEB
TBS_BASS_MINER=TBS_BASS_MINER
TBS_ST=TBS_ST
TBS_CDR_OTHER=TBS_CDR_DATA

export TBS_CDR_OTHER TBS_VOICE TBS_DATA TBS_3H TBS_BILL TBS_ODS_OTHER TBS_DIM TBS_INDEX TBS_APP_OTHER TBS_PSO TBS_USER_TEMP TBS_WEB 
TBS_BASS_MINER TBS_ST

alias ll='ls -l'
alias conn='db2 terminate;db2 connect to bassdb user bass2 using bass2'
#############################################
#########  TOP  Software  ###################
if [ -d /usr/local/bin ]
then
        PATH=${PATH}:/usr/local/bin
fi
#############################################


kpi=/bassapp/bass2/tcl/KPI/BIPlatform/appdw
export kpi

LANG=zh_CN.GBK
export LANG

USER=`who am i|awk '{print $1}'`
export USER
EDITOR=vi;export EDITOR

#add for java crm_interface
CLASSPATH=.:/bassapp/bass2/ifboss/crm_interface/lib/ojdbc14.jar
export CLASSPATH PROPERTY_PATH
###################################################################################

ifconn(){
db2 values 1 > /dev/null
if [ $? -ne 0 ];then
conn > /dev/null
echo now connected
else 
echo connected
fi
}

conn(){
term 
my_pass=`${base_dir}/decode  0312004131`
db2 connect to BASSDB user bass2 using ${my_pass} > /dev/null
}

conn56(){
term
my_pass=`${base_dir}/decode  0312004131`
db2 connect to BASSDB56 user bass2 using ${my_pass} > /dev/null
}

term(){
db2 terminate > /dev/null
}

desc(){
db2 describe table  $1
}
descf() { db2 describe table $1 | awk -F" " '{print $1}'
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


if [ `echo $0` != "ksh" ] ; then 
ksh
fi

if [ `echo $0` = "ksh" -o  `echo $0` = "bash"  ];then 
set -o vi
fi

#########
#javaetl="java ETLMain 20101202 taskList_tmp_pzw.properties"


gb(){
target_table=$1
dimension_field=$2
echo "\n"
echo "select ${dimension_field} , count(0) , count(distinct ${dimension_field} ) \r
from ${target_table} \r
group by  ${dimension_field} \r
order by 1 "
echo "\n"
}

###############################################################
resset(){
#function:中测tcl接口导出时，用来生成结果列表
unset i
unset res
unset rs_set
rs_set="puts \$f \"\$"
i=0
while [ $i -lt $1 ]
do 
if [ $i -lt 10 ];then 
res="result0$i"
else 
res="result$i"
fi
echo "    set $res [lindex \$p_row $i]"
rs_set=${rs_set}"{"${res}"}\\\$\$"
i=`expr $i + 1`
done 
echo ${rs_set}"\""
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


crtab(){
#建表函数：
#1.只需要提供模板表和日期，不需要指定表空间，默认为模板表得表空间
#2.运行之前要先建立数据库连接
tablename=""
TABLE_NAME_TEMPLET=""
TBSPACE=""
INDEX_TBSPACE=""
PARTITIONKEY=""
TABLE_NAME_TEMPLET=$1
DT=$2

if [ $# -ne 2 ];then 
echo run script like :
echo crt.sh DWD_NG2_A02025_YYYYMMDD 20101231
return -1
fi

#TABLE_NAME_TEMPLET=DWD_NG2_A02025_YYYYMMDD
#DT=20101231

LEN=`echo $DT|awk '{print length($1)}'`


if [ $LEN -eq 8 ];then 
tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMMDD' $DT`
else 
 	if [ $LEN -eq 6 ];then 
	tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMM' $DT` 
	else 
	echo tablename not set!
	return -1
	fi
fi

TBTMPLET=""
db2 "select 'xxxxx',tabname from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBTMPLET

if [ -z $TBTMPLET ];then
echo TBTMPLET not set!
return -1
fi

TBSPACE=""
db2 "select 'xxxxx',TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read TBSPACE

if [ -z $TBSPACE ];then 
echo TBSPACE not set!
return -1 
fi


INDEX_TBSPACE=""
db2 "select 'xxxxx',INDEX_TBSPACE from syscat.tables \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' "|\
grep xxxxx|awk '{print $2}'|read INDEX_TBSPACE

if [ -z $INDEX_TBSPACE ];then 
echo INDEX_TBSPACE not set!
return -1
fi


PARTITIONKEY=""
db2 "select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname from syscat.columns \
where tabschema = 'BASS2' and tabname = '${TABLE_NAME_TEMPLET}' and partkeyseq >= 1 order by partkeyseq"|\
grep xxxxx|awk '{print $2}'|awk 'BEGIN{a=""}{for (i = 1; i <= NF; i++) a=a$1}END {print a}'|read PARTITIONKEY


if [ -z $PARTITIONKEY ];then 
echo PARTITIONKEY not set!
return -1
fi

sql="create table ${tablename} like ${TABLE_NAME_TEMPLET} in ${TBSPACE} index in ${INDEX_TBSPACE} partitioning key ( ${PARTITIONKEY} ) using hashing not logged initially "
echo $sql
#db2 $sql
}



crtab_ex(){
		#建表函数：
		#1.只需要提供模板表和日期，不需要指定表空间，默认为模板表得表空间
		#2.运行之前要先建立数据库连接
		#rev log:2011-03-07 18:32:35 增加skm参数
		tablename=""
		TABLE_NAME_TEMPLET=""
		TBSPACE=""
		INDEX_TBSPACE=""
		PARTITIONKEY=""
		TABLE_NAME_TEMPLET=$1
		DT=$2
		SKM=$3
		
		if [ $# -ne 3 ];then 
		echo run script like :
		echo crt.sh DWD_NG2_A02025_YYYYMMDD 20101231 SKM
		return -1
		fi
		
		#TABLE_NAME_TEMPLET=DWD_NG2_A02025_YYYYMMDD
		#DT=20101231
		
		LEN=`echo $DT|awk '{print length($1)}'`
		
		
		if [ $LEN -eq 8 ];then 
		tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMMDD' $DT`
		else 
		 	if [ $LEN -eq 6 ];then 
			tablename=`ReplaceAllSubStr $TABLE_NAME_TEMPLET 'YYYYMM' $DT` 
			else 
			echo tablename not set!
			return -1
			fi
		fi
		
		TBTMPLET=""
		db2 "select 'xxxxx',tabname from syscat.tables \
		where tabschema = '${SKM}' and tabname = '${TABLE_NAME_TEMPLET}' "|\
		grep xxxxx|awk '{print $2}'|read TBTMPLET
		
		if [ -z $TBTMPLET ];then
		echo TBTMPLET not set!
		return -1
		fi
		
		TBSPACE=""
		db2 "select 'xxxxx',TBSPACE from syscat.tables \
		where tabschema = '${SKM}' and tabname = '${TABLE_NAME_TEMPLET}' "|\
		grep xxxxx|awk '{print $2}'|read TBSPACE
		
		if [ -z $TBSPACE ];then 
		echo TBSPACE not set!
		return -1 
		fi
		
		
		INDEX_TBSPACE=""
		db2 "select 'xxxxx',INDEX_TBSPACE from syscat.tables \
		where tabschema = '${SKM}' and tabname = '${TABLE_NAME_TEMPLET}' "|\
		grep xxxxx|awk '{print $2}'|read INDEX_TBSPACE
		
		if [ -z $INDEX_TBSPACE ];then 
		echo INDEX_TBSPACE not set!
		return -1
		fi
		
		
		PARTITIONKEY=""
		db2 "select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname from syscat.columns \
		where tabschema = '${SKM}' and tabname = '${TABLE_NAME_TEMPLET}' and partkeyseq >= 1 order by partkeyseq"|\
		grep xxxxx|awk '{print $2}'|awk 'BEGIN{a=""}{for (i = 1; i <= NF; i++) a=a$1}END {print a}'|read PARTITIONKEY
		
		
		if [ -z $PARTITIONKEY ];then 
		echo PARTITIONKEY not set!
		return -1
		fi
		
		sql="create table ${tablename} like ${TABLE_NAME_TEMPLET} in ${TBSPACE} index in ${INDEX_TBSPACE} partitioning key ( ${PARTITIONKEY} ) using hashing not logged initially "
		echo $sql
		#db2 $sql
}


# db2look -d bassdb56 -i bass2 -w bass2 -z bass1 -t g_s_05001_month -e

##!/usr/bin/ksh
getxlsdata(){
				if [ $# -ne 2 ];then
				echo getxlsdata [inputfile] [yyyymm]
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




test(){
#测试：有条件地修改变量值，变量值的生存期	
while [ true ]
do
			alert_time=`date +%S`
			if [ ${alert_time} = "07" ];then 			
				g_d_recordlvl_sent_flag=0
			fi
			echo ${g_d_recordlvl_sent_flag}
				if [ ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
					g_d_recordlvl_sent_flag=1
					echo "now g_d_recordlvl_sent_flag:" $g_d_recordlvl_sent_flag
				fi
sleep 1
done
}


getunixtime(){
		#获得文件unixtimestamp
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
		echo $tcltimestamp
		return 0
}



getunixtime(){
		if [ $# -ne 1 ];then
		echo "getmtime filename"
		exit
		fi
		in_file=$1
		if [ ! -f ${in_file} ];then 
		echo "getmtime 所操作的文件 ${in_file} 不存在!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp
		return 0
}
#getunixtime test.sh


getunixtime2(){
		if [ $# -ne 0 ];then
		echo "getmtime"
		exit
		fi
		touchfile="./.getunixtime2$$"
		touch $touchfile
		in_file=$touchfile
		if [ ! -f ${in_file} ];then 
		echo "getmtime 所操作的文件 ${in_file} 不存在!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp
		rm $touchfile
		return 0
}
#getunixtime test.sh


test2(){
#测试：在循环中只为变量g_d_recordlvl_sent_flag赋值一次，无论循环多少遍！
touch test.sh
start_run_time=`getunixtime test.sh`
start_num=`echo  $start_run_time`
while [ true ]
do
			touch test.sh
			now_alert_time=`getunixtime test.sh`
			now_num=`echo  $now_alert_time`
			
			#echo $start_num
			#echo $now_num
			#diffs=9
			diffs=`expr $now_alert_time - $start_run_time`
			#echo $diffs
			if [ $diffs -lt 60 ];then 
			echo $diffs
			g_d_recordlvl_sent_flag=0
			fi
			echo $g_d_recordlvl_sent_flag
			#if [ ${alert_time} = "07" ];then 			
			#	g_d_recordlvl_sent_flag=0
			#fi
			#echo ${g_d_recordlvl_sent_flag}
			#	if [ ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
			#		g_d_recordlvl_sent_flag=1
			#		echo "now g_d_recordlvl_sent_flag:" $g_d_recordlvl_sent_flag
			#	fi
sleep 5
done
}

#test2


test3(){
#测试：在循环中只为变量g_d_recordlvl_sent_flag赋值一次，无论循环多少遍！
start_run_time=`getunixtime2`
start_num=`echo  $start_run_time`
while [ true ]
do
			now_alert_time=`getunixtime2`
			now_num=`echo  $now_alert_time`
			
			#echo $start_num
			#echo $now_num
			#diffs=9
			diffs=`expr $now_alert_time - $start_run_time`
			#echo $diffs
			if [ $diffs -lt 60 ];then 
			echo $diffs
			g_d_recordlvl_sent_flag=0
			fi
			echo $g_d_recordlvl_sent_flag
			#if [ ${alert_time} = "07" ];then 			
			#	g_d_recordlvl_sent_flag=0
			#fi
			#echo ${g_d_recordlvl_sent_flag}
			#	if [ ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
			#		g_d_recordlvl_sent_flag=1
			#		echo "now g_d_recordlvl_sent_flag:" $g_d_recordlvl_sent_flag
			#	fi
sleep 5
done
}

test3


getunixtime2(){                                                                     
	#sun os date 不能求 unixtimestamp , 唯有这样来取了！有一定的绝对误差。 
	#这个版本改用 touch "." 来刷新时间，不用借助手工touch 一个文件.
		if [ $# -ne 0 ];then                                                            
		echo "getmtime"                                                                 
		exit                                                                            
		fi                                                                              
		touchfile="."                                                                   
		touch $touchfile                                                                
		in_file=$touchfile                                                              
		if [ ! -d ${in_file} ];then                                                     
		echo "getmtime 所操作的文件 ${in_file} 不存在!!"                                
		return 1                                                                        
		fi                                                                              
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp                                                             
		return 0                                                                        
}                                                                                   