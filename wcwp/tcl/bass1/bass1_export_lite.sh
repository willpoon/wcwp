##############################################################
#author : panzhiwei
#功能：简易的导出程序，按照一经规范将接口数据导出。
#ref data path:/bassapp/backapp/data/bass1/export/export_20110416
#20120511:
#~ 1.增加可定制日期功能
#~ 2.增加自动更新导出次数功能
#~ 3.更改输出目录/bassapp/backapp/data/bass1/export
#~ $ ksh  bass1_export_lite.sh G_I_77780_DAY 20120331
#~ todo: 支持 yyyy-mm-dd 格式 typeset timestamp=`echo $2|awk ' { printf("%s%s%s\n", substr($1,1,4), substr($1,6,2), substr($1,9,2)) } '`
##############################################################

. /bassapp/bihome/panzw/common.ksh

	if [ $# -eq 2 ];then
		data_date=$2
	else 
		if [ $# -eq 1 ];then
			today=`date '+%Y%m%d'`
			data_date=`yesterday ${today}`
		else 
			echo "$0 tabnameWithoutSkm [yyyymm[dd]]"
			exit
		fi		
	fi
	
	skm=bass1
	tabname=`echo $1 | tr  "[A-Z]"  "[a-z]"`
	full_table_name=${skm}.${tabname}
	coarse=`echo ${tabname}|awk -F'_' '{print $2}'`
	echo ${coarse}
	unit_code=`echo ${tabname}|awk -F'_' '{print $3}'`
	echo ${unit_code}

	tmp_exp_dir=/bassapp/backapp/data/bass1/export/export
	exp_dir=/bassapp/backapp/data/bass1/export/export_$data_date
	
	db2 connect to bassdb user bass1 using bass1 > /dev/null
	desc_tmpfile=".desc.tmp$$"
	awk_tmpfile=".awk.tmp$$"
	db2 describe table ${full_table_name} > ${desc_tmpfile}
	
##############################################################
#插入导出记录
sql=" 	
		select 'xxxx', count(0) from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}' and TIME_ID = ${data_date}
		with  ur
	" 
db2 $sql

db2  $sql|grep xxxx | nawk '{print $2}'|read iflog

if [ $iflog -eq 0 ];then
sql="
	insert into APP.G_RUNLOG (
	   TIME_ID
        ,UNIT_CODE
        ,COARSE
        ,EXPORT_TIME
        ,EXPORT_NUM
        ,RETURN_FLAG
	)
	select 
        $data_date TIME_ID
        ,'$unit_code' UNIT_CODE
        ,0 COARSE
        ,current timestamp EXPORT_TIME
        ,0 EXPORT_NUM
        ,0 RETURN_FLAG
	  from sysibm.sysdummy1 with ur
	" 
db2  $sql
else
##############################################################
#导出次数加1
echo increase export count...
sql="
		update (select *from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}'
			and TIME_ID = ${data_date}
			) t
			set export_num = export_num+1
	" 
db2  $sql	
echo increase export count finished!
##############################################################
fi


##############################################################
#取导出次数
sql="select 'xxxx'
			,case when export_num < 10 then '0'||substr(char(export_num),1,1) 
				else substr(char(export_num),1,2) 
			end export_num
		from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}'
			and TIME_ID = ${data_date}
	with ur"
db2  $sql|grep xxxx | nawk '{print $2}'|read exp_cnt
echo export count:$exp_cnt
##############################################################
	dat_file_name=${coarse}_13100_${data_date}_${unit_code}_${exp_cnt}_001.dat
	verf_file_name=${coarse}_13100_${data_date}_${unit_code}_${exp_cnt}.verf
##############################################################
sql="
		update (select *from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}'
			and TIME_ID = ${data_date}
			) t
			set DATA_FILE = '$dat_file_name'
	" 
db2  $sql	
##############################################################
sql="
		update (select *from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}'
			and TIME_ID = ${data_date}
			) t
			set VERF_FILE = '$verf_file_name'
	" 
db2  $sql
##############################################################
sql="
		update (select *from APP.G_RUNLOG
		where UNIT_CODE = '${unit_code}'
			and TIME_ID = ${data_date}
			) t
			set EXPORT_TIME = current timestamp
	" 
db2  $sql	

##############################################################
#export dat file
	
	echo "export to ${tmp_exp_dir}/t_${dat_file_name} of del  modified by nochardel select cast(row_number()over() as char(8)) \\"> ${awk_tmpfile}
	cat ${desc_tmpfile}|grep [0-9] |\
	grep -iv time_id|grep -v  "selected"|\
	awk 'BEGIN{FS=" ";OFS=""}{print "||trim(char("$1"))||","space("$4"-length(trim(char("$1")))) \\"}' >>${awk_tmpfile}
	echo " from ${full_table_name} where time_id = ${data_date} with ur">>${awk_tmpfile}
	#cat ${awk_tmpfile}
	db2 -f ${awk_tmpfile} > /dev/null
	sed "s/$/`echo \\\r`/g" ${tmp_exp_dir}/t_${dat_file_name} > ${exp_dir}/${dat_file_name}
	rm ${tmp_exp_dir}/t_${dat_file_name}
	rm ${desc_tmpfile}
	filesize=`ls -l ${exp_dir}/${dat_file_name}|awk '{print $5}'`
	fileline=`wc -l ${exp_dir}/${dat_file_name}|awk '{print $1}'`
	filetimestamp=`echo "puts [file mtime ${exp_dir}/${dat_file_name}]"|tclsh|awk -F'\r' '{print $1}'`
	export filetimestamp
	file_mtime=`perl -MPOSIX -e '$x=$ENV{"filetimestamp"}; @y=strftime("%Y%m%d%H%M%S", localtime($x)); print "@y\n"'`

##export verf file
	
	echo "export to ${tmp_exp_dir}/t_${verf_file_name} of del  modified by nochardel select \\
	cast('${dat_file_name}' as char(40)) \\
	||cast('${filesize}' as char(20)) \\
	||cast('${fileline}' as char(20)) \\
	||'${data_date}' \\
	||cast('${file_mtime}' as char(14)) \\
	from sysibm.sysdummy1 with ur">${awk_tmpfile}
	#cat ${awk_tmpfile}
	db2 -f ${awk_tmpfile} > /dev/null
	sed "s/$/`echo \\\r`/g" ${tmp_exp_dir}/t_${verf_file_name} > ${exp_dir}/${verf_file_name}
	rm ${tmp_exp_dir}/t_${verf_file_name}
	rm ${awk_tmpfile}
	db2 terminate 

#all done
exit
