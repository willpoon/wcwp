#!/usr/bin/ksh
###################################################################################################
#~ author : 潘志伟 panzw2@asiainfo-linkage.com
#~ function desc：西藏经分测试库表字段脱敏 批量作业程序
#~ revision log：
#~ r1 : 2012-06-12 脱敏函数编写
#~ r2 : 2012-06-12 表脱敏核心功能编写
#~ r3 : 2012-06-13 表脱敏的批量操作功能实现
#~ r4 : 2012-06-13 表脱敏日志功能
#~ r5 : 2012-06-13 表脱敏异常处理
#~ r6 : 2012-06-13 测试、debug
#~ TODO:
#~ 1.分别对模板表和非模板表生成脱敏语句日志，增强日志的可用性
#~ 2.针对潜在、反馈的异常作处理
###################################################################################################

#在主机上执行DB2 SQL

DB2_SQL_EXEC()
{
	DB2_OSS_DB="bassdb"
	DB2_OSS_USER="bass2"
	DB2_OSS_PASSWD="bass2"
		db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
		eval $DB2_SQLCOMM 
					if [ $? -ne 0 ];then 
					echo "error occurs when running DB2_SQLCOMM!"
					db2 connect reset 
					return 1
					fi
		db2 commit 
		db2 connect reset 
		return 0
}

###################################################################################################
#~ 检查表是否有数据
chkIfData(){
	skm=`echo $1 | tr  "[a-z]"  "[A-Z]"`
	Tbl=`echo $2 | tr  "[a-z]"  "[A-Z]"`
	sql_str="
	select 'xxxxx',count(0) cnt from (select 1 from ${skm}.${Tbl} fetch first 10 rows only) t with ur
	"

	DB2_SQLCOMM="db2 \"${sql_str}\""
	DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'
	
	return 0
	
}
###################################################################################################
#~ 导出模板表需要脱敏的字段
expTmplCol(){
	skm=$1
	tabname=$2
	sql_str="
	export to ./${skm}.${tabname}.f of del \
	MODIFIED BY nochardel coldel0x09  STRIPLZEROS decplusblank \
	   select
		   a.TABSCHEMA
		  ,a.tabname
		  ,c.COLNAME
		  ,b.COLNO
	from bass2.sens_two_stage a 
	   join 
		 syscat.columns b on a.TABSCHEMA=b.TABSCHEMA and a.tabname=b.tabname
	  join 
		 bass2.sens_columns c on b.COLNAME=c.COLNAME
		where  a.TABSCHEMA = '${skm}' and a.tabname='${tabname}'
		order by a.tabname, b.COLNO
	with ur
	"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	DB2_SQL_EXEC>/dev/null
	return 0
}


###################################################################################################
#~ 获得字段类型和分类标识
getType(){

	skm=`echo $1 | tr  "[a-z]"  "[A-Z]"`
	Tbl=`echo $2 | tr  "[a-z]"  "[A-Z]"`
	fld=`echo $3 | tr  "[a-z]"  "[A-Z]"`

	sql_str="
		select 'xxxxx', TYPENAME ,coltype
		from (   	 select *from  bass2.sens_one_stage  union all
				 select *from  bass2.sens_two_stage  union all
				 select *from  bass2.sens_three_stage  ) a
		join  syscat.columns b on a.TABSCHEMA=b.TABSCHEMA and a.tabname=b.tabname
		join  bass2.sens_columns c on b.COLNAME=c.COLNAME
		where 
		  b.tabschema = '${skm}'
		and b.tabname = '${Tbl}'
		and b.colname = '${fld}'
		with ur
	"

	DB2_SQLCOMM="db2 \"${sql_str}\""
	DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2,$3}'

	return 0

}
###################################################################################################
#~ 生成更新语句
genUpStmt(){
	skm=`echo $1 | tr  "[a-z]"  "[A-Z]"`
	Tbl=`echo $2 | tr  "[a-z]"  "[A-Z]"`
	fld=`echo $3 | tr  "[a-z]"  "[A-Z]"`
	colType=$4
	resv1=$5
	resv2=$6
	resv3=$7

	typeName=`getType ${skm} ${tabname} ${fld}|awk '{print $1}'`
	typeNo=`getType ${skm} ${tabname} ${fld}|awk '{print $2}'`
	
	case $typeName in
		  VARCHAR)   upStmt="update ${skm}.${Tbl} set ${fld} = char(bass2.encryptcol(char(${fld}),${typeNo}))" ;;
		  BIGINT)    upStmt="update ${skm}.${Tbl} set ${fld} = bigint(bass2.encryptcol(char(${fld}),${typeNo}))" ;;
		  INTEGER)   upStmt="update ${skm}.${Tbl} set ${fld} = int(bass2.encryptcol(char(${fld}),${typeNo}))" ;;
		  SMALLINT)  upStmt="update ${skm}.${Tbl} set ${fld} = int(bass2.encryptcol(char(${fld}),${typeNo}))" ;;
		  TIMESTAMP) upStmt="update ${skm}.${Tbl} set ${fld} = current timestamp" ;;
		  *)           echo "Wrong name...";;
	esac

	echo ${upStmt}

	return 0

}

###################################################################################################
#~ 处理表中需脱敏的各字段
processCol(){
	skm=$1
	tabname=$2
		while read tmplSkm tmplTabname colName colNo
		do
			sql_str=`genUpStmt ${FactSkm} ${FactTabname} $colName  1 0 0 0`
			echo ${sql_str} >>  ${skm}.${tabname}.sql
			cnt=`chkIfData ${FactSkm}  ${FactTabname} `
			echo $cnt|grep [0-9]
			cnt_ret=$?
			if [ ${cnt_ret} -ne 0 ];then
				echo " ${FactSkm}.${FactTabname} error occurs!"			
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 0 
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 0	>>  error.log				
			fi 

			if [ $cnt -gt 0 ];then
				echo executing...		
				DB2_SQLCOMM="db2 \"${sql_str}\""
				DB2_SQL_EXEC>/dev/null
				exec_ret=$?
					if [ ${exec_ret} -eq 0 ];then 
					#~ 写正确日志
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 1 
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 1 >>  success.log
					else
					#~ 写错误日志
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 0 
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 0	>>  error.log
					fi
				echo execute finish!				
			else 
				if [ $cnt -eq 0 ];then
					echo " ${FactSkm}.${FactTabname} is empty!"
				else 
					echo " ${FactSkm}.${FactTabname} error occurs!"			
				fi
			fi
		done< ${skm}.${tabname}.f
}
###################################################################################################
#~ 对实例表作处理
processTableInst(){
		skm=$1
		tabname=$2
		while read FactSkm FactTabname
		do
		processCol $skm $tabname
		
		done< ${skm}.${tabname}.tbl
}


###################################################################################################
#~ 通过模板表得到实例表
getFactTable(){
	while read Tskm Ttabname
	do
	tabprefix=`echo ${Ttabname}|nawk -F"YYYY" '{print $1}'`
	echo ${Ttabname}|grep YYYYMMDD
	ret=$?
	rsFile="${Tskm}.${Ttabname}.tbl"
	if [ $ret -eq 0 ];then #获取实体表
		grep "${tabprefix}[2][0][1][0-9][0-1][0-9][0-3][0-9]$" ${syscat_tables_dat}|grep ${Tskm} >> ${rsFile}
	else
		grep "${tabprefix}[2][0][1][0-9][0-1][0-9]$" ${syscat_tables_dat}|grep ${Tskm}  >>${rsFile}
	fi
	expTmplCol ${Tskm} ${Ttabname}
	processTableInst  ${Tskm} ${Ttabname}
	done <${deal_tables_dat}
}

#~ 处理无模板的表
getFactTable2(){
	while read Tskm Ttabname
	do
	rsFile="${Tskm}.${Ttabname}.tbl"
	grep "${Ttabname}$" ${syscat_tables_dat}|grep ${Tskm} >> ${rsFile}
	expTmplCol ${Tskm} ${Ttabname}
	processTableInst  ${Tskm} ${Ttabname}
	done <${deal_tables_dat}
}


###################################################################################################
#~ main proc

if [ $# -ne 1 ];then 
echo "ksh $0 [T/N]"
exit
fi

#~ 清理现场
rm *.tbl *.f *.sql

syscat_tables_dat=syscat.tables.dat
echo generating ${syscat_tables_dat} ...

#~ 导出表配置信息文件

#~ ${syscat_tables_dat}
sql_str="export to ./${syscat_tables_dat} of del MODIFIED BY nochardel coldel0x09  STRIPLZEROS decplusblank select TABSCHEMA , tabname from syscat.tables where TABSCHEMA in ('MPM'
		,'DMKMARK'
		,'BASS2'
		,'BASSWEB'
		,'REPORT'
		,'MMP'
		,'NGBASS20'
		,'BASS1'
		,'BTS'
		,'NGBASS'
		)
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC>/dev/null


#~ tabletemplate.dat
echo generating tabletemplate.dat ...

sql_str="export to ./tabletemplate.dat of del \
			MODIFIED BY nochardel coldel0x09  STRIPLZEROS decplusblank \
					select distinct TABSCHEMA,TABNAME
					from (   	 select *from  bass2.sens_one_stage  union all
							 select *from  bass2.sens_two_stage  union all
							 select *from  bass2.sens_three_stage  ) a
							 where TABNAME LIKE '%YYYYMM%'
							 order by 2
"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC>/dev/null




#~ tableNoTemplate.dat
echo generating tableNoTemplate.dat ...

sql_str="export to ./tableNoTemplate.dat of del MODIFIED BY nochardel coldel0x09  STRIPLZEROS decplusblank \
select distinct TABSCHEMA,TABNAME \
from ( select *from  bass2.sens_one_stage  \
union all select *from  bass2.sens_two_stage  \
union all select *from  bass2.sens_three_stage  ) a \
where TABNAME  NOT LIKE '%YYYYMM%' order by 2"
DB2_SQLCOMM="db2 \"${sql_str}\""
DB2_SQL_EXEC>/dev/null


if [ $1 = "T" ];then
#~ 处理带日期的模板表
deal_tables_dat=tabletemplate.dat
echo ${deal_tables_dat}
getFactTable
echo all done!
fi

if [ $1 = "N" ];then
#~ 处理不带日期的非模板表
deal_tables_dat=tableNoTemplate.dat
echo ${deal_tables_dat}
getFactTable2
echo all done!
fi

exit
