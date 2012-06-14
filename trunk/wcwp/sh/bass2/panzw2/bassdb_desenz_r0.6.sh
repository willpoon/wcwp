#!/usr/bin/ksh
###################################################################################################
#~ author : ��־ΰ panzw2@asiainfo-linkage.com
#~ function desc�����ؾ��ֲ��Կ���ֶ����� ������ҵ����
#~ revision log��
#~ r1 : 2012-06-12 ����������д
#~ r2 : 2012-06-12 ���������Ĺ��ܱ�д
#~ r3 : 2012-06-13 ��������������������ʵ��
#~ r4 : 2012-06-13 ��������־����
#~ r5 : 2012-06-13 �������쳣����
#~ r6 : 2012-06-13 ���ԡ�debug
#~ TODO:
#~ 1.�ֱ��ģ���ͷ�ģ����������������־����ǿ��־�Ŀ�����
#~ 2.���Ǳ�ڡ��������쳣������
###################################################################################################

#��������ִ��DB2 SQL

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
#~ �����Ƿ�������
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
#~ ����ģ�����Ҫ�������ֶ�
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
#~ ����ֶ����ͺͷ����ʶ
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
#~ ���ɸ������
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
#~ ��������������ĸ��ֶ�
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
					#~ д��ȷ��־
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 1 
						echo `date +%Y%m%d%H%M%S` ${FactSkm} ${FactTabname} ${colName} 1 >>  success.log
					else
					#~ д������־
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
#~ ��ʵ����������
processTableInst(){
		skm=$1
		tabname=$2
		while read FactSkm FactTabname
		do
		processCol $skm $tabname
		
		done< ${skm}.${tabname}.tbl
}


###################################################################################################
#~ ͨ��ģ���õ�ʵ����
getFactTable(){
	while read Tskm Ttabname
	do
	tabprefix=`echo ${Ttabname}|nawk -F"YYYY" '{print $1}'`
	echo ${Ttabname}|grep YYYYMMDD
	ret=$?
	rsFile="${Tskm}.${Ttabname}.tbl"
	if [ $ret -eq 0 ];then #��ȡʵ���
		grep "${tabprefix}[2][0][1][0-9][0-1][0-9][0-3][0-9]$" ${syscat_tables_dat}|grep ${Tskm} >> ${rsFile}
	else
		grep "${tabprefix}[2][0][1][0-9][0-1][0-9]$" ${syscat_tables_dat}|grep ${Tskm}  >>${rsFile}
	fi
	expTmplCol ${Tskm} ${Ttabname}
	processTableInst  ${Tskm} ${Ttabname}
	done <${deal_tables_dat}
}

#~ ������ģ��ı�
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

#~ �����ֳ�
rm *.tbl *.f *.sql

syscat_tables_dat=syscat.tables.dat
echo generating ${syscat_tables_dat} ...

#~ ������������Ϣ�ļ�

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
#~ ��������ڵ�ģ���
deal_tables_dat=tabletemplate.dat
echo ${deal_tables_dat}
getFactTable
echo all done!
fi

if [ $1 = "N" ];then
#~ ���������ڵķ�ģ���
deal_tables_dat=tableNoTemplate.dat
echo ${deal_tables_dat}
getFactTable2
echo all done!
fi

exit
