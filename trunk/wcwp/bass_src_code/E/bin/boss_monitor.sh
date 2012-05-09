#!/bin/ksh

#create by asiainfo LiZhanyong 2008-09-23

#将此sh 加入到boot.sh中进行调度，每天凌晨4点，6点，7点，8点 过5分的时候执行，早上9点55分执行一次，11点25分执行一次
#5 4,6,7,8 * * * . .profile;$HOME/bin/boss_monitor.sh
#55 9 * * * . .profile;$HOME/bin/boss_monitor.sh
#25 11 * * * . .profile;$HOME/bin/boss_monitor.sh


#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

#在ORACLE数据库执行SQL
up_date()
{
sqlplus wginter/wger987@TEST2DB <<SQLEND
update wgcol.iptpc_alarm_interface  set value=$task_num,org_time=sysdate
where id=$task_id;
commit;
SQLEND
}

#在每天第一次执行时，要对wgcol.iptpc_alarm_interface表中 value 字段进行初始化，置为0
set_zero()
{
sqlplus wginter/wger987@TEST2DB <<SQLEND
update wgcol.iptpc_alarm_interface  set value=0;
commit;
SQLEND
}


#主shell
#1、数据库连接信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#Mon Sep 22 21:37:45 CST 2008

echo `date`
time=`date|cut -c12-13`
echo ${time}


#BOSS加载告警，每天凌晨4点统计已加载接口数

if [ ${time} = "04" ] ; then
   set_zero
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(distinct task_id) task_num from BASS2.ETL_TASK_LOG where task_id like '_0%' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo boss_load_num=${task_num}
   task_id="200001"
   up_date
fi


#BOSS加载告警，每天凌晨6点统计未加载接口数

if [ ${time} = "06" ] ; then
   DB2_SQLCOMM="db2 \"select  'xxxxxx',a.task_num-b.task_num as task_num  from   ( select count(*) task_num from APP.SCH_CONTROL_TASK where control_code like 'TR1_L_0%' and deal_time=1 ) a, ( select count(distinct task_id) task_num from BASS2.ETL_TASK_LOG where task_id like '_0%' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','') ) b\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo boss_noload_num=${task_num}
   task_id="200002"
   up_date
fi


#二经用户表生成告警，每天凌晨7点执行

if [ ${time} = "07" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num  from APP.sch_control_runlog  where control_code in ('BASS2_Dw_product_ds.tcl') and ( char(date(begintime),ISO)=char(date(current timestamp),ISO) and flag=0 )\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo bass2_noload_num=${task_num}
   task_id="200003"
   up_date
fi

#KPI分发告警，每天早上8点执行

if [ ${time} = "08" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num from APP.sch_control_runlog where control_code in ('BASS2_kpi_dailyTofront.tcl') and ( char(date(begintime),ISO)=char(date(current timestamp),ISO) and  flag=0 )\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo kpi_noload_num=${task_num}
   task_id="200004"
   up_date
fi   


#MIS上报未正确执行告警，每天早上8点执行

if [ ${time} = "08" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num from APP.sch_control_runlog where control_code in ('REP_day_mis_report.sh','REP_stat_zd_erp02.tcl','REP_export_erp02.sh') and ( char(date(begintime),ISO)=char(date(current timestamp),ISO) and flag=0 )\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo mis_noload_num=${task_num}
   task_id="200005"
   up_date
fi 


#一经10点上报监控，每天上午9点55分执行

if [ ${time} = "09" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num from APP.G_FILE_REPORT where substr(filename,9,8)=replace(char(date(current timestamp- 1 days),ISO),'-','') and err_code='00'\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo bass1_12clk_num=${task_num}
   task_id="200006"
   up_date
fi 

#一经12点上报监控，每天上午11点25分执行

if [ ${time} = "11" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num from APP.G_FILE_REPORT where substr(filename,9,8)=replace(char(date(current timestamp- 1 days),ISO),'-','') and err_code='00'\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo bass1_10clk_num=${task_num}
   task_id="200007"
   up_date
fi 

#一经打包监控,每天早上8点执行

if [ ${time} = "08" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(1) as task_num from app.g_runlog where char(time_id)=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo bass1_8clk_num=${task_num}
   task_id="200008"
   up_date
fi 

echo "程序正常结束！"








