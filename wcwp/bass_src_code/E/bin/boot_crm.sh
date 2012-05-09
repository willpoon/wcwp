#!/bin/sh
#author:lgk
#date: 2006/09/18

#检查日接口分别在两个检查点（4点半、5点）是否送完
check_day_send_sms()
{
sqlplus ngcp/ngcp@xzsale <<SQLEND
insert into kt.sms_info(ID,MSISDN,FLAG,MSG)
select kt.sms_id.nextval,phone_id,'SEND','经分'||to_char(sysdate - 1,'YYYYMMDD')||'日接口数据在凌晨4点半没有送完，请检查原因！'
from zk.mb_if_jyfx_sms_phone
where exists 
(
select task_id from zk.mb_if_jyfx_task_running
where cycle_id=to_char(sysdate - 1,'YYYYMMDD')
and to_char(sysdate,'HH24MI')>='0430'
);
insert into kt.sms_info(ID,MSISDN,FLAG,MSG)
select kt.sms_id.nextval,phone_id,'SEND','经分'||to_char(sysdate - 1,'YYYYMMDD')||'日接口数据在凌晨5点没有送完，请检查原因！'
from zk.mb_if_jyfx_sms_phone
where exists 
(
select task_id from zk.mb_if_jyfx_task_running
where cycle_id=to_char(sysdate - 1,'YYYYMMDD')
and to_char(sysdate,'HH24MI')>='0500'
);
commit;
SQLEND
}

#发送短信，通过插入到BOSS营帐数据库中的表kt.sms_info实现
send_sms()
{
send_message="新接口程序："${send_message}"程序已停，系统会自动重启,请检查原因！"
#sqlplus jyfx/j0y8f2z3@xzsale <<SQLEND
#insert into kt.sms_info(ID,MSISDN,FLAG,MSG)
#select kt.sms_id.nextval,phone_id,'SEND','$send_message' from zk.mb_if_jyfx_sms_phone;
#commit;
#SQLEND
echo "$send_message,模拟发送短信"
}

#added by lizhanyong 2008-09-26
send_sms2()
{
send_message="接口程序："${send_message}"程序已停,请检查原因,并及时处理！"
#sqlplus jyfx/j0y8f2z3@xzsale <<SQLEND
#insert into kt.sms_info(ID,MSISDN,FLAG,MSG)
#select kt.sms_id.nextval,phone_id,'SEND','$send_message' from zk.mb_if_jyfx_sms_phone;
#commit;
#SQLEND
echo "$send_message,模拟发送短信"
}


#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}



#数据库连接信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#added end

#主过程，检查FTP程序和BOSS接口生成程序以及xfer采集程序是否停下来，若停，则自动重启并发送告警短信
#####ps_result=`ps -ef|grep __ftp_crm.bin|grep -v grep|awk '{print $1}'`
#####if [ "$ps_result" = "" ]; then
#####        echo "ftp_crm is stop!"
#####        running_file="$HOME/config/running_info/running_ftp_crm"
#####        if [ -f $running_file ]; then
#####                rm $running_file
#####        fi 
#####        stop_file="$HOME/config/running_info/stop_ftp_crm"
#####        if [ -f $stop_file ]; then
#####                rm $stop_file
#####        fi
#####        
#####        time_str=`date`
#####        echo "$time_str: send message about ftp_crm!" 
#####        send_message="__ftp_crm.bin"
#####        send_sms
#####        
#####        time_str=`date`
#####        echo "$time_str: crontab auto boot ftp_crm!"         
#####        nohup $HOME/bin/__ftp_crm.bin > /dev/null &
#####fi
#####
#####ps_result=`ps -ef|grep __boss_crm_interface.bin|grep -v grep|awk '{print $1}'`
#####if [ "$ps_result" = "" ]; then
#####		echo "boss_crm_interface is stop!"
#####		running_file="$HOME/config/running_info/running_boss_crm_interface"
#####		if [ -f $running_file ]; then
#####		        rm $running_file
#####		fi
#####		stop_file="$HOME/config/running_info/stop_boss_crm_interface"
#####		if [ -f $stop_file ]; then
#####		        rm $stop_file
#####		fi
#####		
#####        time_str=`date`
#####        echo "$time_str: send message about boss_crm_interface!" 
#####        send_message="__boss_crm_interface.bin"
#####        send_sms
#####        
#####        time_str=`date`
#####        echo "$time_str: crontab auto boot boss_crm_interface!"		
#####        nohup $HOME/bin/__boss_crm_interface.bin > /dev/null &
#####        #nohup $HOME/bin/__boss_crm_interface.bin > $HOME/bin/boss_crm_interface.log &
#####fi

ps_result=`ps -ef|grep xfer|grep xfer.cfg|grep -v grep|awk '{print $1}'`
if [ "$ps_result" = "" ]; then
		echo "cdr xfer is stop!"
		
        time_str=`date`
        echo "$time_str: send message about cdr xfer!" 
        send_message="cdr xfer"
        send_sms
        
        time_str=`date`
        echo "$time_str: crontab auto boot cdr xfer!"		
        $HOME/bin/start_xfer.sh
fi

# added by lizhanyong  2008-09-26 
###########增加对load_boss.sh、load_dsmp.sh、load_ca.sh、load_mr.sh、load_nm.sh、load_ota.sh的监控
##########
##########ps_result=`ps -ef|grep load_boss.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_boss.sh is stop!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_boss.sh!" 
##########        send_message="load_boss.sh"
##########        send_sms2
##########fi
##########
##########ps_result=`ps -ef|grep load_dsmp.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_dsmp.sh is stop!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_dsmp.sh!" 
##########        send_message="load_dsmp.sh"
##########        send_sms2
##########fi
##########
##########ps_result=`ps -ef|grep load_ca.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_ca.sh is stop!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_ca.sh!" 
##########        send_message="load_ca.sh"
##########        send_sms2
##########fi
##########
##########ps_result=`ps -ef|grep load_mr.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_mr.sh!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_mr.sh!" 
##########        send_message="load_mr.sh"
##########        send_sms2
##########fi
##########
##########ps_result=`ps -ef|grep load_nm.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_nm.sh!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_nm.sh!" 
##########        send_message="load_nm.sh"
##########        send_sms2
##########fi
##########
##########ps_result=`ps -ef|grep load_ota.sh|grep -v grep|awk '{print $1}'`
##########if [ "$ps_result" = "" ]; then
##########		echo "load_ota.sh!"
##########		
##########        time_str=`date`
##########        echo "$time_str: send message about load_ota.sh!" 
##########        send_message="load_ota.sh"
##########        send_sms2
##########fi
##########
###########增加对语音清单和短信清单的执行监控：load_boss_cdr.sh 每天凌晨1点执行，在3点的时候检查数据load 是否成功，如果没有成功，则认为此程序存在问题。
##########echo `date`
##########time=`date +%H`
##########echo ${time}
##########
##########if [ ${time} = "03" ] ; then
##########   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='A05001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
##########   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
##########   echo task_num=${task_num}
##########   if [ ${task_num} = "0" ] ; then
##########        time_str=`date`
##########        echo "$time_str: send message about load_boss_cdr.sh!" 
##########        send_message="load_boss_cdr.sh"
##########        send_sms2
##########   fi     
##########fi
##########
###########added end
##########
##########
##########
###########屏蔽日接口检查，若需要，再打开，根据实际情况修改库连接和检查点时间
###########check_day_send_sms

echo "程序正常结束！"
