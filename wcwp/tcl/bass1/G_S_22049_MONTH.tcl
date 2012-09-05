######################################################################################################
#接口单元名称：空中充值点酬金信息
#接口单元编码：22049
#接口单元说明：统计所有具备空中充值功能的网点情况
#程序名称: G_S_22049_MONTH.tcl
#功能描述: 生成22049的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_payitem_yyyymm(缴费表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2007-10-24
#问题记录：1.
#修改历史: liuqf 1.7.0规范20101225 来源于酬金报表专题stat_channel_reward_0007
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
		##~   set optime_month 2012-04
		##~   set op_time 2012-04-30
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
    set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
    #本月最后一天 yyyy-mm-dd
	##~   set last_month_day [GetLastDay [string range $timestamp 0 5]01]


	global app_name
	set app_name "G_S_22049_MONTH.tcl"    
	
	
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22049_month where time_id=$op_month"
  exec_sql $sql_buff         

	set sql_buf "ALTER TABLE BASS1.g_s_22049_month_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"

    exec_sql $sql_buf

set sql_buff "insert into bass1.G_S_22049_MONTH_1
select
$op_month
  ,'$op_month'
  ,a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CITY_ID)),'13101') 
  ,max(char(a.channel_id))
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
  ,char(bigint(sum(a.fee )))
from bass2.stat_channel_reward_0007 a
, bass2.Dw_channel_info_$op_month b
WHERE a.channel_type in (90105,90102)
  and a.op_time=$op_month
  and a.CHANNEL_ID = b.CHANNEL_ID
group by
  a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') 
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
   with ur
   "
                       
  exec_sql $sql_buff         


set sql_buff "
insert into G_S_22049_MONTH_1
(
         TIME_ID
        ,STATMONTH
        ,CHRG_NBR
        ,CMCC_ID
        ,CHANNEL_ID
        ,CHONGZHITYPE
        ,CHOUJIN
)		
select 
$op_month TIME_ID
  ,'$op_month' STATMONTH
  ,a.CHRG_NBR CHRG_NBR
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CMCC_ID)),'13101')  CMCC_ID
  ,a.CHNL_ID CHANNEL_ID
  ,'2' CHONGZHITYPE
  ,'0' CHOUJIN
from (select * from g_i_06040_day where time_id = $this_month_last_day ) a
where CHNL_ID in (
select CHNL_ID  from g_i_06040_day
where time_id = $this_month_last_day
except
select CHANNEL_ID
from G_S_22049_MONTH_1 
where time_id = $op_month
)
with ur
"

  exec_sql $sql_buff         



set sql_buff "
insert into G_S_22049_MONTH
(
         TIME_ID
        ,STATMONTH
        ,CHRG_NBR
        ,CMCC_ID
        ,CHANNEL_ID
        ,CHONGZHITYPE
        ,CHOUJIN
)		
select 
TIME_ID
,STATMONTH
,max(CHRG_NBR)
,CMCC_ID
,CHANNEL_ID 
,CHONGZHITYPE
,char(sum(bigint(CHOUJIN)))
from G_S_22049_MONTH_1
group by 
         TIME_ID
        ,STATMONTH
        ,CMCC_ID
        ,CHANNEL_ID
        ,CHONGZHITYPE
with ur
"

  exec_sql $sql_buff         




  set tabname "G_S_22049_MONTH"
        set pk                  "STATMONTH||CHRG_NBR||CMCC_ID"
        chkpkunique ${tabname} ${pk} ${op_month}



			set grade 2
	        set alarmcontent "检查渠道酬金报表0007数据是否为空或正常！"
	        WriteAlarm $app_name $op_month $grade ${alarmcontent}



select OP_TIME , count(0) 
,  sum( PRE_FEE ) 
,  sum( FEE ) 
from bass2.stat_channel_reward_0007 
group by  OP_TIME 
order by 1 



	return 0
}