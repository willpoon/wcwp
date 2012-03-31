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

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
 
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22049_month where time_id=$op_month"
  exec_sql $sql_buff         



set sql_buff "insert into bass1.g_s_22049_month
select
$op_month
  ,'$op_month'
  ,a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CITY_ID)),'13101') 
  ,char(a.channel_id)
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
  ,char(bigint(sum(a.fee )))
from bass2.stat_channel_reward_0007 a
WHERE a.channel_type in (90105,90102)
  and a.op_time=$op_month
group by
  a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') 
  ,char(a.channel_id)  
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end "
                       
  exec_sql $sql_buff         


	return 0
}