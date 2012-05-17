######################################################################################################
#接口名称：实体渠道重点增值业务办理日汇总
#接口编码：22092
#接口说明：记录实体渠道29项重点增值业办理日汇总信息，涉及自营厅、委托经营厅、24小时自助营业厅或社会代理网点
#程序名称: G_S_22092_DAY.tcl
#功能描述: 生成22092的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-06-08
#问题记录：
#修改历史: ref:22062
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set op_time 2011-06-07

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#自然月
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22092_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22092_DAY where time_id=$timestamp"
    exec_sql $sql_buff

		set sql_buff "
		insert into G_S_22092_DAY
		(
         TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
        ,IMP_VAL_TYPE
        ,IMP_VAL_OPEN_CNT		
		)
		select 
		     $timestamp
		    ,'$timestamp' DEAL_DATE
        ,CHANNEL_ID
        ,ACCEPT_TYPE
        ,IMP_ACCEPTTYPE
        ,char(CNT)
		from  BASS1.G_S_22091_DAY_P2 
		where time_id = $timestamp
		"
exec_sql $sql_buff

##~   删除 a.channel_type = 90886 的渠道
  set sql_buff "
delete from 
(select * from bass1.g_s_22092_day where  TIME_ID = $timestamp and channel_id in (
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where  a.channel_type = 90886
	    )
) t
  "
exec_sql $sql_buff

  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22092_DAY"
  set pk   "DEAL_DATE||CHANNEL_ID||DEAL_TYPE||IMP_VAL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.$tabname 3
  
  set sql_buff "
  select count(0)
from bass1.g_s_22092_day where TIME_ID = $timestamp
and  channel_id not in 
			(
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
		 and a.channel_type <> 90886
	    )
  "
chkzero2 $sql_buff "invaid channel_id"

	return 0

}

