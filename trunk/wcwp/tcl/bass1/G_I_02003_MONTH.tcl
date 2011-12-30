######################################################################################################
#接口名称：资费营销案包含业务功能
#接口编码：02003
#接口说明：定义资费营销案中包括的业务功能组合。
#          业务功能是指中国移动在营销活动中提供能够满足客户的移动通信需求和利益的各种服务。
#          例如：语音、短信、彩信、GPRS、WAP、百宝箱等。客户通过购买业务功能来享受传递信息过程中
#          的有益效用，同时为中国移动带来经济收入。业务功能针对不同的客户品牌有不同的选择权限和
#          标准价格，同时业务功能在不同的资费营销案方案中也可以重新定义价格。

#程序名称: G_I_02003_MONTH.tcl
#功能描述: 生成02003的数据
#运行粒度: 月
#源    表：1.bass2.dwd_promoplan_yyyymmdd(资费营销案)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前业务功能标识不能出，按照老系统做法统一给'010'.争取以后让BOSS做改造。
#修改历史: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02003_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
             
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02003_month
	               select a.time_id,
	                      a.plan_id,
	                      a.bus_func_id
	               from (       
                       select
                          $op_month time_id
                          ,char(prod_id) plan_id
                          ,'010' bus_func_id
                          ,row_number()over(partition by char(prod_id) order by $op_month ) row_id
                          from bass2.dwd_promoplan_$this_month_last_day
                       ) a
                 where a.row_id=1         "       
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

	return 0
}