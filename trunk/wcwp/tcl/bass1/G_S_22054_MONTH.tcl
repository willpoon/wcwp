######################################################################################################
#接口名称：实体渠道月汇总
#接口编码：22054
#接口说明：记录中国移动所有实体渠道办理业务的月汇总情况。
#程序名称: G_S_22054_MONTH.tcl
#功能描述: 生成22054的数据
#运行粒度: 月
#源    表：1.bass2.dw_product_yyyymm
#          2.dw_product_busi_dm_yyyymm 工单流水表
#	         3.dw_acct_busicharge_dm_yyyymm  营业缴费记录表
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：xiahuaxue
#编写时间：2008-10-14
#问题记录：
#修改历史: 20100124 修改离网用户口径 userstatus_id in (4,5,7,9)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

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

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22054_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

  #插入 新增客户数  
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$op_month
                     where month_new_mark = 1
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff
  
  #插入 新增全球通客户数 数据
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,'0'
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$op_month
                     where coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') = '1' and  month_new_mark = 1
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff


  #插入 离网 客户数  
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$op_month
                     where userstatus_id in (4,5,7,9) and 
                           valid_date >= date('$this_month_first_day') and 
                           valid_date <= date('$this_month_last_day')
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff
  
  #插入 离网全球通客户数 数据
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,'0'
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$op_month
                     where coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') = '1' and  
                           userstatus_id in (4,5,7,9) and 
                           valid_date >= date('$this_month_first_day') and 
                           valid_date <= date('$this_month_last_day')
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff


  #插入 业务办理量 数据
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,char(count(distinct SO_NBR))
                        ,'0'
                      from bass2.dw_product_busi_dm_$op_month
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff




  #插入 缴费笔数 数据
	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        88888888
                        ,'$op_month'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,char(count(distinct SO_NBR))
                      from bass2.dw_acct_busicharge_dm_$op_month
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff



	set sql_buff "insert into bass1.g_s_22054_month
                     select
                        $op_month
                        ,'$op_month'
                        ,value(Channel_ID,'0')
                        ,char(sum(bigint(NewCustCount     )))
                        ,char(sum(bigint(NewQQTCustCount  )))
                        ,char(sum(bigint(LostCustCount     )))
                        ,char(sum(bigint(LostQQTCustCount  )))
                        ,char(sum(bigint(TerminalCount    )))
                        ,char(sum(bigint(GPRSTerminalCount)))
                        ,char(sum(bigint(OperCount        )))
                        ,char(sum(bigint(ChargeCount      )))
                      from bass1.g_s_22054_month
                     where time_id = 88888888
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff
  

	set sql_buff "delete from bass1.g_s_22054_month where time_id=88888888"
	puts $sql_buff
  exec_sql $sql_buff


	return 0
}


#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------



