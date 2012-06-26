######################################################################################################
#程序名称：	INT_CHECK_R301_DAY.tcl
#校验接口：	
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-06-09 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #程序名
        global app_name
        set app_name "INT_CHECK_R301_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					 'R301' -- 未校验					
					)
			"

		exec_sql $sql_buff



##~   R301	月	02_集团客户	IDC使用集团客户到达数	02054 集团客户业务订购关系	IDC使用集团客户到达数环比绝对值小于等于50%	0.05		"Step1.02054（集团客户业务订购关系）接口中，截止统计月订购状态正常的，业务类型编码为1190（IDC）的当月和上月集团客户标识去重个数；
##~   Step2.当月值与上月值比较。"


##~   无业务



	return 0
}
