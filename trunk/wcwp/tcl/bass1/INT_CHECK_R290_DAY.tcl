######################################################################################################
#程序名称：	INT_CHECK_R290_DAY.tcl
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
        set app_name "INT_CHECK_R290_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R290' -- 未校验					
					)
			"

		exec_sql $sql_buff



##~   R290	月	02_集团客户	商户管家订购的终端与其所在集团客户标识的订购一一对应	"02054 集团业务订购关系
##~   02059 集团业务个人用户绑定关系
##~   02065 商户管家业务订购关系"	订购商户管家业务的终端所在集团客户也须有商户管家业务的订购关系记录	0.05		"Step1.统计02059（集团业务个人用户绑定关系）接口中，截止统计月末订购状态正常，订购业务为“商户管家”产品的非空集团客户标识集合；
##~   Step2.统计02054（集团业务订购关系）和02065（商户管家业务订购关系）接口中订购状态正常，订购业务为“商户管家”产品的集团客户标识集合；
##~   Step3.统计Step1集合不在Step2集合的集团客户标识个数，若结果大于0，则违反规则。"	

##~   无数据，暂不校验！





	return 0
}
