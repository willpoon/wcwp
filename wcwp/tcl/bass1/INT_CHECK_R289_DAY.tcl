######################################################################################################
#程序名称：	INT_CHECK_R289_DAY.tcl
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
        set app_name "INT_CHECK_R289_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R289' -- 未校验					
					)
			"

		exec_sql $sql_buff




##~   R289	月	02_集团客户	商户管家集团客户与业务方式的一一对应	02065 商户管家业务订购关系	02065接口中，订购商户管家业务的集团客户只能对应一种业务方式，单机版或者网络版	0.05		

##~   02065（商户管家业务订购关系）接口中，统计集团客户标识对应去重的“业务方式”个数，若有1家以上的集团客户对应两种业务方式，则违反规则。



##~   G_A_02065_DAY


##~   --~   select count(0)
##~   --~   from
##~   --~   (
##~   --~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
##~   --~   from G_A_02065_DAY a
##~   --~   where time_id / 100 <= $curr_month
##~   --~   ) t where t.rn =1  and CHNL_STATE = '1'


##~   select count(0) from G_A_02065_DAY

##~   无数据，暂不校验！




	return 0
}
