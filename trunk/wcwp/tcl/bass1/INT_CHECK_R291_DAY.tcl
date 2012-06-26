######################################################################################################
#程序名称：	INT_CHECK_R291_DAY.tcl
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
        set app_name "INT_CHECK_R291_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R291'					
					)
			"

		exec_sql $sql_buff



##~   R291	月	02_集团客户	集团客户端口资源使用情况接口的“集团客户标识”均在集团客户接口的“集团客户标识”中	"22036 集团客户端口资源使用情况
##~   01004 集团客户"	集团客户端口资源使用情况接口的“集团客户标识”均在集团客户接口的“集团客户标识”中	0.05		"Step1.统计22036（集团客户端口资源使用情况）接口中，截止到统计周期末客户类型=0“集团客户”的“集团客户标识”集合；
##~   Step2.统计01004（集团客户）接口中，截止统计周期末集团客户标识快照集合；
##~   Step3.统计Step1集合不在Step2集客中的集团客户标识个数，若结果大于0，则违反规则。"



	set sql_buff "
		select count(0) from  table (
		 select distinct APP_LENCODE from 
								(
												select t.*
												,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME 
												order by time_id desc ) rn 
												from 
												G_A_22036_DAY  t
												where 
												TIME_ID/100 <= $curr_month										
								  ) a
								where rn = 1  
								and bigint(OPEN_DATE)/100 <= $curr_month
								and  CUST_TYPE = '0'
		) t where 
		 APP_LENCODE not in (                  
		select enterprise_id
		from  table (
					select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $curr_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' 
			)   t                     
		)
		with ur
"

chkzero2 $sql_buff "R291 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R291',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  


	return 0
}
