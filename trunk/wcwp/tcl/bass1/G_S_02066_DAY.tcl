
######################################################################################################		
#接口名称: 用户在电子渠道预约入网情况                                                               
#接口编码：02066                                                                                          
#接口说明：记录用户通过电子渠道预约进行入网情况，主要包括以下两种情况
#程序名称: G_S_02066_DAY.tcl                                                                            
#功能描述: 生成02066的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#		  2. panzw 20120111     根据Q1201060011请各省公司核查2011年11月电子渠道预约办理差异情况 核查，为保证数据一致，暂不报此数据
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#	      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#	      puts $timestamp
#	    #本月 yyyymm
#	    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#	    puts $op_month
#	      
#	    #上个月 yyyymm
#	    set last_month [GetLastMonth [string range $op_month 0 5]]
#	    puts $last_month
#	
#	        #程序名
#	        global app_name
#	        set app_name "G_S_02066_DAY.tcl"
#		
#	  #删除本期数据
#	  	set sql_buff "delete from bass1.G_S_02066_DAY where time_id <= 20111001"
#		exec_sql $sql_buff
#		
#		set sql_buff "delete from bass1.G_S_02066_DAY where time_id=$timestamp"
#		exec_sql $sql_buff
#	
#	
#		
#	  #直接来源于二经用户表数据，新的接口表
#		set sql_buff1 "
#		
#	insert into bass1.G_S_02066_DAY
#	select 
#		$timestamp time_id
#		,t.user_id USER_ID
#		,'BASS1_ST' CHANNEL_ID
#		,'1' FETCH_WAY
#	from ( select b.user_id , row_number()over(partition by b.user_id order by a.create_date desc )	 rn 
#	from  bass2.dw_res_sel_num_cust_log_ds a 
#		,bass2.dw_product_$timestamp b 
#	where   a.BILL_ID = b.product_no 
#		 and b.userstatus_id in (1,2,3,6,8)
#		 and b.usertype_id in (1,2,9)
#		 and a.opt_code in ('ZDYZ')
#		 and date(a.create_date + 5 days ) >= date(b.create_date)
#	) t where rn = 1 	 
#		with ur
#	  "
#	  
#		set sql_buff2 "
#		
#	insert into bass1.G_S_02066_DAY
#	select 
#		$timestamp time_id
#		,t.user_id USER_ID
#		,'BASS1_ST' CHANNEL_ID
#		,'1' FETCH_WAY
#	from ( select b.user_id , row_number()over(partition by b.user_id order by a.create_date desc )	 rn 
#	from  bass2.dw_res_sel_num_cust_log_ds a 
#		,bass2.dw_product_$timestamp b 
#	where   a.BILL_ID = b.product_no 
#		 and b.userstatus_id in (1,2,3,6,8)
#		 and b.usertype_id in (1,2,9)
#		 and a.opt_code in ('ZDYZ')
#		 and date(a.create_date + 5 days ) >= date(b.create_date)
#		 and date(b.create_date) = '$op_time'
#	) t where rn = 1 	 
#		with ur
#	  "  
#	
#	if { $timestamp == 20111001 } {
#		exec_sql $sql_buff1
#	} else {
#		exec_sql $sql_buff2
#	}
#	
#	  #进行结果数据检查
#	  #1.检查chkpkunique
#		set tabname "G_S_02066_DAY"
#		set pk 			"user_id"
#		chkpkunique ${tabname} ${pk} ${timestamp}
#	  

	return 0
}

