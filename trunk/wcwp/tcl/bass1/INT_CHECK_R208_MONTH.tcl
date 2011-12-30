######################################################################################################
#程序名称：	INT_CHECK_R208_MONTH.tcl
#校验接口：	03017 02054
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  #部分口径差异，后续修正。先解决大于0（同向）的问题。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        global app_name
        set app_name "INT_CHECK_R208_MONTH.tcl"


########################################################################################################3


#R208			新增	月	02_集团客户	城管通当月计费客户数	城管通当月计费客户数≤城管通当月使用客户数	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R208') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1370'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and  MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R208',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R208 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }



#R210			新增	月	02_集团客户	警务通当月计费客户数	警务通当月计费客户数≤警务通当月使用客户数	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R210') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1360'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and  MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R210',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R210 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


#R212			新增	月	02_集团客户	校讯通（ADC）当月计费客户数	校讯通（ADC）当月计费客户数≤校讯通（ADC）当月使用客户数	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R212') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1310'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and MANAGE_MOD = '2'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R212',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R212 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


#R214			新增	月	02_集团客户	银信通当月计费客户数	银信通当月计费客户数≤银信通当月使用客户数	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R214') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1380'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R214',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R214 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


	return 0
}
