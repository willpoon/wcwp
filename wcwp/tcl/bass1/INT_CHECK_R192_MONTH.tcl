######################################################################################################
#程序名称：	INT_CHECK_R192_MONTH.tcl
#校验接口：	03017
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
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
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        set app_name "INT_CHECK_R192_MONTH.tcl"


########################################################################################################3
#R192			新增	月	02_集团客户	集团客户统付收入接口中的集团客户标识存在于集团客户资料中	03017(集团客户统付收入)中的"集团客户标识"都在01004(集团客户)的"集团客户标识"中	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R192') "        

	  exec_sql $sqlbuf


	set sql_buff "
	select count(0) from bass1.G_S_03017_MONTH a 
	where 
		time_id = $op_month
		and not exists (select 1 from (select distinct enterprise_id from bass1.G_A_01004_DAY where time_id / 100 <= $op_month ) t where a.enterprise_id = t.enterprise_id )
	with ur
	"
	#获得结果值
 	set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R192',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R192 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

	return 0
}
