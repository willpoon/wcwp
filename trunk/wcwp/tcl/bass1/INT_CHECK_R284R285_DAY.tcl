######################################################################################################
#程序名称：	INT_CHECK_R284R285_DAY.tcl
#校验接口：	22038
#运行粒度: DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-05-31
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
      #自然月 上月 01 日
      set last_month_first_day ${op_month}01
      set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_R284R285_DAY.tcl"


  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R284','R285') "        
  exec_sql $sqlbuf


########################################################################################################3
##~   R284	日	13_垃圾信息	垃圾短信黑名单中的用户标识	"22420 垃圾短信黑名单
##~   02004 用户"	“垃圾短信黑名单”中的“用户标识”都应该在“用户”表中存在	0.05		统计22420截至当日快照中，用户标识不在02004的当日快照中的个数。	西藏无用户上报，可暂不校验




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "			
		select count(0) from G_I_22420_DAY a where time_id = $timestamp
		and  not exists (select 1 from  bass1.g_a_02004_02008_stage b where a.user_id = b.user_id ) 
		with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

##~   set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R284',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL1} ]] != 0 } {
                set grade 2
                set alarmcontent " R284 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }






########################################################################################################3
##~   R285	日	13_垃圾信息	垃圾短信疑似黑名单处理结果中的用户标识	"22421 垃圾短信疑似黑名单处理结果
##~   02004 用户"	“垃圾短信疑似黑名单处理结果”中的“用户标识”都应该在“用户”表中存在	0.05		计算22420中入库日期为统计日，用户标识不在02004的当日快照中的个数。	西藏无用户上报，可暂不校验

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
		select count(0) from G_S_22421_DAY a where time_id = $timestamp
		and  not exists (select 1 from  bass1.g_a_02004_02008_stage b where a.user_id = b.user_id ) 
		with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

##~   set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R285',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R285 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

	return 0
}

