######################################################################################################
#程序名称：	INT_CHECK_R216_MONTH.tcl
#校验接口：	03017 02054
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  #部分口径差异，后续修正。先解决大于0（同向）的问题。
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
        set app_name "INT_CHECK_R216_MONTH.tcl"



###########################################################

#R216			新增	月	02_集团客户	移动400使用集团客户数与收入同向匹配	移动400使用集团客户数＞0 且 移动400当月总收入＞0	0.05		
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R216') "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
	set sql_buff "
				select a.cnt*1.00 ,value(b.income,0) income
				from
				(select count(0) cnt
							from 
							(
							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
							from (
							select *
							from g_a_02054_day where 
							 time_id/100 <= $op_month and ENTERPRISE_BUSI_TYPE = '1520'
							 and MANAGE_MODE = '3'
							) t 
							) t2 where rn = 1 and STATUS_ID ='1'
				) a,(
				select sum(income)*1.00/100 income
				from (
				select sum(bigint(income)) income from   g_s_03017_month
				where time_id = $op_month
				and ent_busi_id = '1520'
				and MANAGE_MOD = '3'
				union all 
				select sum(bigint(income)) income from   g_s_03018_month
				where time_id = $op_month
				and ent_busi_id = '1520'
				and MANAGE_MOD = '3'
				) t 
				) b 
			with ur
	"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]

 	#set RESULT_VAL [get_single $sql_buff]
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R216',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff
		
 	#检查合法性: 0 - 不正常； 大于0 - 正常
	if {[format %.3f [expr ${RESULT_VAL1} ]] <= 0 } {
		set grade 2
	        set alarmcontent " R216 校验1不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


 	#检查合法性: 0 - 不正常； 大于0 - 正常
	if {[format %.3f [expr ${RESULT_VAL2} ]] <= 0 } {
		set grade 2
	        set alarmcontent " R216 校验2不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

  
      	
	return 0
}
