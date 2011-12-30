######################################################################################################
#程序名称：	INT_CHECK_R255_MONTH.tcl
#校验接口：	03004
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
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
        set app_name "INT_CHECK_R255_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R255') 
 	  "        

	  exec_sql $sqlbuf



#60% ≤（实体渠道放号量/当月新增客户数）×100% ＜ 100%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "
select b.chnl_new_cnt,a.all_cnt , b.chnl_new_cnt*1.00/a.all_cnt
from (
select count(distinct user_id) all_cnt from    bass1.int_02004_02008_month_stage 
where bigint(create_date)/100 = $op_month
) a ,
(
select sum(bigint(NEW_USER_CNT)) chnl_new_cnt 
from G_S_22091_DAY
where time_id / 100 = $op_month
) b where 1 = 1

"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R255',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # 校验值超标时告警	
	if { $RESULT_VAL3 >= 1.00||$RESULT_VAL3 < 0.60 } {
		set grade 2
	  set alarmcontent "R255 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
