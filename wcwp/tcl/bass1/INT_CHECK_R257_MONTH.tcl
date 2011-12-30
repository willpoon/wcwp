######################################################################################################
#程序名称：	INT_CHECK_R257_MONTH.tcl
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
        set app_name "INT_CHECK_R257_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R257') 
 	  "        

	  exec_sql $sqlbuf



#有渠道标识的新增用户占比 ≥ 85%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "



select fee1+fee2 val1,fee1+fee2+fee3+fee4+fee5 val2 ,(fee1+fee2)*1.00/(fee1+fee2+fee3+fee4+fee5) val3
from 
(
select sum(bigint(CHRG_REC_FEE)) fee1 from G_S_22066_DAY
where time_id / 100 = $op_month
) a,
(
select sum(bigint(BNK_TS_FEE)) fee2 from G_S_22068_MONTH
where time_id  = $op_month
) b,
(
select sum(bigint(PAYMENT_REC_FEE)) fee3 from G_S_22091_DAY
where time_id/100  = $op_month
and DEAL_TYPE = '1'
) c,
(
select sum(bigint(CHRG_AMT))  fee4 from G_S_22048_DAY
where time_id/100  = $op_month
) d,
(
select sum(bigint(COUNTER_REC_FEE)+bigint(EBANK_REC_FEE)+bigint(OTH_REC_FEE)) fee5 from G_S_22068_MONTH
where time_id  = $op_month
) e
with ur
"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R257',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # 校验值超标时告警	
	if { $RESULT_VAL3 < 0.05 } {
		set grade 2
	  set alarmcontent "R257 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
