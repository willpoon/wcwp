######################################################################################################
#程序名称：	INT_CHECK_R229_MONTH.tcl
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
        set app_name "INT_CHECK_R229_MONTH.tcl"



###########################################################

#R229			新增	月	04_TD业务	话单汇总计算出的"通话客户数"和用户使用终端通话情况中的通话用户数的偏差	02047中的通话用户数与通过话单汇总计算出的通话客户数的偏差在3%以内	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month 
 	  		and rule_code in ('R229') 
 	  		"        
	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
	set sql_buff "
	select a.val1*1.0000 , b.val2*1.0000
				from 
				(
				select count(distinct product_no ) val1 
				from     bass1.G_S_02047_MONTH
				where time_id = $op_month

			 ) a,
				(
                    select count(0) val2 from  
											(
											select   distinct product_no from        G_S_21003_MONTH where time_id = $op_month
											union 
											select   distinct product_no from        G_S_21006_MONTH where time_id = $op_month
											union 
											select   distinct product_no from        G_S_21009_DAY where time_id/100 = $op_month
											) a
													 
				) b
with ur  
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.03||$RESULT_VAL<-0.03 } {
		set grade 2
	  set alarmcontent "R229 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R229',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff

  	
	return 0
}
