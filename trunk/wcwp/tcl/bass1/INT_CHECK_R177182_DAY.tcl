######################################################################################################
#程序名称：INT_CHECK_R177182_DAY.tcl
#校验接口：02059
#运行粒度: DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  add:02059(集团业务个人用户绑定关系)中的"用户标识"都在02004(用户)的"用户标识"中
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        set app_name "INT_CHECK_R177182_DAY.tcl"



########################################################################################################3
	# R177 02059(集团业务个人用户绑定关系)中的"用户标识"都在02004(用户)的"用户标识"中


 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R177','R182') "        

	  exec_sql $sqlbuf


	set sql_buff "
        select count(0) from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02059_DAY  t
                        where time_id >= 20100626
          ) a
        where rn = 1	and 	STATUS_ID = '1'
    		and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
		with ur
	"
	#获得结果值
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R177',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R177 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

	# R182 02059(集团业务个人用户绑定关系)中非空的"集团客户标识"都在01004(集团客户)的"集团客户标识"中
#集团端在26日零点之前将所有旧的集团客户ID置换成新的集团客户ID。
#集团端在26日零点之前将已有的所有数据置为24日的失效，并将上传的全量数据入库

 	set RESULT_VAL 0
	set sql_buff "
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02059_DAY  t
											where time_id >= 20100626
			  ) a
			where rn = 1	and STATUS_ID = '1'
			AND trim(A.enterprise_id) <> ''
			and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur
		"
  #获得结果值
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R182',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff
 	
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R182 校验不通过"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

