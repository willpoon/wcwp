######################################################################################################
#程序名称：	INT_CHECK_R190_DAY.tcl
#校验接口：	02058
#运行粒度: DAY
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
				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        set app_name "INT_CHECK_R190_DAY.tcl"



########################################################################################################3

#R190			新增	月	02_集团客户	动力100业务订购情况接口中的集团客户标识存在于集团客户资料中	02058(动力100业务订购情况)中的"集团客户标识"都在01004(集团客户)的"集团客户标识"中	0.05		


 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R190') "        

	  exec_sql $sqlbuf

 	set RESULT_VAL 0
	set sql_buff "
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02058_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN bass1.dim_trans_enterprise_id B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur
		"
  #获得结果值
 	set RESULT_VAL [get_single $sql_buff]

	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R190',$RESULT_VAL,0,0,0) 
		"
	exec_sql $sql_buff
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R190 校验不通过"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

