######################################################################################################
#程序名称：	INT_CHECK_R181_MONTH.tcl
#校验接口：	03018
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
        set app_name "INT_CHECK_R181_MONTH.tcl"



########################################################################################################3
#R181			新增	月	02_集团客户	集团业务个人非统付收入关系接口中的用户标识存在于用户表中	"02004 用户
#03018 集团业务个人非统付收入"	03018(集团业务个人非统付收入)中的"用户标识"都在02004(用户)的"用户标识"中	0.05	


 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R181') "        

	  exec_sql $sqlbuf


	set sql_buff "
	select count(0) from bass1.G_S_03018_MONTH a 
	where 
		time_id = $op_month
		and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day ) t where a.user_id = t.user_id )
	with ur
	"
	#获得结果值
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R181',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R181 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


# 2011.12.03
  #1.检查chkpkunique
        set tabname "g_i_02016_month"
        set pk                  "USER_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        
        
#2011.12.03

	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '021000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 1 "


	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '031000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 1 "


	return 0
}

