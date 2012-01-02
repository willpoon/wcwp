######################################################################################################
#程序名称：	INT_CHECK_R258_DAY.tcl
#校验接口：	02054
#运行粒度: DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  2011.11.14 根据集团勘误修改R258口径
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
      
        #程序名
        set app_name "INT_CHECK_R258_DAY.tcl"



########################################################################################################3


##	#R258	
##	日	10_资费订购	统一资费办理量和生效量关系	"02022 用户选择全球通全网统一基础资费套餐
##	02024 全球通基础资费套餐用户成功办理量"	02022新增用户中有成功办理量的用户占比≥98%	0.05	"第一步：取02022接口中生效日期等于统计日，失效日期大于统计日的用户标识和基础套餐标识；
##	第二步：取02024中所有用户标识和基础套餐标识。
##	第三步：第一步的结果通过用户标识和基础套餐标识左关联第二步的结果，能关联上的即为统计日有成功办理量的新增用户；
##	第四步：第三步的用户数除以第一步的用户数，根据比值进行判断。"
##	
  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R258') "        


	  exec_sql $sqlbuf


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
	select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
	from 
	(
	select USER_ID,BASE_PKG_ID from   
	G_I_02022_DAY  a
	where time_id = $timestamp and VALID_DT = '$timestamp'
	) a
	left join (
		select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
		from (
			select  USER_ID,BASE_PKG_ID from 
			G_S_02024_DAY a
			where time_id >= $last_month_first_day and time_id <= $timestamp 
		     ) a 
		left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
	) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
	with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]
        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R258',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
                
        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] < 0.98 } {
                set grade 2
                set alarmcontent " R258 校验1不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

## 2011.11.14 根据集团口径修改，保留原校验，另起名为R258_2
  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R258_2') "        


	  exec_sql $sqlbuf


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
	select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
	from 
	(
	select USER_ID,BASE_PKG_ID from   
	G_I_02022_DAY  a
	where time_id = $timestamp and VALID_DT = '$timestamp'
	) a
	left join (
			select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
		from (
			select  USER_ID,BASE_PKG_ID from 
			G_S_02024_DAY a
			where time_id <= $timestamp 
		     ) a 
		left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
		) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
	with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]
        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R258_2',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
                
        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] < 0.98 } {
                set grade 2
                set alarmcontent " R258_2 校验2不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }


	return 0
}

