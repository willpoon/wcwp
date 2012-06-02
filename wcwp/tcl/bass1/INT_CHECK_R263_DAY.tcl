######################################################################################################
#程序名称：	INT_CHECK_R263_DAY.tcl
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
        set app_name "INT_CHECK_R263_DAY.tcl"


  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R263') "        
  exec_sql $sqlbuf

########################################################################################################3

##~   --~   R263	日	03_话单日志	详单计算出的短信计费量与汇总接口上报的短信计费量之间的平衡关系	
##~   --~   "04005 梦网短信话单
##~   --~   21007 普通短信业务日使用
##~   --~   22012 日KPI"	通过各详单计算出的短信计费量＝22012中的短信计费量	0.05		取KPI表中的短信计费量，与统计日的22012中的短信计费量比较，若相等，则通过该规则

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			select *
			from (
			select (
					select count(0) from   (
							select  PRODUCT_NO  from bass1.g_s_04005_day 
							where time_id =$timestamp
							and calltype_id in ('00','01','10','11')
							and sms_status='0'
					) a
			) + (
				select sum(bigint(SMS_COUNTS)) from bass1.g_s_21007_day
				where time_id =$timestamp
				  and SVC_TYPE_ID in ('11','12','13','70')
				  and END_STATUS='0'
				  and CDR_TYPE_ID in ('00','10','21')
			 ) from bass2.dual
			) a , ( 		
					select bigint(M_BILL_SMS) from G_S_22012_DAY where time_id = $timestamp
				) b where 1 = 1
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R263',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R263 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }






########################################################################################################3

##~   --~   R264	日	03_话单日志	详单计算出的行业网关短信计费量与汇总接口上报的行业网关短信计费量之间的平衡关系	"04016 行业网关短信话单
##~   --~   22012 日KPI"	通过各详单计算出的行业短信计费量＝22012中的行业短信计费量	0.05		"第一步：计算04016中入库日期为统计日,短信话单类型编码属于（00（SMO）,01（SMT）, 10（SMO_F）,11 （SMT_F）），且发送状态为成功的记录数，即为当日的行业短信计费量；
##~   --~   第二步：取22012中入库日期为统计日的行业短信计费量，并与第一步的结果进行比对。"	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			select *
			from (
			select count(0) from    G_S_04016_DAY where time_id = $timestamp
				and RECORD_TYPE in ('00','01','10','11')
				and SEND_STATUS = '0'
			) a , ( 		
					select bigint(M_BILL_HANGYE_SMS) from G_S_22012_DAY where time_id = $timestamp
				) b where 1 = 1
				with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--将校验值插入校验结果表
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R264',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R264 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

	return 0
}

