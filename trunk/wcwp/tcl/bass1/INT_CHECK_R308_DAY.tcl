######################################################################################################
#程序名称：	INT_CHECK_R308_DAY.tcl
#校验接口：	
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-06-09 
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
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #程序名
        global app_name
        set app_name "INT_CHECK_R308_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R308'					
					)
			"

		exec_sql $sql_buff




##~   --R308	月	02_集团客户	移动CRM使用集团客户到达数	02054 集团客户业务订购关系	移动CRM使用集团客户到达数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,2)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R308',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R308 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




	return 0
}
