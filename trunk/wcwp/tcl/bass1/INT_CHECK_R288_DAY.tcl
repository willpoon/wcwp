######################################################################################################
#程序名称：	INT_CHECK_R288_DAY.tcl
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
        set app_name "INT_CHECK_R288_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R288'					
					)
			"

		exec_sql $sql_buff




##~   R288	月	02_集团客户	清单ID与集团客户标识对应关系之间的多对多记录	01007 集团客户与目标市场清单客户对应关系	01007接口中清单客户ID与集团客户标识对应关系不存在多对多记录条数	0.05


##~   统计01007（集团客户与目标市场清单客户对应关系）接口“对应关系状态”为建立的记录中，目标市场清单ID
##~   既在
##~   清单ID与集团客户标识的一对多关系中
##~   又在
##~   清单ID与集团客户标识的多对一关系中
##~   的记录条数，若结果大于0，则违反规则。	

	set sql_buff "
select count(0) from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
and ENTERPRISE_ID in (
select ENTERPRISE_ID from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by ENTERPRISE_ID having count(0) > 1 
) 
and CUST_ID in (
select CUST_ID  from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by CUST_ID having count(0) > 1 
)


"

chkzero2 $sql_buff "R288 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R288',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
	return 0
}
