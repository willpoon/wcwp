
proc getUserList { op_time optime_month } {

puts $op_time
puts $optime_month

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set this_month_last_day [string range $curr_month 0 5][GetThisMonthDays [string range $curr_month 0 5]01]

puts $optime
puts $curr_month

puts $timestamp
puts $this_month_last_day
	
	
	
set sql_buff "
	delete from  G_A_02004_02008_NEWANDLEAVE where time_id = $timestamp
		"
		exec_sql $sql_buff
	
set sql_buff "
		insert into G_A_02004_02008_NEWANDLEAVE
		SELECT 
			$timestamp, USER_ID
			,'LEA1'
		FROM (
		select user_id
		from   bass1.g_a_02004_02008_stage 
								where USERSTATUS IN ('2010','2020','2030','9000')
								  and test_flag='0'
								  and time_id=$timestamp
		except
		select user_id 
		from bass2.dw_product_$timestamp
		where usertype_id in (1,2,9) 
		 and day_off_mark = 1 
		 and userstatus_id not in (1,2,3,6,8)
		 and test_mark<>1
		 ) T
		WITH UR
		"
		exec_sql $sql_buff
set sql_buff "
		insert into G_A_02004_02008_NEWANDLEAVE
		SELECT 
			$timestamp, USER_ID
			,'LEA2'
		FROM (
		select user_id 
		from bass2.dw_product_$timestamp
		where usertype_id in (1,2,9) 
		 and day_off_mark = 1 
		 and userstatus_id not in (1,2,3,6,8)
		 and test_mark<>1
		except 
		 select user_id
		from   bass1.g_a_02004_02008_stage 
				where USERSTATUS IN ('2010','2020','2030','9000')
				  and test_flag='0'
				  and time_id=$timestamp
		 ) T
		WITH UR
		"
		exec_sql $sql_buff
		
set sql_buff "
		insert into G_A_02004_02008_NEWANDLEAVE
		SELECT 
			$timestamp, USER_ID
			,'NEW1'
		FROM (
			select user_id from G_A_02004_02008_STAGE
			where create_date = '$timestamp'
			and test_flag='0'
			except
			select user_id
			from bass2.dw_product_$timestamp
			where usertype_id in (1,2,9) 
			and day_new_mark = 1 and test_mark<>1
		 ) T
		WITH UR
		"
		exec_sql $sql_buff
		

set sql_buff "
		insert into G_A_02004_02008_NEWANDLEAVE
		SELECT 
			$timestamp, USER_ID
			,'NEW2'
		FROM (
			select user_id
			from bass2.dw_product_$timestamp
			where usertype_id in (1,2,9) 
			and day_new_mark = 1 and test_mark<>1
			except
		select user_id from G_A_02004_02008_STAGE
			where create_date = '$timestamp'
			and test_flag='0'

		 ) T
		WITH UR
		"
		exec_sql $sql_buff
		
return 0  

}

