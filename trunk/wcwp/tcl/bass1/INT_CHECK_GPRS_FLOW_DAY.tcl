######################################################################################################
#程序名称： INT_CHECK_GPRS_FLOW_DAY.tcl
#校验接口： G_S_04002_DAY.tcl
#运行粒度:  日
#输入参数: 
#输出参数:  返回值:0 成功;-1 失败
#编 写 人： liuqf
#编写时间： 2009-11-23
#问题记录：
#修改历史: 20100125 在网客户口径变动 usertype_id NOT IN ('2010','2020','2030','9000') 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
		set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]		
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
		
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_GPRS_FLOW_DAY.tcl"

 puts " 删除旧数据"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R173','R174') "        
	  exec_sql $sqlbuf


#建立临时表1 存放最新用户状态 参考R027
	set sqlbuf "	declare global temporary table session.int_check_gprs_day_tmp1
									(
								   user_id        CHARACTER(15)
								  ,product_no     CHARACTER(15)
								  ,test_flag      CHARACTER(1)
								  ,sim_code       CHARACTER(15) 
								  ,usertype_id    CHARACTER(4)
									)                            
									partitioning key           
									 (
									   user_id    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	exec_sql $sqlbuf  

	set sqlbuf "	insert into session.int_check_gprs_day_tmp1 (
				 user_id    
				,product_no 
				,test_flag  
				,sim_code   
				,usertype_id  )
			select e.user_id
				,e.product_no  
				,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
				,e.sim_code
				,f.usertype_id           
	from (select user_id , create_date ,product_no,sim_code,usertype_id
							,row_number() over(partition by user_id order by time_id desc ) row_id   
		  from bass1.g_a_02004_day
		  where time_id<=$timestamp ) e 
	inner join ( select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
					   from bass1.g_a_02008_day
					   where time_id<=$timestamp ) f on f.user_id=e.user_id 
	where e.row_id=1 and f.row_id=1   
	and f.usertype_id not IN ('2010','2020','2030','9000') 
	and  e.usertype_id in ('1','2')
	with ur"
	exec_sql $sqlbuf  

set sql_buff "create index session.idxgprsflowcheck  on session.int_check_gprs_day_tmp1(product_no)"

exec_sql $sql_buff  

 puts "R173	日	GPRS上行流量日变动率<= 50%  "
   ##~   set sqlbuf " 
				##~   select val1
							##~   ,val3
							##~   ,decimal(1.00*(val1-val3)/val3,9,4) rate 
				##~   from 
				 ##~   (select sum(bigint(a.UP_FLOWS)) val1
					##~   from bass1.G_S_04002_DAY a,
					     ##~   session.int_check_gprs_day_tmp1 b 
					##~   where a.product_no = b.product_no 
						##~   and b.usertype_id not IN ('2010','2020','2030','9000') 
						##~   and b.test_flag='0'
					  ##~   and a.time_id=$timestamp ) M
				##~   ,(select sum(bigint(c.UP_FLOWS)) val3
					##~   from bass1.G_S_04002_DAY c,
					     ##~   session.int_check_gprs_day_tmp1 d 
					##~   where c.product_no = d.product_no 
						##~   and d.usertype_id not IN ('2010','2020','2030','9000') 
						##~   and d.test_flag='0'
					  ##~   and c.time_id=$last_day ) N 
    ##~   "
   ##~   set p_row [get_row $sqlbuf]
   ##~   set RESULT_VAL1 [lindex $p_row 0]
	 ##~   set RESULT_VAL2 [lindex $p_row 1]
	 ##~   set RESULT_VAL3 [lindex $p_row 2]  
   set sqlbuf " 
				 select  sum(bigint(a.UP_FLOWS)) val1
						,sum(bigint(a.DOWN_FLOWS)) val2
					from ( select product_no,UP_FLOWS,DOWN_FLOWS from  bass1.G_S_04002_DAY_THIS a where a.time_id=$timestamp )  a
					     ,session.int_check_gprs_day_tmp1 b 
						where a.product_no = b.product_no 
			"											  
	set p_row [get_row $sqlbuf]
	set RESULT_VAL1 [lindex $p_row 0]
	set RESULT_VAL11 [lindex $p_row 1]


   set sqlbuf " 
				 select  sum(bigint(a.UP_FLOWS)) val1
						,sum(bigint(a.DOWN_FLOWS)) val2
					from ( select product_no,UP_FLOWS,DOWN_FLOWS from  bass1.G_S_04002_DAY_PREV a where a.time_id=$last_day )  a
					     ,session.int_check_gprs_day_tmp1 b 
						where a.product_no = b.product_no 
			"											  
	set p_row [get_row $sqlbuf]
	set RESULT_VAL2 [lindex $p_row 0]
	set RESULT_VAL22 [lindex $p_row 1]

   set sqlbuf " 
			values decimal(1.00*($RESULT_VAL1-$RESULT_VAL2)/$RESULT_VAL2,9,4)
			"
			
set RESULT_VAL3 [get_single $sqlbuf]

   set sqlbuf " 
			values decimal(1.00*($RESULT_VAL11-$RESULT_VAL22)/$RESULT_VAL22,9,4)
			"
			
set RESULT_VAL33 [get_single $sqlbuf]


					  
 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R173',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.45||$RESULT_VAL3<-0.45 } {
		set grade 2
	  set alarmcontent "R173校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R173校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.30||$RESULT_VAL3<-0.30 } {
		set grade 2
	  set alarmcontent "R173校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 



 puts "R174	日	GPRS下行流量日变动率<= 50% "

   ##~   set sqlbuf " 
				##~   select val1
							##~   ,val3
							##~   ,decimal(1.00*(val1-val3)/val3,9,4) rate 
				##~   from 
				 ##~   (select sum(bigint(a.DOWN_FLOWS)) val1
					##~   from bass1.G_S_04002_DAY a,
					     ##~   session.int_check_gprs_day_tmp1 b 
					##~   where a.product_no = b.product_no 
						##~   and b.usertype_id not IN ('2010','2020','2030','9000') 
						##~   and b.test_flag='0'
					  ##~   and a.time_id=$timestamp ) M
				##~   ,(select sum(bigint(c.DOWN_FLOWS)) val3
					##~   from bass1.G_S_04002_DAY c,
					     ##~   session.int_check_gprs_day_tmp1 d 
					##~   where c.product_no = d.product_no 
						##~   and d.usertype_id not IN ('2010','2020','2030','9000') 
						##~   and d.test_flag='0'
					  ##~   and c.time_id=$last_day ) N 
    ##~   "

   ##~   set p_row [get_row $sqlbuf]
   ##~   set RESULT_VAL1 [lindex $p_row 0]
	 ##~   set RESULT_VAL2 [lindex $p_row 1]
	 ##~   set RESULT_VAL3 [lindex $p_row 2]  



   ##~   set sqlbuf " 
				 ##~   select sum(bigint(a.UP_FLOWS)) val1
					##~   from ( select product_no,UP_FLOWS from  bass1.G_S_04002_DAY a where a.time_id=$timestamp )  a
					     ##~   ,(select distinct product_no from session.int_check_gprs_day_tmp1 b
								##~   where b.usertype_id not IN ('2010','2020','2030','9000') 
									##~   and b.test_flag='0'
						  ##~   ) b 
						##~   where a.product_no = b.product_no 
			##~   "											  
##~   set RESULT_VAL1 [get_single $sqlbuf]


   ##~   set sqlbuf " 
				 ##~   select sum(bigint(a.UP_FLOWS)) val1
					##~   from ( select product_no,UP_FLOWS from  bass1.G_S_04002_DAY a where a.time_id=$last_day )  a
					     ##~   ,(select distinct product_no from session.int_check_gprs_day_tmp1 b
								##~   where b.usertype_id not IN ('2010','2020','2030','9000') 
									##~   and b.test_flag='0'
						  ##~   ) b 
						##~   where a.product_no = b.product_no 
			##~   "											  
##~   set RESULT_VAL2 [get_single $sqlbuf]

   ##~   set sqlbuf " 
			##~   values decimal(1.00*($RESULT_VAL1-$RESULT_VAL2)/$RESULT_VAL2,9,4)
			##~   "
##~   set RESULT_VAL3 [get_single $sqlbuf]




 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R174',$RESULT_VAL11,$RESULT_VAL22,$RESULT_VAL33,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL33>0.45||$RESULT_VAL33<-0.45 } {
		set grade 2
	  set alarmcontent "R174校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 




##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL33>0.55||$RESULT_VAL33<-0.55 } {
		set grade 2
	  set alarmcontent "R174校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL33>0.30||$RESULT_VAL33<-0.30 } {
		set grade 2
	  set alarmcontent "R174校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 


	return 0
}
