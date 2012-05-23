######################################################################################################
#程序名称：INT_CHECK_COMP_KPI_DAY.tcl
#校验接口：G_S_22073_DAY.tcl
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：
#编写时间：2009-09-08
#问题记录：
#修改历史: 20091123 增加R163-R172 10个规则校验
#修改历史: 2011-05-26 15:13:43 R171,R172 40% -> 50%
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_COMP_KPI_DAY.tcl"
		set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]		
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]

 puts " 删除旧数据"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp 
 	                and rule_code in ('R153','R154','R155','R156','R157','R158','R163','R164','R165','R166','R167','R168','R169','R170','R171','R172') "        
	  exec_sql $sqlbuf     


 puts "R153	日	(当日联通新增客户数-当日联通离网客户数)与(当日联通客户到达数-昨日联通客户到达数)的偏差在1%以内"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from 
				 (select ( int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) )- 
								 ( int(UNION_MOBILE_LOST_CNT)+int(UNION_NET_LOST_CNT)+int(UNION_FIX_LOST_CNT) ) val1
								 ,int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val2
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R153',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R153校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R154	日	(当日联通移动新增客户数-当日联通移动离网客户数)与(当日联通移动客户到达数-昨日联通移动客户到达数)偏差在1％以内"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from 
				 (select ( int(UNION_MOBILE_NEW_ADD_CNT)- int(UNION_MOBILE_LOST_CNT) ) val1
								 ,int(UNION_MOBILE_ARRIVE_CNT)  val2
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R154',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R154校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	
		 		 		 		 
 puts "R155	日	(当日电信新增客户数-当日电信离网客户数)与（当日电信客户到达数-昨日电信客户到达数）的偏差在1%以内"
   set sqlbuf " 
			select val1
						,val2-val3
						,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
			from
			 (select ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) )- 
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) val1
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val2
				from bass1.G_S_22073_DAY
				where time_id=$timestamp ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R155',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R155校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R156	日	(当日电信移动新增客户数-当日电信移动离网客户数)与(当日电信移动客户到达数-昨日电信移动客户到达数)偏差在1％以内"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from
				(select ( int(TEL_MOBILE_NEW_ADD_CNT)- int(TEL_MOBILE_LOST_CNT) ) val1
						 ,int(TEL_MOBILE_ARRIVE_CNT)  val2
				from bass1.G_S_22073_DAY
				where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R156',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R156校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R157	日	当月联通移动用户数≤联通移动客户到达数"
   set sqlbuf " 
		select int(M_UNION_MOBILE_CNT)
					,int(UNION_MOBILE_ARRIVE_CNT)
					,int(UNION_MOBILE_ARRIVE_CNT)-int(M_UNION_MOBILE_CNT)
		from bass1.G_S_22073_DAY
		where time_id=$timestamp
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R157',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3<0 } {
		set grade 2
	  set alarmcontent "R157校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R158	日	当月电信移动用户数≤电信移动客户到达数"
   set sqlbuf " 
				select int(M_TEL_MOBILE_CNT)
							,int(TEL_MOBILE_ARRIVE_CNT)
							,int(TEL_MOBILE_ARRIVE_CNT)-int(M_TEL_MOBILE_CNT)
				from bass1.G_S_22073_DAY
				where time_id=$timestamp
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R158',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3<0 } {
		set grade 2
	  set alarmcontent "R158校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##########################################################################
###1.6.4规范新增的部分校验
##########################################################################

 puts "R163	日	联通到达客户数日变动率≤1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R163',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R163校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R164	日	电信到达客户数日变动率≤1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R164',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R164校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R165	日	联通移动客户总数日变动率≤1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_MOBILE_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R165',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R165校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R166	日	电信移动客户总数日变动率≤1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_MOBILE_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R166',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R166校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R167	日	联通固话客户总数日变动率≤1% "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R167',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R167校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



 puts "R168	日	电信固话客户总数日变动率≤1%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R168',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R168校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



 puts "R169	日	联通新增客户数日变动率≤ 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R169',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R169校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R169校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R169校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 puts "R170	日	电信新增客户数日变动率≤ 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R170',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R170校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R170校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R170校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 
 puts "R171	日	联通移动新增客户数日变动率≤ 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(UNION_MOBILE_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(UNION_MOBILE_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R171',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R171校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R171校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R171校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 

 puts "R172	日	电信移动新增客户数日变动率≤ 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
			    with ur
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " 将校验值插入校验表里 "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R172',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # 校验值超标时告警	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R172校验不通过"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012新版校验！
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R172校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R172校验达到临界值"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 

	return 0
}

