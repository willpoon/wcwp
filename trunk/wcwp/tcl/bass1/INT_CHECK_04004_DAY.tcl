######################################################################################################
#程序名称：INT_CHECK_04004_DAY.tcl
#校验接口：G_S_04004_DAY.tcl
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-30
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        #本月 yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#            
#        #上月  yyyymm
#        set last_month [GetLastMonth [string range $op_month 0 5]]
#        
#        #自然月第一天 yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
#        
#        #本月第一天 yyyymmdd
#        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_R063_DAY.tcl"



   #--R085 点对点彩信的信息费为零；且上行只有通信费用，下行无通信费用
   set sqlbuf " select count(*)
                from BASS1.G_S_04004_DAY
                where time_id= $timestamp
                and BUS_SRV_ID in('4','1')
                and bigint(info_fee)>0
                and MM_KIND='1'
               "
   puts $sqlbuf               
   set RESULT_VAL1 [get_single $sqlbuf]
   puts $RESULT_VAL1
   set sqlbuf " select value(sum(bigint(info_fee)),0) from BASS1.G_S_04004_DAY
                where time_id= $timestamp
                and BUS_SRV_ID in('4','1')
                and MM_BILL_TYPE='00'
               "
   puts $sqlbuf
   set RESULT_VAL2 [get_single $sqlbuf]

   puts $RESULT_VAL2
   set sqlbuf " select value(sum(bigint(call_fee)),0) from BASS1.G_S_04004_DAY
                where time_id= $timestamp
                and BUS_SRV_ID in('4','1')
                and MM_BILL_TYPE='03'
               "
   puts $sqlbuf                
   set RESULT_VAL3 [get_single $sqlbuf]
   
   puts $RESULT_VAL3
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R085',$RESULT_VAL1,$RESULT_VAL2,0,0) " 
   puts $sqlbuf      
   exec_sql $sqlbuf

   puts $RESULT_VAL1
   puts $RESULT_VAL2
	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 || $RESULT_VAL3 > 0 } {
		set grade 2
	        set alarmcontent "R085校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R085 点对点彩信的信息费为零；且上行只有通信费用，下行无通信费用"

		 		 		 		 		 


   #--R086 梦网彩信 梦网彩信的上行只有通信费；下行只有信息费和包月费
   set sqlbuf "select value(sum(bigint(info_fee)),0) from BASS1.G_S_04004_DAY
               where time_id=$timestamp
               and BUS_SRV_ID ='2'
               and MM_BILL_TYPE='00'
              "
   puts $sqlbuf               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set sqlbuf " select value(sum(bigint(call_fee)),0) from BASS1.G_S_04004_DAY
                where time_id=$timestamp
                and BUS_SRV_ID ='2'
                and MM_BILL_TYPE in ('02','03')
               "
   puts $sqlbuf                
   set RESULT_VAL2 [get_single $sqlbuf]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R086',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 } {
		set grade 2
	        set alarmcontent "R086校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R085	梦网彩信的上行只有通信费；下行只有信息费和包月费"
		 		 

	return 0
}


#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}