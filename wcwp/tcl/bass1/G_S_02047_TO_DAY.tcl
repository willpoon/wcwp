######################################################################################################
#接口名称：日语音清单累计
#接口编码：02047
#接口说明：日语音清单相关字段汇总
#程序名称: G_I_02047_TO_DAY.tcl
#功能描述: 生成bass1.cdr_call_dm的数据
#运行粒度: 月
#源    表：1.bass2.cdr_call_yyyymmdd --'TR1_L_05001'
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-04
#问题记录：
#修改历史: 20090907 liuzhiong 修改 1.只保存60天数据 2.对于异常IMEI信息直接加进 where条件里
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month
              

        #本月第一天 yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day


			 #上月当天  yyyymmdd
			 set last_month_day [GetLastMonth [string range $op_month 0 5]][string range $op_time 8 9]
			 
			 
			 
  #删除本期数据  以及只保存两个月数据
	set sql_buff "delete from BASS1.CDR_CALL_DM where ( time_id=$timestamp or time_id<=$last_month_day )"
	puts $sql_buff
  exec_sql $sql_buff
  
  
##  #插入本期数据
##	set sql_buff "insert into BASS1.CDR_CALL_DM select $timestamp, USER_ID, PRODUCT_NO, IMEI from bass2.cdr_call_$timestamp"
##	puts $sql_buff
##  exec_sql $sql_buff
##       
##
##  
##
##  #删除异常IMEI信息  
##	set sql_buff "delete from BASS1.CDR_CALL_DM where imei is null"
##	puts $sql_buff
##  exec_sql $sql_buff
##  
##  
##  #删除异常IMEI信息  
##	set sql_buff "delete from BASS1.CDR_CALL_DM where
##                  (
##                  (ascii(substr(imei,1 ,1)) between  65 and 90 or ascii(substr(imei,1 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,2 ,1)) between  65 and 90 or ascii(substr(imei,2 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,3 ,1)) between  65 and 90 or ascii(substr(imei,3 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,4 ,1)) between  65 and 90 or ascii(substr(imei,4 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,5 ,1)) between  65 and 90 or ascii(substr(imei,5 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,6 ,1)) between  65 and 90 or ascii(substr(imei,6 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,7 ,1)) between  65 and 90 or ascii(substr(imei,7 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,8 ,1)) between  65 and 90 or ascii(substr(imei,8 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,9 ,1)) between  65 and 90 or ascii(substr(imei,9 ,1)) between 97  and 122) or 
##                  (ascii(substr(imei,10,1)) between  65 and 90 or ascii(substr(imei,10,1)) between 97  and 122) or 
##                  (ascii(substr(imei,11,1)) between  65 and 90 or ascii(substr(imei,11,1)) between 97  and 122) or 
##                  (ascii(substr(imei,12,1)) between  65 and 90 or ascii(substr(imei,12,1)) between 97  and 122) or 
##                  (ascii(substr(imei,13,1)) between  65 and 90 or ascii(substr(imei,13,1)) between 97  and 122) or 
##                  (ascii(substr(imei,14,1)) between  65 and 90 or ascii(substr(imei,14,1)) between 97  and 122) or 
##                  (ascii(substr(imei,15,1)) between  65 and 90 or ascii(substr(imei,15,1)) between 97  and 122) 
##                  )"
##	puts $sql_buff
##  exec_sql $sql_buff


  #插入本期数据  20090907修改 对于异常IMEI信息直接加进 where条件里
	set sql_buff "insert into BASS1.CDR_CALL_DM 
								select $timestamp, USER_ID, PRODUCT_NO, IMEI 
								from bass2.cdr_call_$timestamp 
								where imei is not null 
										and not (
				                 (ascii(substr(imei,1 ,1)) between  65 and 90 or ascii(substr(imei,1 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,2 ,1)) between  65 and 90 or ascii(substr(imei,2 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,3 ,1)) between  65 and 90 or ascii(substr(imei,3 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,4 ,1)) between  65 and 90 or ascii(substr(imei,4 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,5 ,1)) between  65 and 90 or ascii(substr(imei,5 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,6 ,1)) between  65 and 90 or ascii(substr(imei,6 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,7 ,1)) between  65 and 90 or ascii(substr(imei,7 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,8 ,1)) between  65 and 90 or ascii(substr(imei,8 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,9 ,1)) between  65 and 90 or ascii(substr(imei,9 ,1)) between 97  and 122) or 
				                 (ascii(substr(imei,10,1)) between  65 and 90 or ascii(substr(imei,10,1)) between 97  and 122) or 
				                 (ascii(substr(imei,11,1)) between  65 and 90 or ascii(substr(imei,11,1)) between 97  and 122) or 
				                 (ascii(substr(imei,12,1)) between  65 and 90 or ascii(substr(imei,12,1)) between 97  and 122) or 
				                 (ascii(substr(imei,13,1)) between  65 and 90 or ascii(substr(imei,13,1)) between 97  and 122) or 
				                 (ascii(substr(imei,14,1)) between  65 and 90 or ascii(substr(imei,14,1)) between 97  and 122) or 
				                 (ascii(substr(imei,15,1)) between  65 and 90 or ascii(substr(imei,15,1)) between 97  and 122) 
				                 )
								"
	puts $sql_buff
  exec_sql $sql_buff

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
#--------------------------------------------------------------------------------------------------------------

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
#--------------------------------------------------------------------------------------------------------------



