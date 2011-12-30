######################################################################################################
#接口名称：用户区域归属
#接口编码：02050
#接口说明：用户的区域归属信息。
#程序名称: G_A_02052_MONTH.tcl
#功能描述: 生成02050的数据
#运行粒度: 月
#源    表：bass2.STAT_ZD_VILLAGE_USERS_YYYYMM
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败  
#编 写 人：张海涛
#编写时间：2009-05-06
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
	set sql_buff "DELETE FROM bass1.G_A_02052_MONTH where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff

  
  
	set sql_buff "insert into bass1.G_A_02052_MONTH 
	              select $op_month,user_id,char(LOCNTYPE_ID)
	                from bass2.STAT_ZD_VILLAGE_USERS_$op_month
                      where month_new_mark = 1
				       with ur "
	
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


