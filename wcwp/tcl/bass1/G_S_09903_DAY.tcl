######################################################################################################
#接口名称：动力100业务量情况
#接口编码：09903
#接口说明：每日上报动力100业务量情况
#程序名称: G_S_09903_DAY.tcl
#功能描述: 生成09903的数据
#运行粒度: 日
#源    表：
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-03-12
#问题记录：
#修改历史: 
#######################################################################################################


#proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#
#	#当天 yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#        #当天 yyyy-mm-dd
#        set optime $op_time
#        
#        #删除本期数据
#	set sql_buff "delete from bass1.g_s_09903_day where time_id=$timestamp"
#	puts $sql_buff
#  exec_sql $sql_buff
#       
#
#  #插入 新增客户数  
#	set sql_buff "insert into BASS1.G_S_09903_DAY values
#                 ($timestamp,                    
#                 '101',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0');"
#                        
#  puts $sql_buff
#  exec_sql $sql_buff
#  
#
#	return 0
#}
#
#
##内部函数部分	
#proc exec_sql {MySQL} {
#
#	global env
#
#	global conn
#
#	global handle
#
#	set handle [aidb_open $conn]
#	set sql_buff $MySQL
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		puts $errmsg
#		exit -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	return 0
#}
##--------------------------------------------------------------------------------------------------------------
#
#proc get_single {MySQL} {
#
#	global env
#
#	global conn
#
#	global handle
#
#	set handle [aidb_open $conn]
#	set sql_buff $MySQL
#  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		puts $errmsg
#		exit -1
#	}
#	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		puts $errmsg
#		exit -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	return $result
#}
##--------------------------------------------------------------------------------------------------------------
#
#
#
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	return 0
}