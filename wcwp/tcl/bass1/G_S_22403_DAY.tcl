######################################################################################################
#接口名称：学生客户市场竞争日汇总
#接口编码：22403
#接口说明：
#程序名称: G_S_22403_DAY.tcl
#功能描述:  
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-7-26
#问题记录：西藏没此业务，因此接口暂时送空文件
#修改历史:
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time

##  #删除本期数据
##  set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.G_S_22403_DAY where time_id=$timestamp"
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}      
##	aidb_commit $conn
##	aidb_close $handle
##
##       
##
##  set handle [aidb_open $conn]
##	set sql_buff " insert into BASS1.G_S_22403_DAY"
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}      
##	aidb_commit $conn
##	aidb_close $handle

	return 0
}

