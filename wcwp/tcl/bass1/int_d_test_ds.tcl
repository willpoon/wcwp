#################################################################
# 程序名称: int_d_template_ds.tcl
# 功能描述: 一经模板d程序
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

        set handle [aidb_open $conn]
        
	set sql_buff "insert into bass2.tmp_zcg (a) values ('北京')"
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "插入数据有问题" 1000
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn
	
	aidb_close $handle

	return 0
}
