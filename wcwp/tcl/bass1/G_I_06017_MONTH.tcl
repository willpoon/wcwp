######################################################################################################
#接口名称：员工
#接口编码：06017
#接口说明：本次接口中该实体记录营销人员/客服人员的员工信息。
#          包括：客户经理，呼叫中心话务员，营业厅销售人员，直销人员。使用"营销人员类型代码"区分上述人员。
#程序名称: G_I_06017_MONTH.tcl
#功能描述: 生成06017的数据
#运行粒度: 月
#源    表：1.bass2.DIM_CUSTSVC_STAFF(客服员工)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前bass2.DIM_CUSTSVC_STAFF(客服员工)的数据只能出该接口员工标识/姓名。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
  
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06017_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06017_month
                        select
                          $op_month
                          ,char(op_id)
                          ,substr(op_name,1,20)
                          ,' '
                          ,' '
                          ,'20300101'
                          ,' '
                          ,' '
                          ,' '
                          ,' '
                          ,'01'
                          ,'13101'
                        from bass2.dim_custsvc_staff "

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	return 0
}