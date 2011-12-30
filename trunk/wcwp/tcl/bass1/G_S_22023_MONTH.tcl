######################################################################################################
#接口名称：客服月呼入量
#接口编码：22023
#接口说明：记录客服月呼入量相关信息。
#程序名称: G_S_22023_MONTH.tcl
#功能描述: 生成22023的数据
#运行粒度: 月
#源    表：1.bass2.dw_custsvc_manual_inbound_dm_yyyymm(人工呼入统计表)
#          2.bass2.dw_custsvc_inbound_dm_yyyymm(系统呼入统计表)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22023_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22023_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,'00040001'
                        ,char(int(sum(call_count)))
                      from 
                        bass2.dw_custsvc_manual_inbound_dm_$op_month 
                      union all
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,'00040002'
                        ,char(int(sum(call_count)))
                      from
                        bass2.dw_custsvc_inbound_dm_$op_month "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle

	return 0
}