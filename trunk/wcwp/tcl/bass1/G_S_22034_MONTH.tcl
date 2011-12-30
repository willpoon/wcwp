######################################################################################################
#接口名称：客户话费/网络类月投诉
#接口编码：22034
#接口说明：记录客户话费类、网络类的投诉相关信息。
#程序名称: G_S_22034_MONTH.tcl
#功能描述: 生成22034的数据
#运行粒度: 月
#源    表：1.bass2.dw_custsvc_work_accept_dm_yyyymm(申告工单受理表)
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
	set sql_buff "delete from bass1.g_s_22034_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
    
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22034_month
                      select
                        ${op_month}                                                                                                                                                                                                                                   
                        ,'${op_month}'                                                                                                                                                                                                                                
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')  
                        ,char(count(*))
                      from 
                        bass2.dw_custsvc_work_accept_dm_yyyymm 
                      where 
                        accept_type=10 
                        and substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999'),1,2) in ('02','04')  
                      group by 
                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')"
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