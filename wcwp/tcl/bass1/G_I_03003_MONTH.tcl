######################################################################################################
#接口名称：帐本
#接口编码：03003
#接口说明：帐本是帐户的各种交费渠道的收支余额信息。帐本以帐户标识、帐本科目为单元登记帐户的收支余额信息。
#程序名称: G_I_03003_MONTH.tcl
#功能描述: 生成03003的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_book_yyyymm(帐本)
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
	set sql_buff "delete from bass1.g_i_03003_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
              
        set handle [aidb_open $conn]
       	set sql_buff "insert into bass1.g_i_03003_month
                      select
                        $op_month
                        ,acct_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0056',char(book_item_id)),'99')
                        ,char(bigint(sum(amount)*100))
                      from 
                        bass2.dw_acct_book_$op_month
                      group by
                        acct_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0056',char(book_item_id)),'99') "
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