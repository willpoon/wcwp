######################################################################################################
#接口名称：用户信用额度
#接口编码：02009
#接口说明：用户的当前信用额度。
#程序名称: G_I_02009_MONTH.tcl
#功能描述: 生成02009的数据
#运行粒度: 月
#源    表：1.bass2.dw_product_yyyymm(用户资料月表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.考虑到该接口送离网用户的资料无意义，所以只送在网的
#修改历史: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
             
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02009_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02009_month
                       select
                         $op_month
                         ,user_id
                         ,char(int(CREDIT/10))
                       from 
                         bass2.dw_product_$op_month
                       where 
                         userstatus_id in (1,2,3,6)
                         and usertype_id in (1,2,9)  "   
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