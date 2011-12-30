######################################################################################################
#接口名称：呼叫中心特征
#接口编码：06020
#接口说明：呼叫中心的特征信息。
#程序名称: G_I_06020_MONTH.tcl
#功能描述: 生成06020的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为西藏的呼叫中心渠道目前没有接口，所以程序采用直接插入数据的方法。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06020_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06020_month 
	          values ($op_month,'10086','8','20','45'),
	                 ($op_month,'10085','0','10','4'),
	                 ($op_month,'12582','0','1','1'),
	                 ($op_month,'12580','6','3','3') "
	          
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
###########################参考######################################
#渠道标识  2M电路中继数	    坐席数	话务员数	
#10086	  共用8个中继数	    20	        45	
#10085		            10	        4	
#12582		            1	        1	
#12580	  6中继数	    3	        3	
#-------
#前三个共用8M电路，10086为人工、自动混合，10085为外呼，12582为农信通，12580为人工、
#################################################################