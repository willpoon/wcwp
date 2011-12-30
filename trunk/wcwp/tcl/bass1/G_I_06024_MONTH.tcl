######################################################################################################
#接口名称：代销代办商渠道代销业务
#接口编码：06024
#接口说明：记录代销代办商渠道代销的业务种类。
#程序名称: G_I_06024_MONTH.tcl
#功能描述: 生成06022的数据
#运行粒度: 月
#源    表：1.bass2.dim_pub_channel(渠道维表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前渠道表在BOSS没有代销业务类型，所以统一填"99:其他"。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06024_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06024_month
                          select
                            $op_month
                            ,'03'
                            ,char(channel_id)
                          from bass2.dim_pub_channel
                          where sts=1 
                            and channeltype_id in (6,7,8)"
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