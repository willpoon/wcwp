######################################################################################################
#接口名称：银行渠道
#接口编码：06016
#接口说明：与中国移动及其各个省公司或地市公司签订合作协议的银行实体。
#程序名称: G_I_06016_MONTH.tcl
#功能描述: 生成06016的数据
#运行粒度: 月
#源    表：1.bass2.dim_cust_bank(银行维表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前银行维表只能满足该接口的渠道标识/银行名称/所在地市行政区标识字段。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06016_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06016_month
                        select
                          $op_month 
                          ,char(bank_id)
                          ,substr(bank_name,1,20)
                          ,' '
                          ,' '
                          ,'20000101'
                          ,'20300101'
                          ,char(int(city_id)+12210)
                          ,'13101'
                        from 
                          bass2.dim_cust_bank "
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