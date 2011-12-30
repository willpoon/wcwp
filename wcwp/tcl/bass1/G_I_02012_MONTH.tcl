######################################################################################################
#接口名称：用户关系历史
#接口编码：02012
#接口说明：记录用户之间的关系，根据关系类型区分。目前关注的用户关系类型包括：
#          一卡双号、子母卡和一卡多号。
#程序名称: G_I_02012_MONTH.tcl
#功能描述: 生成02012的数据
#运行粒度: 月
#源    表：1.bass2.dwd_product_relation_yyyymm(用户关系表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.西藏没有一卡双号，一卡多号的业务，关系类型只取子母卡,参考dim_busi_type
#          2.考虑到该接口送离网用户的资料无意义，所以只送在网的
#修改历史: 1.20091217 dwd_product_relation_yyyymm 修改成dw的月表
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
       
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
     
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02012_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02012_month
                select
                  $op_month
                  ,'02'
                  ,user_id
                  ,rserv_id
                from 
                  bass2.dw_product_relation_$op_month
                where 
                  busi_type=3 "
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