######################################################################################################
#接口名称：国际漫游来访用户拨打12593情况
#接口编码：21020
#接口说明：记录国际漫游来访的用户在本省内拨打12593号码的通话情况。
#程序名称: G_S_21020_MONTH.tcl
#功能描述: 生成21020的数据
#运行粒度: 月
#源    表：1.bass2.dw_call_roamin_dm_yyyymm(漫入话单多日表)
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
	set sql_buff "delete from bass1.g_s_21020_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#直接汇总到结果表
        set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_s_21020_month
                      select
                         $op_month
                        ,'$op_month'
                        ,char(count(distinct opp_noaccess_number))
                        ,char(bigint(sum(call_counts)))
                        ,char(bigint(sum(call_duration)))
                        ,char(bigint(sum(call_duration_m)))
                        ,char(bigint(sum(basecall_fee)*100))
                        ,char(bigint(sum(toll_fee)*100))
                      from
                        bass2.dw_call_roamin_dm_$op_month
                      where
                        opp_access_type_id=83 "
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