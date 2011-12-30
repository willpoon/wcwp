######################################################################################################
#接口名称：IMSI资源
#接口编码：06006
#接口说明：IMSI资源库。
#程序名称: G_I_06006_MONTH.tcl
#功能描述: 生成06006的数据
#运行粒度: 月
#源    表：1.bass2.dwd_res_sim_yyyymmdd(卡资源)
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
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06006_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06006_month
                        select
                          $op_month
                          ,substr(imsi,1,15)
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(city_id)),'13100')
                          ,value(sim_id,' ')
                          ,char(channel_id)
                          ,case
                             when sts=1 then '01'
                             when sts=2 then '02'
                             when sts=3 then '03'
                             when sts=4 then '04'
                             when sts=5 then '06'
                             else '03'
                           end 
                        from  bass2.dwd_res_sim_$this_month_last_day  "
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