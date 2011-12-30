######################################################################################################
#接口名称：资费营销案固定费用
#接口编码：02022
#接口说明：定义资费营销案中按一定的时间频率（月、年）收取的固定费用。根据有限公司市场部
#          《关于调整GPRS资费标准的通知.》（市通[2005]474号），为统计各省GPRS套餐情况，
#           请各省在此接口上报本省相应GPRS套餐数据。GPRS套餐的填报方式为：月租费项编码填写11，
#           费用金额填写套餐相应的月租费，如5、20、100、200，同时计量单位为分。
#程序名称: G_I_02022_MONTH.tcl
#功能描述: 生成02022的数据
#运行粒度: 月
#源    表：1.bass2.dwd_promoplan_rent_yyyymmdd(资费营销案固定费用)
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
	set sql_buff "delete from bass1.g_i_02022_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02022_month
                         select
                           $op_month
                           ,char(prod_id)
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')
                           ,char(sum(case
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then bigint(month_fee/10)                        
                              else month_fee
                            end))
                           ,case 
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then '08'
                              else '09'
                            end 
                         from 
                           bass2.dwd_promoplan_rent_$this_month_last_day
                         group by
                          char(prod_id)
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')
						               ,case 
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then '08'
                              else '09'
                            end
							order by char(prod_id)"
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