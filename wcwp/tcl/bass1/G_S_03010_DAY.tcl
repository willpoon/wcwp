######################################################################################################
#接口名称：营业费缴费记录
#接口编码：03010
#接口说明：营业费的缴费记录是记录客户在营业厅办理业务时缴纳的费用信息。
#程序名称: G_S_03010_DAY.tcl
#功能描述: 生成03010的数据
#运行粒度: 日
#源    表：1.bass2.dwd_acct_busicharge_yyyymmdd(营业受理费用)
#          2.bass2.dwd_product_busi_yyyymmdd(用户活动订购工单表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录： 数据转换异常，把int改成bigint
#修改历史: 
#        char(sum(bigint(a.item_value*100)))
#        char(sum(bigint(value(a.item_discount,0)*100)))

#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
       
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_03010_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_03010_day
                       (
                        time_id
                       ,accept_fee_id
                       ,pay_date
                       ,pay_time
                       ,channel_id
                       ,should_fee
                       ,disc_chrg
                       ,pay_meth
                       ,chzh_id
                       ,user_id
                       )
                    select 
                      ${timestamp}
                      ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0049',char(a.item_id)),'010000')
                      ,replace(char(date(a.charge_date)),'-','')
                      ,replace(char(time(a.charge_date)),'.','')                      
                      ,char(a.channel_id)
                      ,char(sum(bigint(a.item_value*100)))
                      ,char(sum(bigint(value(a.item_discount,0)*100)))
                      ,'01'
                      ,case when b.isnormal=2 then '1' else '0' end
                      ,b.user_id
                    from 
                      bass2.dwd_acct_busicharge_$timestamp a,
                      bass2.dwd_product_busi_$timestamp b
                    where 
                      a.so_nbr=b.so_nbr
                    group by
                      coalesce(bass1.fn_get_all_dim('BASS_STD1_0049',char(a.item_id)),'010000')
                      ,replace(char(date(a.charge_date)),'-','')
                      ,replace(char(time(a.charge_date)),'.','')                      
                      ,char(a.channel_id)
                      ,case when b.isnormal=2 then '1' else '0' end
                      ,b.user_id   "                 
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