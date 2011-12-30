######################################################################################################
#接口名称：话费缴费记录
#接口编码：03006
#接口说明：客户缴纳话费费用的记录，包括客户缴纳话费预存款。
#程序名称: G_S_03006_DAY.tcl
#功能描述: 生成03006的数据
#运行粒度: 日
#源    表：1.bass2.dw_acct_payitem_dm_yyyymm(帐务缴费记录表)     
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.缴费中存在为集团客户，这些用户product_no为0，所以二经在做处理时，
#            不能关联出user_id，这样导致user_id为NULL值。所以一经在程序中剔除。
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_03006_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_03006_day
           ( 
              time_id       
              ,channel_id   
              ,pay_meth     
              ,pay_date     
              ,pay_time     
              ,owe_fee      
              ,acct_id      
              ,chzh_id      
              ,user_id 
             )
           select
             $timestamp
             ,char(so_channel_id)
             ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(paytype_id)),'99')
             ,replace(char(date(pay_date)),'-','')
             ,replace(char(time(pay_date)),'.','')
             ,char(sum(case 
                        when rec_sts=1 then bigint(round(-recv_cash*100,0))
                        else bigint(round(recv_cash*100,0))
              end ))
             ,acct_id
             ,char(rec_sts)
             ,user_id
           from 
             bass2.dw_acct_payitem_dm_${op_month} 
           where 
             op_time=date('${optime}')
             and user_id is not null
           group by 
             char(so_channel_id)
             ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(paytype_id)),'99')
             ,replace(char(date(pay_date)),'-','')
             ,replace(char(time(pay_date)),'.','')
             ,acct_id
             ,char(rec_sts)
             ,user_id   "       
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