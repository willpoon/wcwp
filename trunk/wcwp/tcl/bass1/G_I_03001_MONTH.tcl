######################################################################################################
#接口名称：帐户
#接口编码：03001
#接口说明：帐户是用户使用移动服务的付费实体。
#          1. 帐户是用户缴费、定制综合帐单的最小管理单元，它的唯一标识号是帐户标识；
#          2. 一个帐户可以为多个用户对应的帐目付费；
#          3. 一个用户对应的所有帐目可以由多个帐户付费。
#程序名称: G_I_03001_MONTH.tcl
#功能描述: 生成03001的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_msg_yyyymm(帐户)
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
	set sql_buff "delete from bass1.g_i_03001_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_03001_month
                      select
                        $op_month
                        ,acct_id
                        ,value(acct_name,'未知')
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0059',char(sts_id)),'9')
                        ,replace(char(date(create_date)),'-','')
                        ,cust_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(pay_type)),'01')
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0057',char(post_type)),'9')
                      from 
                        bass2.dw_acct_msg_$op_month "
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