######################################################################################################
#接口名称：付费计划
#接口编码：03002
#接口说明：定义用户的付费计划，即用户的某个明细帐目科目费用由哪个帐户支付。
#程序名称: G_I_03002_MONTH.tcl
#功能描述: 生成03002的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_shoulditem_yyyymm(明细帐单)
#          2.bass2.dw_acct_msg_yyyymm(帐户)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.bass1.G_S_03002_MONTH_TYM 修改成bass1.G_S_03002_MONTH
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
   
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_I_03002_MONTH where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_S_03002_MONTH_TYM where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    
    puts "00000000000000000"         
    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03002_MONTH_TYM
                      select
                        $op_month
                        ,b.user_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',CHAR(b.item_id)),'0901') 
                        ,a.acct_id
                      from 
                        bass2.dw_acct_shoulditem_$op_month b,
                        bass2.dw_acct_msg_$op_month a
                      where 
                        a.acct_id=b.acct_id"
#                        a.cust_id=b.cust_id
#                      group by 
#                        b.user_id
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',CHAR(b.item_id)),'0901') 
#                        ,a.acct_id "                
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}  
	puts "11111111111"      
	aidb_commit $conn
	puts "2222222222222222"
	aidb_close $handle
    puts "3333333333333333"
    
    
    puts "00000000000000000"         
    set handle [aidb_open $conn]
	set sql_buff "update G_S_03002_MONTH_TYM 
                   set item_id = '0401' 
                 where time_id = $op_month and item_id = '0400' ;"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}  
	puts "44444"      
	aidb_commit $conn
	puts "55555"
	aidb_close $handle
    puts "66666" 
        
    puts "00000000000000000"         
    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_I_03002_MONTH
                      select distinct TIME_ID,USER_ID,ITEM_ID,ACCT_ID
                        from bass1.G_S_03002_MONTH_TYM
                      where 
                        time_id = $op_month"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}  
	puts "44444"      
	aidb_commit $conn
	puts "55555"
	aidb_close $handle
    puts "66666"    
	return 0
}