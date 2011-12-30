######################################################################################################
#接口名称：欠费记录
#接口编码：03007
#接口说明：在"记录生成年月"产生的全部的欠费帐户帐单信息,不包括呆坏帐记录。
#程序名称: G_I_03007_MONTH.tcl
#功能描述: 生成03007的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_owe_yyyymm(欠费月表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.为了满足F7校验，修改程序
#修改历史: 1.20090717 内关联dw_acct_msg_  去除不在帐户表中的帐户标识
#          20100125 在网用户口径变动 usertype_id not in ('2010','2020','2030','9000') 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_03007_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
      
    
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_03007_month
                      select
                        $op_month
                        ,a.acct_id
                        ,substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.fee_item_id)),'0901'),1,2)||'00'
                        ,a.bill_cycle
                        ,'$op_month'
                        ,a.user_id
                        ,char(bigint(sum(a.unpay_fee*100)))
                      from 
                        bass2.dw_acct_owe_$op_month a,bass2.dw_acct_msg_$op_month b
                      where
                        bigint(bill_cycle)<$op_month and a.acct_id=b.acct_id
                      group by 
                        a.acct_id
                        ,substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.fee_item_id)),'0901'),1,2)||'00'
                        ,a.bill_cycle
                        ,a.user_id   "            
        puts $sql_buff       
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle


#剔除离网用户  
          set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_I_03007_month
                      (
                      time_id
                      ,ACCT_ID
                      ,BILL_CYC_ID
                      ,ITEM_ID
                      ,CREATE_DATE
                      ,USER_ID
                      ,UNPAY_FEE
                      )
                    select
                      888888
                      ,a.ACCT_ID
                      ,a.BILL_CYC_ID
                      ,ITEM_ID
                      ,a.CREATE_DATE
                      ,a.USER_ID
                      ,a.UNPAY_FEE
                 from bass1.G_I_03007_month a,
                       (select a.user_id from
                       (select time_id,user_id,usertype_id from G_A_02004_DAY where time_id <= $this_month_last_day) a,
                       (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$this_month_last_day group by user_id)b
                       where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','9000')
                     )b
                 where a.time_id = $op_month and a.user_id = b.user_id and rtrim(a.ACCT_ID) <> ''   and rtrim(a.BILL_CYC_ID) <> '' ;"      
                      
         puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}     
	aidb_commit $conn
	aidb_close $handle
	
	
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.G_I_03007_month where time_id = $op_month;"           
         puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}     
	aidb_commit $conn
	aidb_close $handle
	
	
  set handle [aidb_open $conn]
	set sql_buff "update bass1.G_I_03007_month set time_id = $op_month where time_id = 888888 "
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle	



  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.G_I_03007_month where  time_id = 888888 "
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle
	
	
  set handle [aidb_open $conn]
	set sql_buff "	
	delete from   G_I_03007_MONTH where time_id = $op_month and user_id in (
    select a.user_id from
    (
     select time_id,user_id from G_A_02008_DAY where time_id <= $this_month_last_day and usertype_id in ('2010','2020','2030','9000') ) a,
     (
      select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$this_month_last_day and usertype_id in ('2010','2020','2030','9000')
      group by user_id
     )b
     where a.time_id=b.time_id and a.user_id=b.user_id
    )
    with ur"
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle
	
	#剔出欠费金额<=0的用户
  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.G_I_03007_month where  time_id = $op_month  and bigint(unpay_fee) <= 0; "
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