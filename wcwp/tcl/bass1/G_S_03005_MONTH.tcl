######################################################################################################
#接口名称：综合帐单
#接口编码：03005
#接口说明：综合帐单是根据用户和帐户间的帐务关系，以及明细帐目和综合帐目对应关系将用户在某个
#          帐务周期的明细帐单累计到付费帐户下的综合费用信息。只上报个人手机用户和数据SIM卡用户的
#          相应费用统计。
#程序名称: G_S_03005_MONTH.tcl
#功能描述: 生成03005的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_shoulditem_yyyymm(明细帐)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.03005接口存在本月账单包含历史里网用户的问题
#修改历史: 1.modify by zhanght on 20090530   20090805 修改口径：排除历史离网用户
#　　　　　3.20100124 在网用户口径修改 userstatus_id in (0,4,5,7) 修改为userstatus_id in (0,4,5,7,9)
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]  
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_03005_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
       
              
       set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_03005_month
                             select
                               $op_month
                               ,a.acct_id
                               ,a.user_id
                               ,'$op_month'
                               ,substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'),1,2)||'00'
                               ,char(bigint(sum(a.fact_fee)*100))
                             from bass2.dw_acct_shoulditem_${op_month} a
                             inner join 
	                             (
	                             select user_id from bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9)
	                             except
	                             select user_id from bass2.dw_product_${last_month} where userstatus_id in (0,4,5,7,9)
	                             ) b on a.user_id=b.user_id
                             where item_id not in (80000027,80000032,80000033,80000101)
                             group by
                                a.acct_id
                               ,a.user_id
                               ,substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'),1,2)||'00' "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle





#删除历史离网用户本月账单 add by zhanght on 20090530

#  set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE bass1.check_02008_03005 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#  
#  
#  
#  set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.check_02008_03005
#  select user_id from
#(
#select user_id,usertype_id from
#(
#select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
#  from BASS1.G_A_02008_DAY
#  where time_id/100<=$op_month
#  ) k
#  where k.row_id=1
# ) l
# where l.usertype_id like '2%' 
# and user_id in (select distinct  a.user_id
#  from BASS1.G_S_03005_MONTH a
#  left outer join (select distinct user_id from BASS1.G_A_02008_DAY
#  where usertype_id like '2%' 
#  and time_id/100=$op_month) b
#  on a.user_id=b.user_id
#  where a.time_id=$op_month
#  and b.user_id is null)  with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#  
#  
#  
#  set handle [aidb_open $conn]
#	set sql_buff "delete from BASS1.G_S_03005_MONTH
#where user_id in(select user_id from bass1.check_02008_03005)
#and time_id=$op_month with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
	
#		
#	select ACCT_ID,USER_ID,BILL_CYC_ID,ITEM_ID , count(0) 
#	--,  count(distinct ACCT_ID,USER_ID,BILL_CYC_ID,ITEM_ID ) 
#	from G_S_03005_MONTH where time_id = 201108
#	group by  ACCT_ID,USER_ID,BILL_CYC_ID,ITEM_ID  having count(0) > 1
#	order by 1 
#	
#	
#	
#	
#	select USER_ID,ACCT_ITEM_ID,BILL_CYC_ID , count(0) 
#	--,  count(distinct USER_ID,ACCT_ITEM_ID,BILL_CYC_ID ) 
#	from G_S_03004_MONTH where time_id = 201108
#	group by  USER_ID,ACCT_ITEM_ID,BILL_CYC_ID  having count(0) > 1
#	order by 1 
#	

	return 0
}