######################################################################################################
#接口名称：明细帐单
#接口编码：03004
#接口说明：明细帐单是中国移动"用户"使用中国移动服务后，在指定"帐务周期"，
#          按照"明细帐目科目"产生的费用细项信息，且经过了帐务级优惠处理。只上报个人手机用户和数据
#          SIM卡用户的相应费用统计。
#程序名称: G_S_03004_MONTH.tcl
#功能描述: 生成03004的数据
#运行粒度: 月
#源    表：1.bass2.dw_acct_shoulditem_yyyymm(明细帐)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.增加了#CSD通信费和#GPRS通信费。
#          2. liuzhilong 20090706修改 用户不正常的进行过滤
#　　　　　3.20100124 在网用户口径修改 userstatus_id in (0,4,5,7) 修改为userstatus_id in (0,4,5,7,9)
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]      
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
 
        #删除本期数据
	set sql_buff "delete from bass1.g_s_03004_month where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff
  
  
	set sql_buff "insert into bass1.g_s_03004_month
                        select
                          $op_month
                          ,a.user_id
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901')
                          ,'$op_month'
                          ,char(bigint(sum(a.fact_fee)*100))
                          ,substr(char(bigint(sum(case when a.fav_fee >0 then a.fav_fee else 0 end)*-100)),1,8) 
                        from   bass2.dw_acct_shoulditem_$op_month a
                        inner join 
	                        (
	                         select user_id from bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9)
	                         except
	                         select user_id from bass2.dw_product_${last_month} where userstatus_id in (0,4,5,7,9)
	                         ) b on a.user_id=b.user_id
                        where
                          a.item_id not in (80000027,80000032,80000033,80000101) 
                        group by
                          a.user_id                       
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901') "                          
  puts $sql_buff
  exec_sql $sql_buff
	



  #统一主键	
  set sql_buff "delete from G_S_03004_month  where time_id = 888888;"
  puts $sql_buff
  exec_sql $sql_buff


  set sql_buff "update  G_S_03004_month set ACCT_ITEM_ID = '0401' where time_id = $op_month and ACCT_ITEM_ID = '0400';"
  puts $sql_buff
  exec_sql $sql_buff

  set sql_buff "update  G_S_03004_month set time_id = 888888 where time_id = $op_month;"
  puts $sql_buff
  exec_sql $sql_buff

  set sql_buff "insert into G_S_03004_month 
                select $op_month,user_id,acct_item_id,bill_cyc_id,char(sum(bigint(fee_receivable))),char(sum(bigint(fav_chrg)))
                  from G_S_03004_month 
                 where time_id = 888888
              group by user_id,acct_item_id,bill_cyc_id;"
  puts $sql_buff
  exec_sql $sql_buff


  set sql_buff "delete from G_S_03004_month  where time_id = 888888;"
  puts $sql_buff
  exec_sql $sql_buff


	
#	#CSD通信费
#	set sql_buff "insert into bass1.g_s_03004_month
#              select $op_month
#                     ,user_id
#                     ,'0617'
#                     ,'$op_month'
#                     ,char(sum(int(fact_fee*100)))
#                     ,'0'
#                from bass2.dw_acct_should_dtl_$op_month
#               where feetype_id = 0617
#               group by user_id"
#
#  exec_sql $sql_buff


#  #GPRS通信费
#	set sql_buff "insert into bass1.g_s_03004_month
#              select $op_month
#                     ,user_id
#                     ,'0618'
#                     ,'$op_month'
#                     ,char(sum(int(fact_fee*100)))
#                     ,'0'
#                from bass2.dw_acct_should_dtl_$op_month
#               where feetype_id = 0618
#               group by user_id"
#
#  exec_sql $sql_buff
	
		
#	#　　　　　GPRS通信费
#	set sql_buff "insert into bass1.g_s_03004_month
#              select $op_month
#                     ,user_id
#                     ,'0619'
#                     ,'$op_month'
#                     ,char(sum(int(fact_fee*100)))
#                     ,'0'
#                from bass2.dw_acct_should_dtl_$op_month
#               where feetype_id = 0619
#               group by user_id"
#
#  exec_sql $sql_buff


#  #　　　　　CSD通信费
#	set sql_buff "insert into bass1.g_s_03004_month
#              select $op_month
#                     ,user_id
#                     ,'0621'
#                     ,'$op_month'
#                     ,char(sum(int(fact_fee*100)))
#                     ,'0'
#                from bass2.dw_acct_should_dtl_$op_month
#               where feetype_id = 0621
#               group by user_id"
#
#  exec_sql $sql_buff



#  #　　　　　号薄管家
#	set sql_buff "insert into bass1.g_s_03004_month
#              select $op_month
#                     ,user_id
#                     ,'0630'
#                     ,'$op_month'
#                     ,char(sum(int(fact_fee*100)))
#                     ,'0'
#                from bass2.dw_acct_should_dtl_$op_month
#               where feetype_id = 0630
#               group by user_id"
#
#  exec_sql $sql_buff



	return 0
}




#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------

	