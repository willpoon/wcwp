#=====================================================================*
#*规则编号：22072     G_S_22072_MONTH.tcl                              *
#源表                                                                  *
#  DW_ACCT_SHOULDITEM_yyyymm                                          *
#*规则类型：月                                                          *
#*规则描述：22072接口程序                                               *
#功能描述：统计集团客户行业应用客户端发展情况息                           *
#调用例程：int -s G_S_22072_MONTH.tcl                                  *
#创建日期：2008-06-05                                                  *
#创 建 者：xiahuaxue                                                   *
#修改历史：1.5.4版本                                                    *
#======================================================================*

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
	    global env
      global handle

       
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]    
        puts $op_month

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay
       
       


        #上月	$last_month	
       set last_month [GetLastMonth [string range $op_month 0 5]]
       
     
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22072_MONTH where TIME_ID= $op_month"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  
#22072建表脚本  
#CREATE TABLE BASS1.G_S_22072_MONTH
# (TIME_ID        INTEGER         NOT NULL,  --
#  BILL_MONTH     CHARACTER(6)    NOT NULL,  --月份
#  TRADE_CLASS    CHARACTER(4)    NOT NULL,  --行业应用客户端分类编码
#  ARRIVE_NUM     CHARACTER(9)    NOT NULL,  --客户端到达数
#  ADD_NUM        CHARACTER(9)    NOT NULL,  --月新增客户端数量
#  INCOME         CHARACTER(15)   NOT NULL   --月业务收入
# )
#  DATA CAPTURE NONE
#  IN TBS_APP_BASS1
#  INDEX IN TBS_INDEX
#  PARTITIONING KEY
#   (TIME_ID,
#    BILL_MONTH,
#    TRADE_CLASS
#   ) USING HASHING
#  NOT LOGGED INITIALLY;
#
#ALTER TABLE BASS1.G_S_22072_MONTH
#  LOCKSIZE ROW
#  APPEND OFF
#  NOT VOLATILE;
#
#GRANT CONTROL ON TABLE BASS1.G_S_22072_MONTH TO USER DB2INST1;
  
#  set sql_buff "  insert into G_S_22072_MONTH
#                      select $op_month,
#                              '$op_month',
#						                  '0305',
#                              char(count(*)),
#                              char(count(*)-(select count(*) from bass2.DW_ACCT_SHOULDITEM_$last_month a,
#                                                                  bass2.dw_enterprise_sub_$last_month b,
#	                                                                bass2.dw_product_$last_month d
#                                                            where b.service_id in ('944')   and a.acct_id in ('1001316278','1001402987') and 
#	                                                              a.acct_id = b.Acct_id and rec_status = 1 and 
#	                                                              a.user_id = d.user_id and d.usertype_id in (1,2,9)	)),
#                              char(bigint(sum(a.fact_fee*100)))
#
#							              from bass2.DW_ACCT_SHOULDITEM_$op_month a,
#                                 bass2.dw_enterprise_sub_$op_month b,
#	                               bass2.dw_product_$op_month d
#                           where b.service_id in ('944')   and a.acct_id in ('1001316278','1001402987') and 
#	                               a.acct_id = b.Acct_id and rec_status = 1 and 
#	                               a.user_id = d.user_id and d.usertype_id in (1,2,9)		
#							             group by 	b.service_id "
#  
#  
#    puts $sql_buff      
#    exec_sql $sql_buff      
    
    
    
  #警务通用户数  

   set sql_buff "  insert into G_S_22072_MONTH
                     select  $op_month,
                             '$op_month',
                   		       '0202',    
                             char( count(distinct a.user_id)),
                             '0' ,
                             '0'
                   from bass2.DW_ENTERPRISE_MEMBERSUB_$op_month a  
                    where a.order_id in 
                    (
                     select order_id from bass2.dw_enterprise_sub_$op_month 
                      where service_id='945' 
                         and rec_status=1 
                         and enterprise_id not in ('891910006274')
                    )
                    and rec_status=1  
               "
    puts $sql_buff      
    exec_sql $sql_buff   
    
    
  #警务通用户数 (0101) 
   set sql_buff "  insert into G_S_22072_MONTH
                     select  $op_month,
                             '$op_month',
                   		       '0101',    
                             char( count(distinct a.user_id)),
                             '0' ,
                             '0'
                   from bass2.DW_ENTERPRISE_MEMBERSUB_$op_month a  
                    where a.order_id in 
                    (
                     select order_id from bass2.dw_enterprise_sub_$op_month 
                      where service_id='945' 
                         and rec_status=1 
                         and enterprise_id not in ('891910006274')
                    )
                    and rec_status=1  
               "
    puts $sql_buff      
    exec_sql $sql_buff    
    
							             
#	#警务通收入 						            
#  set sql_buff "  insert into G_S_22072_MONTH
#                      select  $op_month,
#                              '$op_month',
#						                  '0202',
#                              '0',
#                              '0',
#                              char(bigint(sum(b.should_fee)*100))
#							           from bass2.DW_ENTERPRISE_MEMBERSUB_$op_month a ,bass2.DW_ACCT_SHOULDITEM_200808 b 
#							          where a.order_id in ('6618') and a.user_id=b.user_id and b.item_id=80000104 
#							                and b.acct_id='1001479405'  and a.rec_status=1"
#
#    puts $sql_buff      
#    exec_sql $sql_buff      

    set sql_buff " update G_S_22072_MONTH set time_id = 888888 where time_id = $op_month"
    puts $sql_buff      
    exec_sql $sql_buff      

    set sql_buff " insert into G_S_22072_MONTH 
                    select $op_month,
                           '$op_month',
                           trade_class,
                           char(sum(bigint(arrive_num))),
                           char(sum(bigint(add_num))),
                           char(sum(bigint(income)))
                      from G_S_22072_MONTH 
                     where time_id = 888888 
                     group by  trade_class"
    puts $sql_buff      
    exec_sql $sql_buff      

    set sql_buff " delete from  G_S_22072_MONTH where  time_id = 888888 "
    puts $sql_buff      
    exec_sql $sql_buff      

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


