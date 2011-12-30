#=====================================================================*
#*规则编号：22041     G_S_22041_MONTH.tcl                              *
#*规则类型：月                                                         *
#原表                                                                  *
#   bass2.DW_RES_MSISDN_yyyymmdd                                      *
#   bass2.DWD_RES_SIM_yyyymmdd                                         *
#   bass2.dw_product_yyyymm                                            *
#*规则描述：22041接口程序                                              *
#功能描述：记录BOSS帐户号码的分布情况                                  *
#调用例程：int -s G_S_22041_MONTH.tcl                                  *
#创建日期：2007-08-23                                                  *
#创 建 者：xiahuaxue                                                   *
#修改历史：20100120 修改在网用户口径userstatus_id in (1,2,3,6,8)     *
#======================================================================*

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
	    global env
      global handle

        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
        puts $op_month

        set ThisMonthFirstDay [string range $op_month 0 5]01
        puts $ThisMonthFirstDay
        set PrevMonthLastDay [GetLastDay [string range $ThisMonthFirstDay 0 7]]

        puts $PrevMonthLastDay
        
        set op_time [string range $PrevMonthLastDay 0 3]-[string range $PrevMonthLastDay 4 5]-[string range $PrevMonthLastDay 6 7]  
        puts $op_time
        
        set this_month [string range $op_time 0 3][string range $op_time 5 6]    
        

	      #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
              

        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
	      #本年天数 this_year_days	
        set this_year_days [GetThisYearDays ${op_month}01]

	      #本月天数	this_month_days
        set this_month_days [GetThisMonthDays ${op_month}01]


        #上月	$last_month	
       set last_month [GetLastMonth [string range $op_month 0 5]]
       
       #上年最后一个月
       set LastYearMonth [GetLastMonth [string range $op_month 0 3]01]
   
      scan   $op_time "%04s-%02s-%02s" year month day
      set    tmp_time_string  ""
      set    last_1_month [GetLastMonth [string range $year$month 0 5]]01 
      set    last_2_month [GetLastMonth [string range $last_1_month 0 5]]01 
      set    last_3_month [GetLastMonth [string range $last_2_month 0 5]]01 
      set    last_4_month [GetLastMonth [string range $last_3_month 0 5]]01 
      set    last_5_month [GetLastMonth [string range $last_4_month 0 5]]01
      scan   $last_3_month "%04s%02s%02s" last3_month_year last3_month_month last3_month_days
      scan   $last_5_month "%04s%02s%02s" last5_month_year last5_month_month last5_month_days

     puts $timestamp
     puts $op_month
     
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22041_MONTH where TIME_ID= $op_month"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  

  #1.未分配号码数
 	set sql_buff "select count(distinct product_no) from bass2.DW_RES_MSISDN_$op_month where sts=1"
  set UnAllot_Count [get_single $sql_buff]
  puts "UnAllot_Count： $UnAllot_Count"
  #--------------------------------------------------------------------------------------------------------------
     
  #2.未完成数据制作号码数
 	set sql_buff "select count(*) from bass2.DW_RES_SIM_$op_month where  sts=1 and  puk1 is null and pin1 is null"
  set UnFacture_Count [get_single $sql_buff]
  puts "UnFacture_Count： $UnFacture_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #3.库存号码数
 	set sql_buff "select count(distinct product_no) from bass2.DW_RES_MSISDN_$op_month where sts=2"
  set Store_Count [get_single $sql_buff]
  puts "Store_Count： $Store_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #4.渠道铺号量
 	set sql_buff "select count(distinct product_no)  
 	                from bass2.DW_RES_MSISDN_$op_month 
 	               where sts=2 and res_sts=4 and purpose=3 and wholesale_date is not null"
  set Trench_Count [get_single $sql_buff]
  puts "Trench_Count： $Trench_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #5.可回收号码数
 	set sql_buff "select count(distinct product_no)
                  from
                     ((select product_no
                       from bass2.dw_product_$op_month
                       where userstatus_id = 0
                         and usertype_id in (1,2,9)
                       except
                       select product_no
                       from bass2.dw_product_$op_month
                       where userstatus_id <> 0
                         and usertype_id in (1,2,9))
                       union all
                       select product_no
                       from bass2.dw_product_$op_month   
                       where userstatus_id not in (0,1,2,3,6,8)
                         and usertype_id in (1,2,9)
                         and sts_date < '${last5_month_year}-${last5_month_month}-${last5_month_days}') a"
  set CanCallback_Count [get_single $sql_buff]
  puts "CanCallback_Count： $CanCallback_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #6.正常使用客户号码数/7.零次户号码数

  
	set handle [aidb_open $conn]
  set sql_buff "select case 
                when userstatus_id in (2,3,6) or (stopstatus_id in (1,12,14,16,26,27,28,29,31,32,33) and sts_date >= '${last3_month_year}-${last3_month_month}-${last3_month_days}') then 12
                when call_counts <> 0 then 11
                     else 15
                end,
                count(user_id)
            from bass2.dw_product_$op_month
           where userstatus_id in (1,2,3,6,8) and 
                 usertype_id in (1,2,9)
           group by 
            case 
                 when userstatus_id in (2,3,6) or (stopstatus_id in (1,12,14,16,26,27,28,29,31,32,33) and sts_date >= '${last3_month_year}-${last3_month_month}-${last3_month_days}') then 12
                 when call_counts <> 0 then 11
                 else 15
             end"
       
  if [catch {aidb_sql $handle $sql_buff} errmsg] {
    WriteTrace $errmsg 1300
    puts "errmsg:$errmsg"
    return -1
  }
   while {[set this_row [aidb_fetch $handle]] != ""} {
             set  tmp_row_id           [lindex $this_row 0 ]
             set  tmp_count            [lindex $this_row 1 ]
#           puts $tmp_row_id
            
          #正常使用客户号码数
          if { $tmp_row_id == 11 } {
                 set Using_Count $tmp_count
                   puts "Using_Count： $Using_Count"
                }
          #零次户号码数
          if { $tmp_row_id == 15 } {
                 set UnUsing_Count $tmp_count
                 puts "UnUsing_Count： $UnUsing_Count"
                }
  }
                 aidb_close $handle
          if [catch {set handle [aidb_open $conn]} errmsg] {
                 WriteTrace $errmsg 1302
                 puts $errmsg
                 return -1
                 }

       
#  #6.正常使用客户号码数
# 	set sql_buff "select count(distinct product_no) 
# 	                from bass2.dw_product_$op_month 
# 	               where userstatus_id in (1,2,3,6)   AND 
# 	                     usertype_id in (1,2,9)       AND
#  call_counts <> 0"
#  set Using_Count [get_single $sql_buff]
#  puts "Using_Count： $Using_Count"
#  #--------------------------------------------------------------------------------------------------------------
#  
#  #7.零次户号码数
# 	set sql_buff "select count(distinct product_no) from bass2.DW_PRODUCT_$op_month  where zero_mark = 0"
#  set UnUsing_Count [get_single $sql_buff]
#  puts "UnUsing_Count： $UnUsing_Count"
#  #--------------------------------------------------------------------------------------------------------------
  
  
  #8.新业务占用号码数
 	set sql_buff "select count(product_no)
                  from bass2.dw_product_$op_month
                 where userstatus_id in (1,2,3,6,8) and 
                       usertype_id in (1,2,9)     and 
                       crm_brand_id3 in (800,700)"
  set NewOperate_Count [get_single $sql_buff]
  puts "NewOperate_Count： $NewOperate_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #9.测试号码数
 	set sql_buff "select count(product_no)
                  from bass2.dw_product_$op_month
                 where userstatus_id in (1,2,3,6,8) and 
                       usertype_id in (1,2,9)     and 
                       test_mark = 1"
  set Test_Count [get_single $sql_buff]
  puts "Test_Count： $Test_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #10.预销号码数
 	set sql_buff "select count(distinct product_no) from bass2.dw_product_$op_month where userstatus_id in (2,3,6,7)"
  set Preplogout_Count [get_single $sql_buff]
  puts "Preplogout_Count： $Preplogout_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #11.冷冻期号码数
 	set sql_buff "select count(distinct product_no) from bass2.dw_product_$op_month where userstatus_id in (9)"
  set Freeze_Count [get_single $sql_buff]
  puts "Freeze_Count： $Freeze_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  
  set sql_buff "insert into BASS1.G_S_22041_MONTH
                   (TIME_ID           ,
                    BILL_MONTH        ,
                    UnAllot_Count     ,
                    UnFacture_Count   ,
                    Store_Count       ,
                    Trench_Count      ,
                    CanCallback_Count ,
                    Using_Count       ,
                    UnUsing_Count     ,
                    NewOperate_Count  ,
                    Test_Count        ,
                    Preplogout_Count  ,
                    Freeze_Count      
                   ) VALUES
                   (
                     $op_month         ,
                    '$op_month'        ,
                    '$UnAllot_Count'   ,
                    '$UnFacture_Count ',
                    '$Store_Count'     ,
                    '$Trench_Count'    ,
                    '$CanCallback_Count',
                    '$Using_Count'     ,
                    '$UnUsing_Count'   ,
                    '$NewOperate_Count',
                    '$Test_Count'      ,
                    '$Preplogout_Count',
                    '$Freeze_Count'
                    ) "  
    puts $sql_buff      
    exec_sql $sql_buff      
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


