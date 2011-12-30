#=====================================================================*
#*规则编号：22043     G_S_22043_MONTH.tcl                              *
#*规则类型：月                                                         *
#*规则描述：22043接口程序                                               *
#功能描述：记录号码利用率和回收的情况                                    *
#调用例程：int -s G_S_22043_MONTH.tcl                                  *
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
	set sql_buff "delete from bass1.G_S_22043_MONTH where TIME_ID= $op_month"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  

  #1.号码总数 集团公司分配给本省的号码数
 	set sql_buff "select count(distinct product_no)  from bass2.DW_RES_MSISDN_$op_month "
  set Total_Count [get_single $sql_buff]
  puts "Total_Count： $Total_Count"
  #--------------------------------------------------------------------------------------------------------------
     
  #2.客户到达数
 	set sql_buff "select  count(user_id)
          from   bass2.dw_product_$this_month
          where  userstatus_id in (1,2,3,6,8) 
            and usertype_id in (1,2,9)
			with ur "
  set Arrive_Count [get_single $sql_buff]
  puts "Arrive_Count： $Arrive_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #3.本月放号数
 	set sql_buff " select count(*)
                   from bass2.dw_product_$this_month
                  where month_new_mark = 1  and 
                        usertype_id in (1,2,9)"
  set Putout_Count [get_single $sql_buff]
  puts "Putout_Count： $Putout_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #4.本月放号数中新号码投放数
 	set sql_buff "select count(distinct a.user_id)
                  from bass2.dw_product_$this_month a
                       left join
                         bass2.dw_product_$last_month b
                       on a.product_no = b.product_no
                 where a.month_new_mark = 1     and 
                       a.usertype_id in (1,2,9) and 
                       b.product_no is null"
  set PutoutNewNo_Count [get_single $sql_buff]
  puts "PutoutNewNo_Count： $PutoutNewNo_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #5.本月放号数中旧号码投放数
 	set sql_buff "select count(distinct a.user_id)
                  from bass2.dw_product_$this_month a
                       left join
                         bass2.dw_product_$last_month b
                       on a.product_no = b.product_no
                 where a.month_new_mark = 1     and 
                       a.usertype_id in (1,2,9) and 
                       b.product_no is not null"
  set PutoutOldNo_Count [get_single $sql_buff]
  puts "PutoutOldNo_Count： $PutoutOldNo_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #6.本月离网数	本月离网（销号）客户数
 	set sql_buff "select count(*)
                  from bass2.dw_product_$this_month
                 where month_off_mark = 1 and 
                       usertype_id in (1,2,9)"
  set Leave_Count [get_single $sql_buff]
  puts "Leave_Count： $Leave_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  
  #7.本年累计放号数
 	set sql_buff "select value(char(SUM(bigint(Putout_Count))),'0')
                  from bass1.G_S_22043_MONTH
                 where time_id/100 = $year"
  set PutoutLJ_Count [get_single $sql_buff]
  puts "PutoutLJ_Count： $PutoutLJ_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #8.本年累计投放的旧号码数
 	set sql_buff "select value(char(SUM(bigint(PutoutOldNo_Count))),'0')
                  from bass1.G_S_22043_MONTH
                 where time_id/100 = $year"
  set PutoutOldLJ_Count [get_single $sql_buff]
  puts "PutoutOldLJ_Count： $PutoutOldLJ_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #9.本年累计离网数
 	set sql_buff "select value(char(SUM(bigint(Leave_Count))),'0')
                  from bass1.G_S_22043_MONTH
                 where time_id/100 = $year"
  set LeaveLJ_Count [get_single $sql_buff]
  puts "LeaveLJ_Count： $LeaveLJ_Count"
  #--------------------------------------------------------------------------------------------------------------
  
  #10.可回收号码数
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
  
  set sql_buff "insert into BASS1.G_S_22043_MONTH
                   (TIME_ID           ,
                    BILL_MONTH        ,
                    VestIn_Flat       ,
                    Total_Count       ,
                    Arrive_Count      ,
                    Putout_Count      ,
                    PutoutNewNo_Count ,
                    PutoutOldNo_Count ,
                    Leave_Count       ,
                    PutoutLJ_Count    ,
                    PutoutOldLJ_Count ,
                    LeaveLJ_Count     ,
                    CanUse_Count 
                   ) VALUES
                   (
                     $op_month        ,
                    '$op_month'        ,
                    '1'                ,
                    '$Total_Count'     ,
                    '$Arrive_Count'    ,
                    '$Putout_Count'    ,
                    '$PutoutNewNo_Count',
                    '$PutoutOldNo_Count',
                    '$Leave_Count'      ,
                    '$PutoutLJ_Count'   ,
                    '$PutoutOldLJ_Count',
                    '$LeaveLJ_Count'    ,
                    '$CanCallback_Count'
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


