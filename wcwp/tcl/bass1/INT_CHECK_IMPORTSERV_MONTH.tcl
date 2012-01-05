######################################################################################################
#程序名称：INT_CHECK_IMPORTSERV_MONTH.tcl
#校验接口：03004 03005
#运行粒度: 月
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-23
#问题记录：
#修改历史: # 2011.12.26 修改错误的日期取值，格式错误
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $last_month
        
        #自然月第一天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        puts $timestamp
        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        puts $l_timestamp
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #程序名
        set app_name "INT_CHECK_IMPORTSERV_MONTH.tcl"

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #本月最后一天#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #上月最后一天 yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day


  #删除本期数据	set sql_buff "delete from BASS1.G_RULE_CHECK where time_id=$op_month	and rule_code in ('R130','R129','R128')"	exec_sql $sql_buff

#########################################################################################
#                                                                                       #
#                                   重点业务指标校验                                    #
#                                                                                       #
#########################################################################################

####  R130 增值业务收入                        ##########################################
   
   set sqlbuf "select cast(cast(sum(case when bigint(acct_item_id)/100 in (5,6,7) then bigint(fee_receivable) else 0 end) as decimal(20,6))/cast( sum(bigint(fee_receivable)) as decimal(20,6)) as decimal(10,6))
               from G_S_03004_MONTH 
               where time_id = $op_month
               with ur"
   

               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.4f" [expr ${RESULT_VAL1} /1.00]]
   
   puts $RESULT_VAL1
   
   set sqlbuf "select cast(cast(sum(case when bigint(acct_item_id)/100 in (5,6,7) then bigint(fee_receivable) else 0 end) as decimal(20,6))/cast( sum(bigint(fee_receivable)) as decimal(20,6)) as decimal(10,6))
               from G_S_03004_MONTH 
               where time_id = $last_month
               with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.4f" [expr ${RESULT_VAL2} /1.00]]
   
   puts $RESULT_VAL2
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R130',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [format "%.4f" [expr $RESULT_VAL1*1.000/$RESULT_VAL2 -1 ]]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.2 || $RESULT_VAL3<-0.2 } {
		set grade 2
	        set alarmcontent "R130 校验不通过:增值业务收入占比指标月变动率 > 20%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R130:增值业务收入占比指标月变动率 <= 20%  end"

####################################################################################
          
###  R129 全球通目标客户市场占有率   ###############################################

   set sqlbuf "select cast(cast(sum(case when i.brand_id='1' then 1 else 0 end) as decimal(20,6))  /cast (sum(case when h.user_id is not null then 1 else 0 end) as decimal(20,6)) as decimal(10,6))  from
               (
               select user_id
                 from BASS1.G_S_03005_MONTH 
                 where time_id=$op_month
               group by user_id
               having sum(bigint(SHOULD_FEE))>12000
               ) h
               inner join
               (
               select a.user_id,a.brand_id
               from
               (
               select user_id, brand_id
               from
               (
               select USER_ID, BRAND_ID,row_number()over(partition by user_id order by time_id desc) row_id
                 from BASS1.G_A_02004_DAY 
               where time_id<=$last_month_day 
               ) k
               where k.row_id=1
               ) a
               inner join 
               (
               select user_id,usertype_id from
               (
               select f.user_id,f.usertype_id from
               (
               select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id 
               from BASS1.G_A_02008_DAY where time_id<=$last_month_day
               ) f
               where f.row_id=1
               ) m
               ) b
               on a.user_id=b.user_id
                and b.usertype_id like '1%'
               ) i
               on h.user_id=i.user_id
               
                 with ur  "
   

               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.4f" [expr ${RESULT_VAL1} /1.00]]
   
   puts $RESULT_VAL1
   
   set sqlbuf "select cast(cast(sum(case when i.brand_id='1' then 1 else 0 end) as decimal(20,6))  /cast (sum(case when h.user_id is not null then 1 else 0 end) as decimal(20,6)) as decimal(20,6))  from
               (
               select user_id
                 from BASS1.G_S_03005_MONTH 
                 where time_id=$last_month
               group by user_id
               having sum(bigint(SHOULD_FEE))>12000
               ) h
               inner join
               (
               select a.user_id,a.brand_id
               from
               (
               select user_id, brand_id
               from
               (
               select USER_ID, BRAND_ID,row_number()over(partition by user_id order by time_id desc) row_id
                 from BASS1.G_A_02004_DAY 
               where time_id<=$last_month_last_day
               ) k
               where k.row_id=1
               ) a
               inner join 
               (
               select user_id,usertype_id from
               (
               select f.user_id,f.usertype_id from
               (
               select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id 
               from BASS1.G_A_02008_DAY where time_id<=$last_month_last_day
               ) f
               where f.row_id=1
               ) m
               ) b
               on a.user_id=b.user_id
                and b.usertype_id like '1%'
               ) i
               on h.user_id=i.user_id
               
                 with ur  "
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.4f" [expr ${RESULT_VAL2} /1.00]]
   
   puts $RESULT_VAL2
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R129',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  #set  RESULT_VAL3 [expr $RESULT_VAL1-$RESULT_VAL2 ]  set  RESULT_VAL3 [format "%.4f"  [expr $RESULT_VAL1-$RESULT_VAL2 ]]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.1 || $RESULT_VAL3<-0.1 } {
		set grade 2
	        set alarmcontent "R129 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "增值业务收入占比指标月变动率 ≤ 10％"
	
################################################################################	



####  R128 中高端保有客户数月变动率   ##########################################
   
   set sqlbuf " select count(distinct m.user_id) from
                (
                select a.user_id from bass2.dw_product_snapshot a
                except
                select user_id from
                (
                select distinct f.user_id from
                              (
                              select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id 
                              from BASS1.G_A_02008_DAY where time_id<=$last_month_day
                              ) f
                  where f.row_id=1 
               	and  f.usertype_id like '2%'		
                union	 
                
                select distinct user_id from
                (
                select user_id,count(*)
                 from
                 (
                 select time_id,user_id
                 from BASS1.G_S_03005_MONTH 
                 where time_id<=$op_month
                 and time_id>=200901
                 group by time_id,user_id
                 having sum(bigint(SHOULD_FEE))<5000
                 ) a
                 group by user_id
                 having count(*)>=3
                ) a
               ) b
               ) m
                 with ur  "
   
   puts $sqlbuf
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.4f" [expr ${RESULT_VAL1} /1.00]]
   
   puts $RESULT_VAL1
   
   set sqlbuf "select count(distinct m.user_id) from
                (
                select a.user_id from bass2.dw_product_snapshot a
                except
                select user_id from
                (
                select distinct f.user_id from
                              (
                              select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id 
                              from BASS1.G_A_02008_DAY where time_id<=$last_month_last_day
                              ) f
                  where f.row_id=1 
               	and  f.usertype_id like '2%'		
                union	 
                
                select distinct user_id from
                (
                select user_id,count(*)
                 from
                 (
                 select time_id,user_id
                 from BASS1.G_S_03005_MONTH 
                 where time_id<=$last_month
                 and time_id>=200901
                 group by time_id,user_id
                 having sum(bigint(SHOULD_FEE))<5000
                 ) a
                 group by user_id
                 having count(*)>=3
                ) a
               ) b
               ) m
                 with ur  "
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.4f" [expr ${RESULT_VAL2} /1.00]]
   
   puts $RESULT_VAL2
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R128',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
#  set  RESULT_VAL3 [expr 1-$RESULT_VAL2/$RESULT_VAL1 ]set  RESULT_VAL3 [format "%.4f"  [expr 1-$RESULT_VAL2/$RESULT_VAL1 ]]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.05 } {
		set grade 2
	        set alarmcontent "R128 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "中高端拍照客户保有数指标月变动率 ≤ 5％"

####################################################################################

		
	return 0
}

