######################################################################################################
#程序名称：	INT_CHECK_R221_MONTH.tcl
#校验接口：	03017 02054
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  #部分口径差异，后续修正。先解决大于0（同向）的问题。
#select (val1 + val2 - val3 )/100  fee -> select (val1 + val2 - val3 )  fee
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        global app_name
        set app_name "INT_CHECK_R221_MONTH.tcl"


########################################################################################################3

#R221			新增	月	04_TD业务	各TD详单表中计算出的"TD客户总数"与汇总接口中的"TD客户总数"的偏差	汇总接口"TD客户总数"与通过详单统计的TD客户总数的偏差在1%以内	0.1		
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
			where time_id=$op_month and rule_code in ('R221') "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
	 set RESULT_VAL	 0   
	 set sql_buff "
	select a.TD_CUST*1.0000 , b.cnt*1.0000
				from 
				(
					select  bigint(TD_CUST) TD_CUST from   G_S_22204_MONTH
					where time_id = $op_month
					and CYCLE_ID = '3'							
				)a,
				(
					select count(distinct PRODUCT_NO) cnt 
					from bass1.td_check_06_2010 a 
				) b
			with ur
	"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL
	 

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R221 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	} 

	 	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R221',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff
		

###########################################################

#R222			新增	月	04_TD业务	各TD详单表中计算出的"使用TD网络的客户数"与汇总接口中的"使用TD网络的客户数"的偏差	汇总接口"使用TD网络的客户数"与通过详单统计的使用TD网络客户数的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
	  where time_id=$op_month and rule_code in ('R222') "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   
	set sql_buff "
	select a.TD_NET_CUST*1.0000 , b.cnt*1.0000
				from 
				(
					select  bigint(TD_NET_CUST) TD_NET_CUST from   G_S_22204_MONTH
					where time_id = $op_month
					and CYCLE_ID = '3'			
				)a,
				(
					select count(distinct PRODUCT_NO) cnt 
					from bass1.td_check_06_2010 a 
					where ( TD_CDR_NET_MARK = '1' or TD_GPRS_NET_MARK = '1' or SHANGWANGBEN_MARK = '1' )
				) b
			with ur
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R222 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R222',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R223			新增	月	04_TD业务	各TD详单表中计算出的"TD终端客户数"与汇总接口中的"TD终端客户数"的偏差	汇总接口"TD终端客户数"与通过详单统计的TD终端客户数的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
			where time_id=$op_month and rule_code in ('R223') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   
	set sql_buff "
	select a.TD_TERM_CUST*1.0000 , b.cnt*1.0000
				from 
				(
					select  bigint(TD_TERM_CUST) TD_TERM_CUST from   G_S_22204_MONTH
					where time_id = $op_month
					and CYCLE_ID = '3'			
				)a,
				(
					select count(distinct PRODUCT_NO) cnt 
					from bass1.td_check_06_2010 a 
					where ( TD_TERMINAL_MARK = '1' )
				) b
			with ur
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R223 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R223',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R224			新增	月	04_TD业务	各TD详单表中计算出的"TD客户的总计费时长"与汇总接口中的"TD客户的总计费时长"的偏差	汇总接口"TD客户的总计费时长"与通过详单统计的TD客户的总计费时长的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
			where time_id=$op_month and rule_code in ('R224') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   
	set sql_buff "
	select a.TD_CUST_DUR*1.0000 , b.dur*1.0000
				from 
				(
					select  bigint(TD_CUST_DUR) TD_CUST_DUR from   G_S_22204_MONTH
					where time_id = $op_month
					and CYCLE_ID = '3'
				)a,
				(
					select sum(bigint(BILL_DURATION)) dur from bass1.g_s_21003_month_td 

				) b
			with ur
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R224 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R224',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R225			新增	月	04_TD业务	各TD详单表中计算出的"TD客户在T网上的计费时长"与汇总接口中的"TD客户在T网上的计费时长"的偏差	汇总接口"TD客户在T网上的计费时长"与通过详单统计的TD客户在T网上的计费时长的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
			where time_id=$op_month and rule_code in ('R225') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   
	set sql_buff "
	select a.td_cust_t_dur*1.0000 , b.dur*1.0000
				from 
				(
					select  bigint(td_cust_t_dur) td_cust_t_dur from   G_S_22204_MONTH
					where time_id = $op_month
					and CYCLE_ID = '3'
				)a,
				(
					select sum(bill_duration)  dur
					from bass1.g_s_21003_month_td
				   where mns_type='1'
				) b
			with ur
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R225 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R225',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff



###########################################################

#R226			新增	月	04_TD业务	各TD详单表中计算出的"TD客户的总数据流量"与汇总接口中的"TD客户的总数据流量"的偏差	汇总接口"TD客户的总数据流量"与通过详单统计的TD客户的总数据流量的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  	where time_id=$op_month and rule_code in ('R226') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow 仅上网本
	set sql_buff "
select a.TD_CUST_FLOWS*1.0000 , b.flows*1.0000
from 
(
select  bigint(TD_CUST_FLOWS) TD_CUST_FLOWS from   G_S_22204_MONTH
where time_id = $op_month
and CYCLE_ID = '3'
)a,
(
	select (val1 + val2 - val3)/1024/1024 flows
	from 
	(
			select 
			sum(bigint(a.flows))*1.00 val3 
			from bass1.g_s_04002_day_flows a,
			bass1.td_check_user_flow b
			where a.product_no=b.product_no
	) a
	,(
	
			select 
			sum(bigint(flows))*1.00 val2 
			from bass1.g_s_04018_day_flows
	) b
	,(
			select 
			sum(bigint(flows))*1.00 val1 
			from bass1.g_s_04002_day_flows
	) c
) b
with ur
"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R226 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R226',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




###########################################################

#R227			新增	月	04_TD业务	各TD详单表中计算出的"TD客户在T网上的数据流量"与汇总接口中的"TD客户在T网上的数据流量"的偏差	汇总接口"TD客户在T网上的数据流量"与通过详单统计的TD客户在T网上的数据流量的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month and rule_code in ('R227') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow 仅上网本
set sql_buff "
		select a.TD_CUST_T_FLOWS*1.0000 , b.flows*1.0000
		from 
		(
			select  bigint(TD_CUST_T_FLOWS)  TD_CUST_T_FLOWS from   G_S_22204_MONTH
			where time_id = $op_month
			and CYCLE_ID = '3'
		) a,
		(
		select (val4 + val5 - val6)/1024/1024 flows
		from 
		(
			select 
			sum(bigint(a.flows))*1.00 val6
			from bass1.g_s_04002_day_flows a,
			bass1.td_check_user_flow b
			where a.product_no=b.product_no
			  and a.mns_type='1'
		) a ,
		(
			select 
			sum(bigint(flows))*1.00 val5
			from bass1.g_s_04018_day_flows
			where mns_type='1'
		) b ,
		(
			select 
			sum(bigint(flows))*1.00 val4
			from bass1.g_s_04002_day_flows
			where mns_type='1'                        
		) c     

		) b
		with ur  
"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R227 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R227',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




###########################################################

#R228			新增	月	04_TD业务	各TD详单表中计算出的"TD客户收入"与汇总接口中的"TD客户收入"的偏差	汇总接口"TD客户收入"与通过详单统计的TD客户收入的偏差在1%以内	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month and rule_code in ('R228') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow 仅上网本
	set sql_buff "
select a.TD_CUST_FEE*1.0000 , b.fee*1.0000
from 
(
select  bigint(TD_CUST_FEE) TD_CUST_FEE  from   G_S_22204_MONTH
where time_id = $op_month
and CYCLE_ID = '3'

) a,
(
select (val1 + val2 - val3 )  fee
	from 
	(
		select value(sum(a.book_fee),0)*1.00 val1
		  from g_s_03004_month_td a,bass1.td_check_user_fee1 b
		where a.product_no=b.product_no
	)  a,          
	
	(
		select value(sum(a.all_fee-a.book_fee),0)*1.00 val2 
		  from g_s_03004_month_td a,bass1.td_check_user_fee2 b
		where a.product_no=b.product_no
	)  b,          
	(
		select value(sum(a.all_fee),0)*1.00 VAL3
		  from g_s_03004_month_td a,bass1.td_check_user_fee3 b
		where a.product_no=b.product_no
	)  C               
		 
) b
with ur  
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]]
	 puts $RESULT_VAL

 	#检查合法性: 0 - 不正常； 大于0 - 正常
 # 校验值超标时告警	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R228 校验不通过"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R228',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




	return 0
}
