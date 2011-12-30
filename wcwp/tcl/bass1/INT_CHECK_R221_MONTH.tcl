######################################################################################################
#�������ƣ�	INT_CHECK_R221_MONTH.tcl
#У��ӿڣ�	03017 02054
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  #���ֿھ����죬�����������Ƚ������0��ͬ�򣩵����⡣
#select (val1 + val2 - val3 )/100  fee -> select (val1 + val2 - val3 )  fee
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #������
        global app_name
        set app_name "INT_CHECK_R221_MONTH.tcl"


########################################################################################################3

#R221			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ�����"����ܽӿ��е�"TD�ͻ�����"��ƫ��	���ܽӿ�"TD�ͻ�����"��ͨ���굥ͳ�Ƶ�TD�ͻ�������ƫ����1%����	0.1		
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
	 

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R221 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	} 

	 	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R221',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff
		

###########################################################

#R222			����	��	04_TDҵ��	��TD�굥���м������"ʹ��TD����Ŀͻ���"����ܽӿ��е�"ʹ��TD����Ŀͻ���"��ƫ��	���ܽӿ�"ʹ��TD����Ŀͻ���"��ͨ���굥ͳ�Ƶ�ʹ��TD����ͻ�����ƫ����1%����	0.1		

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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R222 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R222',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R223			����	��	04_TDҵ��	��TD�굥���м������"TD�ն˿ͻ���"����ܽӿ��е�"TD�ն˿ͻ���"��ƫ��	���ܽӿ�"TD�ն˿ͻ���"��ͨ���굥ͳ�Ƶ�TD�ն˿ͻ�����ƫ����1%����	0.1		

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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R223 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R223',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R224			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ����ܼƷ�ʱ��"����ܽӿ��е�"TD�ͻ����ܼƷ�ʱ��"��ƫ��	���ܽӿ�"TD�ͻ����ܼƷ�ʱ��"��ͨ���굥ͳ�Ƶ�TD�ͻ����ܼƷ�ʱ����ƫ����1%����	0.1		

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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R224 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R224',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


###########################################################

#R225			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ���T���ϵļƷ�ʱ��"����ܽӿ��е�"TD�ͻ���T���ϵļƷ�ʱ��"��ƫ��	���ܽӿ�"TD�ͻ���T���ϵļƷ�ʱ��"��ͨ���굥ͳ�Ƶ�TD�ͻ���T���ϵļƷ�ʱ����ƫ����1%����	0.1		

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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R225 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R225',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff



###########################################################

#R226			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ�������������"����ܽӿ��е�"TD�ͻ�������������"��ƫ��	���ܽӿ�"TD�ͻ�������������"��ͨ���굥ͳ�Ƶ�TD�ͻ���������������ƫ����1%����	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  	where time_id=$op_month and rule_code in ('R226') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow ��������
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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R226 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R226',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




###########################################################

#R227			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ���T���ϵ���������"����ܽӿ��е�"TD�ͻ���T���ϵ���������"��ƫ��	���ܽӿ�"TD�ͻ���T���ϵ���������"��ͨ���굥ͳ�Ƶ�TD�ͻ���T���ϵ�����������ƫ����1%����	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month and rule_code in ('R227') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow ��������
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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R227 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R227',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




###########################################################

#R228			����	��	04_TDҵ��	��TD�굥���м������"TD�ͻ�����"����ܽӿ��е�"TD�ͻ�����"��ƫ��	���ܽӿ�"TD�ͻ�����"��ͨ���굥ͳ�Ƶ�TD�ͻ������ƫ����1%����	0.1		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month and rule_code in ('R228') 
 	  "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
   #--td_check_user_flow ��������
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

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.01||$RESULT_VAL<-0.01 } {
		set grade 2
	  set alarmcontent "R228 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R228',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff




	return 0
}
