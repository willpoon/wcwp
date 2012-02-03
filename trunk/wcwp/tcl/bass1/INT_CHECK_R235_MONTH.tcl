######################################################################################################
#�������ƣ�	INT_CHECK_R235_MONTH.tcl
#У��ӿڣ�	03004
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
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
        set app_name "INT_CHECK_R235_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R235','R237') 
 	  "        

	  exec_sql $sqlbuf



#R235			����	��	06_��������	������ͨ���û���������������û���ռ��	������ͨ���û������������루�������ײͷѺͻ�������ѣ����û���ռ�ȡ�3%	0.05		



set sql_buff "ALTER TABLE G_S_21003_MONTH_mobile ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"

exec_sql $sql_buff
#ȡͨ���û�
set sql_buff "
insert into  BASS1.G_S_21003_MONTH_mobile
select  PRODUCT_NO from G_S_21003_MONTH 
where time_id = $op_month  
group by PRODUCT_NO
having sum(bigint(BASE_BILL_DURATION)) > 0
"
exec_sql $sql_buff


aidb_runstats bass1.G_S_21003_MONTH_mobile 3


source /bassapp/bass1/tcl/INT_FIX_TMP.tcl

ADJ_R235_MONTH1  $op_time $optime_month
ADJ_R235_MONTH2  $op_time $optime_month



#������ͨ���û�
set sql_buff "
select count(distinct b.user_id) from     BASS1.G_S_21003_MONTH_mobile a 
,int_02004_02008_month_stage b 
where a.product_no = b.product_no
and  b.usertype_id NOT IN ('2010','2020','2030','2040','9000')
and b.test_flag = '0'
"
 	set RESULT_VAL1 0
 	set RESULT_VAL1 [get_single $sql_buff]

#
set sql_buff "
 select count(0)
 from
 (
		 select distinct user_id from 
		  BASS1.G_S_21003_MONTH_mobile 	 a 
			,int_02004_02008_month_stage b 
		where a.product_no = b.product_no
		and  b.usertype_id NOT IN ('2010','2020','2030','9000')
		and b.test_flag = '0'
) a 
left join 
(		select user_id
		from g_s_03004_month
		where time_id = $op_month 
		and 
		 ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)
		 		or ACCT_ITEM_ID in ('0401','0403','0407')
		 )
		 group by user_id 
		 having sum(bigint(FEE_RECEIVABLE)) > 0
) b  on  a.user_id = b.user_id 
 where b.user_id is  null 
 
 "
 
  set RESULT_VAL2 0
 	set RESULT_VAL2 [get_single $sql_buff]
 	
set sql_buff "
values $RESULT_VAL2*1.000/$RESULT_VAL1    
"

 
  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.03||$RESULT_VAL<-0.03 } {
		set grade 2
	  set alarmcontent "R235 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R235',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff



########################################################
#R237			����	��	06_��������	��ĩ״̬Ϊ���������˵����û���ռ��	��ĩ״̬Ϊ����(1010)�������˵����û���ռ�ȡ�5%	0.05		

set sql_buff "
                select count(0) val1 
                from  (select user_id from  int_02004_02008_month_stage 
                            where usertype_id  IN ('1010') and  TEST_FLAG = '0'
                    ) a
                left join 
                (               select user_id
                                from g_s_03004_month
                                where time_id = $op_month 
                                 group by user_id                                  
                ) b  on  a.user_id = b.user_id
 							where b.user_id is  null 
"
 	set RESULT_VAL1 0
 	set RESULT_VAL1 [get_single $sql_buff]


set sql_buff "
select count(0) val2 from   int_02004_02008_month_stage a 
		  where a.usertype_id  IN ('1010')

 "
 
  set RESULT_VAL2 0
 	set RESULT_VAL2 [get_single $sql_buff]
 	
 	
set sql_buff "
		values $RESULT_VAL1*1.00/$RESULT_VAL2 
 "
 
  	

  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.05||$RESULT_VAL<-0.05 } {
		set grade 2
	  set alarmcontent "R237 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R237',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff  
      	
	return 0
}
