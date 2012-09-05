######################################################################################################
#�������ƣ�	INT_CHECK_R147R148_DAY.tcl
#У��ӿڣ�	01007
#��������: DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
		##~   set op_time 2012-08-25
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
	set last_day [GetLastDay [string range $timestamp 0 7]]
#for WriteAlarm
set optime $op_time
        
        #������
        set app_name "INT_CHECK_R147R148_DAY.tcl"



########################################################################################################3

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R147','R148') "        
	  exec_sql $sqlbuf


##~   R147	��	04_TDҵ��	ʹ��TD����ͻ�����T���ϼƷ�ʱ���ձ䶯��	22202 ʹ��TD����Ŀͻ�ͨ������ջ���	ʹ��TD����ͻ�����T���ϼƷ�ʱ���ձ䶯�� �� 35%	0.05	�޸�ָ��У����ֵ��

	set sqlbuf "
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$timestamp with ur"
	set RESULT_VAL1 [get_single $sqlbuf]

	set sqlbuf "
		select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$last_day  with ur"
	set RESULT_VAL2 [get_single $sqlbuf]

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3

	#��У��ֵ����У������
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R147'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)
	"
	  exec_sql $sqlbuf


	if {$RESULT_VAL3>=0.3 || $RESULT_VAL3<=-0.3 } {
		set grade 2
	    set alarmcontent "R147 �����Լ��ʹ��TD����Ŀͻ���T���ϼƷ�ʱ������30%���ӽ�35%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R147 end---------------------------------------"

##~   R148	��	04_TDҵ��	ʹ��TD����ͻ�����T�������������ձ䶯��	22203 ʹ��TD����Ŀͻ����������ջ���	ʹ��TD����ͻ�����T�������������ձ䶯�� �� 50%	0.05	�޸�ָ��У����ֵ��
	set RESULT_VAL1 0
	set RESULT_VAL2 0
	set RESULT_VAL3 0

	set sqlbuf "
	select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$timestamp with ur"
	set RESULT_VAL1 [get_single $sqlbuf]


    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ʹ��TD����Ŀͻ����������ջ���(tb_sum_td_usd_net_cust_data_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ�ʹ��TD����Ŀͻ���T���ϵ�������������
    
	set sqlbuf "
		select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$last_day with ur"
	set RESULT_VAL2 [get_single $sqlbuf]

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3


	#��У��ֵ����У������
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R148'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)
	"
	  exec_sql $sqlbuf


	if {$RESULT_VAL3>=0.45 || $RESULT_VAL3<=-0.45 } {
		set grade 2
	    set alarmcontent "R148 �����Լ��ʹ��TD����Ŀͻ���T���ϵ�������������45%���ӽ�50%"
	    WriteAlarm $app_name $timestamp $grade ${alarmcontent}
	} 

   puts "R148 end---------------------------------------"

	return 0
}

