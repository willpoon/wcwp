######################################################################################################
#�������ƣ�	INT_CHECK_R257_MONTH.tcl
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
        set app_name "INT_CHECK_R257_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R257') 
 	  "        

	  exec_sql $sqlbuf


##~   R257	��	09_��������	���������ɷ�ռ��	"22048 ���г�ֵ��ҵ���ջ���
##~   22066 ���������ؼ������ջ���
##~   22068 ���д��մ��ɼ������Ϣ
##~   22091 ʵ������ҵ������ջ���"	���������ɷ�ռ�� �� 5%	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

##~   4.1���ǰ�ᔵ����Ҳһ�ӣ��õı�Ҳ��ͬ
if { $op_month == 201203 } {
	set TAB22048 "G_S_22048_DAY_bak20120331"
} else {
	set TAB22048 "G_S_22048_DAY"
}

set sql_buff "
		select fee1+fee2 val1,fee1+fee2+fee3+fee4+fee5 val2 ,(fee1+fee2)*1.00/(fee1+fee2+fee3+fee4+fee5) val3
		from 
		(
		select sum(bigint(CHRG_REC_FEE)) fee1 from G_S_22066_DAY
		where time_id / 100 = $op_month
		) a,
		(
		select sum(bigint(BNK_TS_FEE)) fee2 from G_S_22068_MONTH
		where time_id  = $op_month
		) b,
		(
		select sum(bigint(PAYMENT_REC_FEE)) fee3 from G_S_22091_DAY
		where time_id/100  = $op_month
		and DEAL_TYPE = '1'
		) c,
		(
		select sum(bigint(CHRG_AMT))  fee4 from $TAB22048
		where time_id/100  = $op_month
		) d,
		(
		select sum(bigint(COUNTER_REC_FEE)+bigint(EBANK_REC_FEE)+bigint(OTH_REC_FEE)) fee5 from G_S_22068_MONTH
		where time_id  = $op_month
		) e
		with ur
"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R257',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL3 < 0.05 } {
		set grade 2
	  set alarmcontent "R257 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
