######################################################################################################
#�������ƣ�	INT_CHECK_R291_DAY.tcl
#У��ӿڣ�	
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-06-09 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_R291_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R291'					
					)
			"

		exec_sql $sql_buff



##~   R291	��	02_���ſͻ�	���ſͻ��˿���Դʹ������ӿڵġ����ſͻ���ʶ�����ڼ��ſͻ��ӿڵġ����ſͻ���ʶ����	"22036 ���ſͻ��˿���Դʹ�����
##~   01004 ���ſͻ�"	���ſͻ��˿���Դʹ������ӿڵġ����ſͻ���ʶ�����ڼ��ſͻ��ӿڵġ����ſͻ���ʶ����	0.05		"Step1.ͳ��22036�����ſͻ��˿���Դʹ��������ӿ��У���ֹ��ͳ������ĩ�ͻ�����=0�����ſͻ����ġ����ſͻ���ʶ�����ϣ�
##~   Step2.ͳ��01004�����ſͻ����ӿ��У���ֹͳ������ĩ���ſͻ���ʶ���ռ��ϣ�
##~   Step3.ͳ��Step1���ϲ���Step2�����еļ��ſͻ���ʶ���������������0����Υ������"



	set sql_buff "
		select count(0) from  table (
		 select distinct APP_LENCODE from 
								(
												select t.*
												,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME 
												order by time_id desc ) rn 
												from 
												G_A_22036_DAY  t
												where 
												TIME_ID/100 <= $curr_month										
								  ) a
								where rn = 1  
								and bigint(OPEN_DATE)/100 <= $curr_month
								and  CUST_TYPE = '0'
		) t where 
		 APP_LENCODE not in (                  
		select enterprise_id
		from  table (
					select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $curr_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' 
			)   t                     
		)
		with ur
"

chkzero2 $sql_buff "R291 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R291',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  


	return 0
}
