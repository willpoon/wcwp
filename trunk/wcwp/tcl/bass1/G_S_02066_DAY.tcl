
######################################################################################################		
#�ӿ�����: �û��ڵ�������ԤԼ�������                                                               
#�ӿڱ��룺02066                                                                                          
#�ӿ�˵������¼�û�ͨ����������ԤԼ���������������Ҫ���������������
#��������: G_S_02066_DAY.tcl                                                                            
#��������: ����02066������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#		  2. panzw 20120111     ����Q1201060011���ʡ��˾�˲�2011��11�µ�������ԤԼ���������� �˲飬Ϊ��֤����һ�£��ݲ���������
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#	      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#	      puts $timestamp
#	    #���� yyyymm
#	    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#	    puts $op_month
#	      
#	    #�ϸ��� yyyymm
#	    set last_month [GetLastMonth [string range $op_month 0 5]]
#	    puts $last_month
#	
#	        #������
#	        global app_name
#	        set app_name "G_S_02066_DAY.tcl"
#		
#	  #ɾ����������
#	  	set sql_buff "delete from bass1.G_S_02066_DAY where time_id <= 20111001"
#		exec_sql $sql_buff
#		
#		set sql_buff "delete from bass1.G_S_02066_DAY where time_id=$timestamp"
#		exec_sql $sql_buff
#	
#	
#		
#	  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
#		set sql_buff1 "
#		
#	insert into bass1.G_S_02066_DAY
#	select 
#		$timestamp time_id
#		,t.user_id USER_ID
#		,'BASS1_ST' CHANNEL_ID
#		,'1' FETCH_WAY
#	from ( select b.user_id , row_number()over(partition by b.user_id order by a.create_date desc )	 rn 
#	from  bass2.dw_res_sel_num_cust_log_ds a 
#		,bass2.dw_product_$timestamp b 
#	where   a.BILL_ID = b.product_no 
#		 and b.userstatus_id in (1,2,3,6,8)
#		 and b.usertype_id in (1,2,9)
#		 and a.opt_code in ('ZDYZ')
#		 and date(a.create_date + 5 days ) >= date(b.create_date)
#	) t where rn = 1 	 
#		with ur
#	  "
#	  
#		set sql_buff2 "
#		
#	insert into bass1.G_S_02066_DAY
#	select 
#		$timestamp time_id
#		,t.user_id USER_ID
#		,'BASS1_ST' CHANNEL_ID
#		,'1' FETCH_WAY
#	from ( select b.user_id , row_number()over(partition by b.user_id order by a.create_date desc )	 rn 
#	from  bass2.dw_res_sel_num_cust_log_ds a 
#		,bass2.dw_product_$timestamp b 
#	where   a.BILL_ID = b.product_no 
#		 and b.userstatus_id in (1,2,3,6,8)
#		 and b.usertype_id in (1,2,9)
#		 and a.opt_code in ('ZDYZ')
#		 and date(a.create_date + 5 days ) >= date(b.create_date)
#		 and date(b.create_date) = '$op_time'
#	) t where rn = 1 	 
#		with ur
#	  "  
#	
#	if { $timestamp == 20111001 } {
#		exec_sql $sql_buff1
#	} else {
#		exec_sql $sql_buff2
#	}
#	
#	  #���н�����ݼ��
#	  #1.���chkpkunique
#		set tabname "G_S_02066_DAY"
#		set pk 			"user_id"
#		chkpkunique ${tabname} ${pk} ${timestamp}
#	  

	return 0
}

