
######################################################################################################		
#�ӿ�����: �û�ѡ��WLAN�Զ���֤�ʷ��ײ�                                                               
#�ӿڱ��룺02037                                                                                          
#�ӿ�˵�������ϱ��û�ѡ���WLAN�Զ���֤�ʷ��ײ͵Ķ�����¼���ײ�����Ϊ��WLAN�Զ���֤�Էѡ����ײ͵���Ϊ20���ײ��Էѱ���Ϊ999912120590020001���ײͲ���ʱ���� ����1G�ⶥ��
#��������: G_I_02037_DAY.tcl                                                                            
#��������: ����02037������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120507
#�����¼��
#�޸���ʷ: 1. panzw 20120507	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.0) 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #�ϸ��� yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #������
        global app_name
        set app_name "G_I_02037_DAY.tcl"
	
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02037_DAY where time_id=$timestamp"
	exec_sql $sql_buff

  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
	set sql_buff "
			
		insert into bass1.G_I_02037_DAY
		(
				 TIME_ID
				,USER_ID
				,PKG_ID
				,EFF_DT
		)
		select 
			$timestamp TIME_ID
			,a.PRODUCT_INSTANCE_ID USER_ID
			,'999912120590020001' PKG_ID
			,replace(char(date(a.VALID_DATE) ),'-','') EFF_DT
		from  bass2.Dw_product_ins_off_ins_prod_ds a 
			, bass2.dw_product_$timestamp b 
		where  a.state =1 
			and a.PRODUCT_INSTANCE_ID=b.user_id 
			and b.usertype_id in (1,2,9) 
			and b.userstatus_id in (1,2,3,6,8)
			and a.valid_type in (1,2)
			 and a.OP_TIME = '$op_time'
			 and date(a.VALID_DATE)<='$op_time'
			 and date(a.expire_date) >= '$op_time'
			and not exists ( 
					 select 1 from bass2.dwd_product_test_phone_$timestamp b 
					 where a.product_instance_id = b.USER_ID  and b.sts = 1
					)   
			and a.offer_id in (113500001225,113500001225)
		with ur
		
		  "
	exec_sql $sql_buff

	
  #���н�����ݼ��
  #1.���chkpkunique
	set tabname "G_I_02037_DAY"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}
  
  #2.��������û��Ƿ����û�����ͷ
	set sql_buff "select count(*) from 
	            (
		     select user_id from bass1.G_I_02037_DAY
		      where time_id =$timestamp
		       except
		  select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
	            ) as a
	            "
	chkzero2 $sql_buff "���û������û�����ͷ"

	return 0
}

