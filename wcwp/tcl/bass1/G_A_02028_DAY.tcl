
######################################################################################################		
#�ӿ�����: �û�ѡ��BlackBerry��BIS���ʷ��ײ�                                                               
#�ӿڱ��룺02028                                                                                          
#�ӿ�˵�������ӿ�Ϊ�������ӿڣ��״��ϱ�����״̬Ϊ������ȫ��������ϵ��
#��������: G_A_02028_DAY.tcl                                                                            
#��������: ����02028������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
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
        set app_name "G_A_02028_DAY.tcl"
	
  #ɾ����������
  	set sql_buff "delete from bass1.G_A_02028_DAY where time_id <= 20111001"
	exec_sql $sql_buff
	set sql_buff "delete from bass1.G_A_02028_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	
  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
	set sql_buff "
		insert into bass1.G_A_02028_DAY
		select 
		$timestamp time_id
		,USER_ID
		,PKG_ID
		,ORDER_DT
		,STS_CD
		from table (
		select 
		PRODUCT_INSTANCE_ID USER_ID
		,case 
			when OFFER_ID = 113000000013 then '9999121202900981011001'
			when OFFER_ID = 113000000014 then '9999121202901081011001' 
			end pkg_id
		,replace(char(date(VALID_DATE)),'-','') ORDER_DT
		,case when STATE = 1 then '1' else '2' end STS_CD
		from bass2.dw_product_ins_off_ins_prod_ds a
		where OFFER_ID in (113000000013,113000000014)
			and a.valid_type in (1,2)
			and date(a.expire_date) >= '$op_time'
			and a.VALID_DATE < a.expire_date
		except
		select 
			USER_ID
			,PKG_ID
			,ORDER_DT
			,STS_CD
		from (	
			select 
			 TIME_ID
			,USER_ID
			,PKG_ID
			,ORDER_DT
			,STS_CD
			,row_number()over(partition by user_id order by TIME_ID desc ) rn 
			from G_A_02028_DAY a 
		) t where rn = 1 
		) o
		with ur

  "
	exec_sql $sql_buff

	
	set tabname "G_A_02028_DAY"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}


	return 0
}

