
######################################################################################################		
#�ӿ�����: ������ֵҵ�����˷Ѻ��֤�ջ���                                                               
#�ӿڱ��룺22089                                                                                          
#�ӿ�˵�����ɼ�������ֵҵ�����˷ѣ����֤�������ձ����ݣ��������˷ѱ������˷ѽ��ȡ�
#��������: G_S_22089_DAY.tcl                                                                            
#��������: ����22089������
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
      set op_month [string range $timestamp 0 5]

      set curr_month [string range $op_time 0 3][string range $op_time 5 6]


  #ɾ����������
	set sql_buff "delete from bass1.G_S_22087_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	




  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
  #����ҵ���˶���CANCEL_BUSI_CNT�����ܲ��Ǻ�׼���淶ָ����ÿ�β�ѯ�󣬷������˶���ҵ����?��
  #CANCEL_CNT�ھ�ҲҪ����ȷ�ϡ�
	set sql_buff "
	insert into bass1.G_S_22087_DAY
		  (
			 TIME_ID
			,OP_TIME
			,SMS_QUERY_CNT
			,SMS_REPLY_CNT
			,ORDER_FAIL_CNT
			,ORDER_CNT
		  )
 select      $timestamp TIME_ID
             ,'$timestamp' op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,'${RESULT_VAL}'                cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char( case when (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) < 0 
			then 0 else (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) 
		   end 
		  ) CANCEL_BUSI_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_$op_month a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
              					,count(0) CANCEL_BUSI_CNT
                       from   
                       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month a
                        where replace(char(date(a.create_date)),'-','') =  '$timestamp'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '$timestamp' 
and    replace(char(date(a.create_date)),'-','') = b.op_time
with ur
  "
	exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22087_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
