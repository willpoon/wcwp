
######################################################################################################		
#�ӿ�����: ��ֵҵ��1111���������ջ���                                                               
#�ӿڱ��룺22087                                                                                          
#�ӿ�˵�����ɼ���ֵҵ��1111��ݶ���������ձ����ݣ��������ͻ����Ų�ѯ�������Żظ���������ʧ�������ͻ�����ҵ����������;�ɼ�����������ʡ�����ʧ���ʡ��ȣ�������=�ͻ�����ҵ������/�ͻ����Ų�ѯ��������ʧ����=����ʧ����/���Żظ�������
#��������: G_S_22087_DAY.tcl                                                                            
#��������: ����22087������
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
      ##~   set op_time  2012-05-02
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
	 select        $timestamp TIME_ID
				 ,'$timestamp' OP_TIME
				 ,char((	select count(0) from  bass2.dw_kf_sms_cmd_receive_dm_$curr_month 
					where cmd_id =600997 and OP_TIME = '$op_time'
				  ))  SMS_QUERY_CNT
				 ,char(count(distinct a.PRODUCT_NO||a.SP_CODE ))  SMS_REPLY_CNT
				 ,char(count(distinct case when  a.STS = 0  then  a.PRODUCT_NO||a.SP_CODE  end ))    ORDER_FAIL_CNT
				 ,char(count(distinct case when  a.STS = 1  then  a.PRODUCT_NO||a.SP_CODE  end ))    ORDER_CNT
		from   bass2.dw_product_unite_order_dm_$curr_month a
		where OP_TIME = '$op_time' and date(CREATE_DATE) =  '$op_time'
	with ur
  "
	exec_sql $sql_buff

##~   ��������=�ͻ�����ҵ������/�ͻ����Ų�ѯ��������ʧ����=����ʧ����/���Żظ�������

set sql_buff "
select sum(val) from (
	select case when  bigint(SMS_QUERY_CNT)  < bigint(ORDER_CNT) then 1 else 0 end val from bass1.G_S_22087_DAY where time_id = $timestamp
) t
"
chkzero2 $sql_buff "���ݲ����߼�"


set sql_buff "
select sum(val) from (
	select case when  bigint(SMS_REPLY_CNT)  < bigint(ORDER_FAIL_CNT) then 1 else 0 end val from bass1.G_S_22087_DAY where time_id = $timestamp
) t
"
chkzero2 $sql_buff "���ݲ����߼�"


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22087_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
