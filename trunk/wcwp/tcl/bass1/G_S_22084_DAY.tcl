######################################################################################################
#�ӿڵ�Ԫ���ƣ��շ��������˷Ѻ��֤�ջ��� 
#�ӿڵ�Ԫ���룺22084
#�ӿڵ�Ԫ˵�����ɼ������շ����顰���˷ѣ����֤�������ձ����ݣ��������˷ѱ������˷ѽ��ȡ�
#��������: G_S_22084_DAY.tcl
#��������: ����22084������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzhiwei
#��дʱ�䣺2011-04-26
#�����¼��1.
#�޸���ʷ: 1. 1.7.2 �淶
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp



  #ɾ����������
	set sql_buff "delete from bass1.G_S_22084_DAY where time_id=$timestamp"
	exec_sql $sql_buff


#  #��Դ�ڶ����ɷѱ�����
#	set sql_buff "
#	insert into bass1.G_S_22084_DAY
#		  (
#         TIME_ID
#        ,OP_TIME
#        ,BACK_CNT
#        ,BACK_FEE
#		  )
#		select 
#					$timestamp 	time_id
#					,replace(char(date(a.OP_TIME)),'-','')  OP_TIME
#				--,char(count(0)) back_cnt	--Ϊ�����±���һ�£���0
#				--,char(bigint(sum(amount))) back_fee --Ϊ�����±���һ�£���0
#				,'0' back_cnt
#				,'0' back_fee
#		from bass2.DW_ACCT_PAYMENT_DM_$op_month a
#		where a.remarks like '%SP�˷�%'
#				and replace(char(date(a.OP_TIME)),'-','') = '$timestamp'
#		group by replace(char(date(a.OP_TIME)),'-','')
#  "

# 	puts $sql_buff
#	exec_sql $sql_buff


  #��Դ�ڶ����ɷѱ����� --Ϊ�����½ӿ�22085����һ�£�back_cnt,back_fee��0
	set sql_buff "
	insert into bass1.G_S_22084_DAY
		  (
			 TIME_ID
			,OP_TIME
			,BACK_CNT
			,BACK_FEE
		  )
		select 
			$timestamp 	time_id		
			,'$timestamp' 	OP_TIME
			,'0' back_cnt	
			,'0' back_fee  
	from sysibm.sysdummy1 a
  "
	exec_sql $sql_buff
	
	return 0
}
