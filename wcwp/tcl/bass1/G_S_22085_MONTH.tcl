######################################################################################################
#�ӿڵ�Ԫ���ƣ��շ��������˷Ѻ��֤�»��� 
#�ӿڵ�Ԫ���룺22085
#�ӿڵ�Ԫ˵�����ɼ������շ����顰���˷ѣ����֤�������±����ݣ������˷ѵı������˷ѵ�SP���ơ�SP��ҵ���롢�˷��ܶ����SPʵ���˿�ȡ�
#��������: G_S_22085_MONTH.tcl
#��������: ����22085������
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

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month


  #ɾ����������
	set sql_buff "delete from bass1.G_S_22085_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "
	insert into bass1.G_S_22085_MONTH
		  (
			 TIME_ID
			,OP_TIME
			,BACK_SP_NAME
			,BACK_SP_CODE
			,BACK_CNT
			,BACK_FEE
			,SP_ACT_INCOME
		  )
	select 
			 $op_month TIME_ID
			,'$op_month' OP_TIME
			,'����׿����Ѷ��Ϣ�������޹�˾' BACK_SP_NAME
			,'701167' BACK_SP_CODE
			,'0' BACK_CNT
			,'0' BACK_FEE
			,'100' SP_ACT_INCOME
	from sysibm.sysdummy1 a
  "

	exec_sql $sql_buff
	
  #1.���chkpkunique
	set tabname "G_S_22085_MONTH"
	set pk 			"OP_TIME||BACK_SP_NAME||BACK_SP_CODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
		
	return 0
}
