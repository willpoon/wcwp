
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
	set sql_buff "delete from bass1.G_S_22089_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	




  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
  #����ҵ���˶���CANCEL_BUSI_CNT�����ܲ��Ǻ�׼���淶ָ����ÿ�β�ѯ�󣬷������˶���ҵ����?��
  #CANCEL_CNT�ھ�ҲҪ����ȷ�ϡ�
	set sql_buff "
	insert into bass1.G_S_22089_DAY
		  (
			 TIME_ID
			,OP_TIME
			,TUIFEI_CNT
			,TUIFEI
		  )
		select 
			$timestamp 	time_id		
			,'$timestamp' 	OP_TIME
			,'0' TUIFEI_CNT	
			,'0' TUIFEI  
	from sysibm.sysdummy1 a
	with ur
  "
	exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22089_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
