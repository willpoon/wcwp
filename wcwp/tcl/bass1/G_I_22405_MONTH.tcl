
######################################################################################################		
#�ӿ�����: У԰�мĿ�                                                               
#�ӿڱ��룺22405                                                                                          
#�ӿ�˵����"��ָͨ���ʵݵķ�ʽ���ſ���Ϊ׼ȷ���ʵݵ�ѧ���ͻ����С�ע���ޱ����Ͷ�ź��롣"
#��������: G_I_22405_MONTH.tcl                                                                            
#��������: ����22405������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110807
#�����¼��
#�޸���ʷ: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]



  #������
  global app_name
  set app_name "G_I_22405_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_22405_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_22405_MONTH

  "
 
# exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_22405_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}
