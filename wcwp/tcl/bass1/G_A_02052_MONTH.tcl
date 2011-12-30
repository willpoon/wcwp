######################################################################################################
#�ӿ����ƣ��û��������
#�ӿڱ��룺02050
#�ӿ�˵�����û������������Ϣ��
#��������: G_A_02052_MONTH.tcl
#��������: ����02050������
#��������: ��
#Դ    ��bass2.STAT_ZD_VILLAGE_USERS_YYYYMM
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��  
#�� д �ˣ��ź���
#��дʱ�䣺2009-05-06
#�����¼��
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
	set sql_buff "DELETE FROM bass1.G_A_02052_MONTH where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff

  
  
	set sql_buff "insert into bass1.G_A_02052_MONTH 
	              select $op_month,user_id,char(LOCNTYPE_ID)
	                from bass2.STAT_ZD_VILLAGE_USERS_$op_month
                      where month_new_mark = 1
				       with ur "
	
	puts $sql_buff
  exec_sql $sql_buff


	return 0
}


#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------


