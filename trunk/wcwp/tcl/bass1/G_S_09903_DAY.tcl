######################################################################################################
#�ӿ����ƣ�����100ҵ�������
#�ӿڱ��룺09903
#�ӿ�˵����ÿ���ϱ�����100ҵ�������
#��������: G_S_09903_DAY.tcl
#��������: ����09903������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-03-12
#�����¼��
#�޸���ʷ: 
#######################################################################################################


#proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#
#	#���� yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#        #���� yyyy-mm-dd
#        set optime $op_time
#        
#        #ɾ����������
#	set sql_buff "delete from bass1.g_s_09903_day where time_id=$timestamp"
#	puts $sql_buff
#  exec_sql $sql_buff
#       
#
#  #���� �����ͻ���  
#	set sql_buff "insert into BASS1.G_S_09903_DAY values
#                 ($timestamp,                    
#                 '101',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0',                   
#                 '0');"
#                        
#  puts $sql_buff
#  exec_sql $sql_buff
#  
#
#	return 0
#}
#
#
##�ڲ���������	
#proc exec_sql {MySQL} {
#
#	global env
#
#	global conn
#
#	global handle
#
#	set handle [aidb_open $conn]
#	set sql_buff $MySQL
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		puts $errmsg
#		exit -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	return 0
#}
##--------------------------------------------------------------------------------------------------------------
#
#proc get_single {MySQL} {
#
#	global env
#
#	global conn
#
#	global handle
#
#	set handle [aidb_open $conn]
#	set sql_buff $MySQL
#  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		puts $errmsg
#		exit -1
#	}
#	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		puts $errmsg
#		exit -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	return $result
#}
##--------------------------------------------------------------------------------------------------------------
#
#
#
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	return 0
}