######################################################################################################
#�ӿ����ƣ�ѧ���ͻ��г������ջ���
#�ӿڱ��룺22403
#�ӿ�˵����
#��������: G_S_22403_DAY.tcl
#��������:  
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-7-26
#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
#�޸���ʷ:
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time

##  #ɾ����������
##  set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.G_S_22403_DAY where time_id=$timestamp"
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}      
##	aidb_commit $conn
##	aidb_close $handle
##
##       
##
##  set handle [aidb_open $conn]
##	set sql_buff " insert into BASS1.G_S_22403_DAY"
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}      
##	aidb_commit $conn
##	aidb_close $handle

	return 0
}

