######################################################################################################
#�ӿ����ƣ�WAP�����±�
#�ӿڱ��룺22105
#�ӿ�˵������¼WAP���ص��·������
#��������: G_S_22105_MONTH.tcl
#��������: ����22105������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-04-22
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month
              

        #���µ�һ�� yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22105_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� �����ͻ���  
	set sql_buff "insert into bass1.g_s_22105_month
                values( $op_month,'$op_month'
                        ,'1' ,'0' ,'0' ,'0' 
                       )
               "        
                     
                        
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



