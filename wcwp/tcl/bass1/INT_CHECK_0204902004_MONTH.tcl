######################################################################################################
#�������ƣ�INT_CHECK_0204902004_MONTH.tcl
#У��ӿڣ�
#��������: ��
#У�����: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-06-25
#�����¼��
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #��Ȼ�µ�һ�� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #������
        set app_name "INT_CHECK_0204902004_MONTH.tcl"

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #�������һ��,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #�������һ�� yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day



	#--R007:���ų�Ա�û��е��û���ʶ��Ӧ�����û�����

  set handle [aidb_open $conn]
##	set sql_buff "select count(distinct user_id)
##                from BASS1.G_I_02049_MONTH 
##                where time_id=$op_month
##                and user_id not in (select distinct user_id from bass1.G_A_02004_DAY)
##                with ur"
	set sql_buff " select count(distinct a.user_id)
								 from BASS1.G_I_02049_MONTH a
								 left join (select distinct user_id from bass1.G_A_02004_DAY where time_id<=$last_month_day) b  on a.user_id = b.user_id
								 where a.time_id=$op_month
								       and b.user_id is null"
  puts $sql_buff

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R00709',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1�����ų�Ա�û��е��û���ʶ��Ӧ�����û�����


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R00709У�鲻ͨ��:���ų�Ա�û��е��û���ʶ�в����û����е��û�"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




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