######################################################################################################
#�������ƣ�INT_CHECK_22204_MONTH.tcl
#У��ӿڣ�G_S_22204_MONTH.tcl
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-06-22
#�����¼��
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $last_month
        
        #��Ȼ�µ�һ�� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        puts $timestamp
        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        puts $l_timestamp
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #������
        set app_name "INT_CHECK_22204_MONTH.tcl"

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #�������һ��#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #�������һ�� yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month 
	                    and rule_code in ('R117','R118','R119','R121','R122','R123')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

########################################################################################################3
	#--R117:TD�ֻ��ͻ���=188�Ŷε�TD�ֻ��ͻ�+157�Ŷε�TD�ֻ��ͻ�+�����ںϵ�TD�ֻ��ͻ�

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)
                from BASS1.G_S_22204_MONTH
                where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R117',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��TD�ֻ��ͻ���=188�Ŷε�TD�ֻ��ͻ�+157�Ŷε�TD�ֻ��ͻ�+�����ںϵ�TD�ֻ��ͻ�


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R117 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R118:TD���ݿ��ͻ���=157�Ŷε�TD���ݿ��ͻ�+147�Ŷε�TD���ݿ��ͻ�+�����ںϵ����ݿ��ͻ�

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_DATACARD_CNT)-bigint(TD_157_DATACARD_CNT)-bigint(TD_147_DATACARD_CNT)-bigint(TD_GSM_DATACARD_CNT)
                from BASS1.G_S_22204_MONTH
                where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R118',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��TD���ݿ��ͻ���=157�Ŷε�TD���ݿ��ͻ�+147�Ŷε�TD���ݿ��ͻ�+�����ںϵ����ݿ��ͻ�


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R118 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


########################################################################################################3
	#--R119:TD�ֻ��ͻ���=ȫ��ͨTD�ֻ��ͻ���+������TD�ֻ��ͻ���+���еش�TD�ֻ��ͻ���

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_QQT_CNT)-bigint(TD_SZX_CNT)-bigint(TD_DGDD_CNT)
                from BASS1.G_S_22204_MONTH
                where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R119',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��TD�ֻ��ͻ���=ȫ��ͨTD�ֻ��ͻ���+������TD�ֻ��ͻ���+���еش�TD�ֻ��ͻ���


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R119 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R121:ʹ�ù�TD�����TD�ͻ��� �� TD�ͻ�����

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_TNET_USE_CNT)-bigint(TD_CUSTOMER_CNT)
               from BASS1.G_S_22204_MONTH
               where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R121',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��ʹ�ù�TD�����TD�ͻ��� �� TD�ͻ�����


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R121 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




########################################################################################################3
	#--R122:ʹ�ù�TD������ֻ��ͻ��� �� TD�ֻ��ͻ���

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TNET_USE_CNT)-bigint(TD_MOBILE_CUSTOMER_CNT)
                from BASS1.G_S_22204_MONTH
                where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R122',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��ʹ�ù�TD������ֻ��ͻ��� �� TD�ֻ��ͻ���


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R122 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R123:ʹ�ù�TD��������ݿ��ͻ��� �� TD���ݿ��ͻ���

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TNET_USE_DATACARD_CNT)-bigint(TD_DATACARD_CNT)
                from BASS1.G_S_22204_MONTH
                where time_id=$op_month with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R123',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��ʹ�ù�TD��������ݿ��ͻ��� �� TD���ݿ��ͻ���


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R123 У�鲻ͨ��"
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