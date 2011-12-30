#################################################################
#��������: INT_CHECK_A58B14_DAY.tcl
#��������:
#�����ţ�A5,A8,B1,B4
#�������ԣ�������֤
#�������ͣ���
#ָ��ժҪ��A5��������VPMN��ʱ�μƷ�ʱ��/������VPMNҵ��ʹ�üƷ�ʱ��
#          A8��������VPMN��ʱ��ͨ������/������VPMNҵ��ʹ��ͨ������
#          B1��������VPMN��ʱ��ͨ��ʱ��/������VPMNҵ��ʹ��ͨ��ʱ��
#          B4: ������VPMN��ʱ���ܷ���/������VPMNҵ��ʹ���ܷ���
#����������A5��|������VPMN��ʱ�μƷ�ʱ��/������VPMNҵ��ʹ�üƷ�ʱ�� - 1| �� 0.1%
#          A8��|������VPMN��ʱ��ͨ������/������VPMNҵ��ʹ��ͨ������ - 1| �� 0.1%
#          B1��|������VPMN��ʱ��ͨ��ʱ��/������VPMNҵ��ʹ��ͨ��ʱ�� - 1| �� 0.1%
#          B4: |������VPMN��ʱ���ܷ���/������VPMNҵ��ʹ���ܷ��� - 1| �� 0.1%
#У�����
#          BASS1.G_S_21009_DAY     ������VPMNҵ����ʹ��
#          BASS1.G_S_21016_DAY     ������VPMNҵ����ʱ��ʹ��
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #��������
        set app_name "INT_CHECK_A58B14_DAY.tcl"


#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "
		delete from bass1.g_rule_check where time_id=$timestamp
    	              and rule_code in ('A5','A8','B1','B4') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
##################################A5��������VPMN��ʱ�μƷ�ʱ��/������VPMNҵ��ʹ�üƷ�ʱ��#######
	#������VPMN��ʱ�μƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #������VPMNҵ��ʹ�üƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A5',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�A5��|������VPMN��ʱ�μƷ�ʱ��/������VPMNҵ��ʹ�üƷ�ʱ�� - 1| �� 0.1%����
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {
		set grade 2
	        set alarmcontent "׼ȷ��A5�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
            puts "A5����"       
##################################A8��������VPMN��ʱ��ͨ������/������VPMNҵ��ʹ��ͨ������#######
	#������VPMN��ʱ��ͨ������
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #������VPMNҵ��ʹ��ͨ������
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A8',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�A8��|������VPMN��ʱ��ͨ������/������VPMNҵ��ʹ��ͨ������ - 1| �� 0.1%����
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��A8�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
		aidb_close $handle
            } 
            puts "A8����"             
##################################B1��������VPMN��ʱ��ͨ��ʱ��/������VPMNҵ��ʹ��ͨ��ʱ��#######
	#������VPMN��ʱ��ͨ��ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #������VPMNҵ��ʹ��ͨ��ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B1',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�B1��|������VPMN��ʱ��ͨ��ʱ��/������VPMNҵ��ʹ��ͨ��ʱ�� - 1| �� 0.1%����
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��B1�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
          puts "B1����"           
##################################B4: ������VPMN��ʱ���ܷ���/������VPMNҵ��ʹ���ܷ���#######
	#������VPMN��ʱ���ܷ���
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #������VPMNҵ��ʹ���ܷ���
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B4',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж� B4: |������VPMN��ʱ���ܷ���/������VPMNҵ��ʹ���ܷ��� - 1| �� 0.1%����
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��B4�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
            puts "B4����"                     
##################################END#######################
	return 0
}