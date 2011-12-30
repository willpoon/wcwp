#################################################################
#��������: INT_CHECK_A369B2C2_DAY.tcl
#��������:
#�������:
#	    op_time		����ʱ�䣨���Σ�,��yyyy-mm-dd��
#	    province_id	        ʡ�ݱ���
#	    redo_number	        �ش���š�ֻ�����յ�һ�����붨�������ļ���Ҫ�ش����
#	    trace_fd	        trace�ļ����
#	    temp_data_dir	��ʱ�����ļ��Ĵ��λ�ã�$BASS1FILEDIR/tempdata/�����ڱ���e_transform���ɵ������ļ�
#	    semi_data_dir	�м������ļ��Ĵ��λ�ã�$BASS1FILEDIR/semidata/�����ڱ���.bass1��.bass2��.sample���ļ�
#	    final_data_dir	���������ļ��Ĵ��λ�ã�$BASS1FILEDIR/finaldata/�����ڱ���.dat����һ�����붨�������ļ�
#	    conn		���ݿ�����
#	    src_data	        ����Դ
#	    obj_data	        ����Ŀ��
#
#�����ţ�A3,A6,A9,B2,C2
#�������ԣ�������֤
#�������ͣ���
#ָ��ժҪ��A3��GSM��ʱ�μƷ�ʱ��/GSM�ջ��ܼƷ�ʱ��
#          A6��GSM��ʱ��ͨ������/GSM�ջ���ͨ������
#          A9��GSM��ʱ��ͨ��ʱ��/GSM�ջ���ͨ��ʱ��
#          B2: GSM��ʱ���ܷ���/GSM�ջ����ܷ���
#          C2: ȫ��ͨ��ʱ�μƷ�ʱ��/ȫ��ͨ�ջ��ܼƷ�ʱ��
#����������A3��|GSM��ʱ�μƷ�ʱ��/GSM�ջ��ܼƷ�ʱ�� - 1| �� 0.1%
#          A6��|GSM��ʱ��ͨ������/GSM�ջ���ͨ������ - 1| �� 0.1%
#          A9��|GSM��ʱ��ͨ��ʱ��/GSM�ջ���ͨ��ʱ�� - 1| �� 0.1%
#          B2: |GSM��ʱ���ܷ���/GSM�ջ����ܷ��� - 1| �� 0.1%
#          C2: |ȫ��ͨ��ʱ�μƷ�ʱ��/ȫ��ͨ�ջ��ܼƷ�ʱ�� - 1| �� 0.1%
#У�����
#          BASS1.G_S_21001_DAY     GSM��ͨ����ҵ����ʹ��
#          BASS1.G_S_21002_DAY     GSM��ͨ����ҵ����ʱ��ʹ��
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #��������
        set app_name "INT_CHECK_A369B2C2_DAY.tcl"


        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$timestamp
    	              and rule_code in ('A3','A6','A9','B2','C2') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
###################################A3��|GSM��ʱ�μƷ�ʱ��/GSM�ջ��ܼƷ�ʱ�� - 1| �� 0.1%##########################	
	#GSM��ʱ�μƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM�ջ��ܼƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21002_DAY
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
	
#��������
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A3',
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

	#�ж�A3��|GSM��ʱ�μƷ�ʱ��/GSM�ջ��ܼƷ�ʱ�� - 1| �� 0.1%����
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��A3�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
###################################A6��|GSM��ʱ��ͨ������/GSM�ջ���ͨ������ - 1| �� 0.1%##########################        
	#GSM��ʱ��ͨ������
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM�ջ���ͨ������
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21002_DAY
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
	
#��������
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A6',
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

	#�ж�A6��|GSM��ʱ��ͨ������/GSM�ջ���ͨ������ - 1| �� 0.1%����
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��A6�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
##################################A9��|GSM��ʱ��ͨ��ʱ��/GSM�ջ���ͨ��ʱ�� - 1| �� 0.1%###############
	#GSM��ʱ��ͨ��ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM�ջ���ͨ��ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21002_DAY
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
	
#��������
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A9',
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

	#�ж�A9��|GSM��ʱ��ͨ��ʱ��/GSM�ջ���ͨ��ʱ�� - 1| �� 0.1%����
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��A9�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
##################################B2: |GSM��ʱ���ܷ���/GSM�ջ����ܷ��� - 1| �� 0.1%###########
	#GSM��ʱ���ܷ���
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM�ջ����ܷ���
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21002_DAY
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
	
#��������
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B2',
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

	#�ж�B2��|GSM��ʱ���ܷ���/GSM�ջ����ܷ��� - 1| �� 0.1%����
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��B2�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }        
##################################C2: |ȫ��ͨ��ʱ�μƷ�ʱ��/ȫ��ͨ�ջ��ܼƷ�ʱ�� - 1| �� 0.1%##########
	#ȫ��ͨ��ʱ�μƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21001_DAY
                where time_id=$timestamp AND BRAND_ID='1'"

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
     
        #ȫ��ͨ�ջ��ܼƷ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21002_DAY
                where time_id=$timestamp AND BRAND_ID='1'"

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
			'C2',
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

	#�ж�C2��|ȫ��ͨ��ʱ�μƷ�ʱ��/ȫ��ͨ�ջ��ܼƷ�ʱ�� - 1| �� 0.1%����
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "׼ȷ��C2�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }        
##################################END#######################
	return 0
}