#################################################################
#��������: INT_CHECK_X0123_MONTH.tcl
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
#�����ţ�X0,X1,X2,X3
#�������ԣ�ƽ���ϵ
#�������ͣ���
#ָ��ժҪ��X0: ʡ�ڼƷ�����������
#          X1: �ƷѶ�����
#          X2: ������㻰����
#          X3: �����໰����
#����������X0: ʡ�ڼƷ����������� = ȫ��ͨ������+ �����л�����+���еش�������
#          X1: �ƷѶ����� =  ��Ե�MO + �ƶ�������Ϣ�ѻ��� + �ƶ�����ͨ�ŷѻ���
#          X2: ������㻰���� = ���й����� + ���й���ͨ + ���й���ͨ + ���й���ͨ + ����
#          X3: �����໰���� = ʡ�ʱ߽����λ��� +  17950���� + ���Ż��� + ��ת���� + WAP���� + ����
#У�����
#          BASS1.G_S_22021_MONTH   ҵ���������ۺ�
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month      
        #��������
        set app_name "INT_CHECK_X0123_MONTH.tcl"

#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code in ('X0','X1','X2','X3') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

###############################X0: ʡ�ڼƷ�����������
        #ʡ�ڼƷ�����������
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030001'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#ȫ��ͨ������+ �����л�����+���еش�������
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN('00030002','00030003','00030004')"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'X0',
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
        
	#�ж�X0��ʡ�ڼƷ����������� = ȫ��ͨ������+ �����л�����+���еش�����������
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��X0�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
        
###############################X1: �ƷѶ�����
        #�ƷѶ�����
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030006'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#��Ե�MO + �ƶ�������Ϣ�ѻ��� + �ƶ�����ͨ�ŷѻ���
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030007','00030008','00030009')"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'X1',
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
        
        
	#�ж�X1���ƷѶ����� =  ��Ե�MO + �ƶ�������Ϣ�ѻ��� + �ƶ�����ͨ�ŷѻ�������
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��X1�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

###############################X2: ������㻰����
        #������㻰����
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030012'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#���й����� + ���й���ͨ + ���й���ͨ + ���й���ͨ + ����
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030013','00030014','00030015','00030016','00030017')"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'X1',
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
        
        
	#�ж�X2��������㻰���� = ���й����� + ���й���ͨ + ���й���ͨ + ���й���ͨ + ��������
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��X2�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

###############################X3: �����໰����
        #�����໰����
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030018'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#ʡ�ʱ߽����λ��� +  17950���� + ���Ż��� + ��ת���� + WAP���� + ����
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030019','00030020','00030021','00030022','00030023','00030024')"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'X1',
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
        
        
	#�ж�X3�������໰���� = ʡ�ʱ߽����λ��� +  17950���� + ���Ż��� + ��ת���� + WAP���� + ��������
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��X3�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##################################END#######################
	return 0
}        