#################################################################
#��������: INT_CHECK_F7_MONTH.tcl
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
#�����ţ�F7
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��F7: Ƿ�Ѽ�¼
#����������F7: Ƿ�Ѽ�¼�ӿڵ��ļ������ڣ�Ƿ������
#У�����
#          BASS1.G_I_03007_MONTH   Ƿ�Ѽ�¼
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #��������
        set app_name "INT_CHECK_F7_MONTH.tcl"

#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code='F7' "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        
        
        #Ƿ�Ѽ�¼�ӿڵ��ļ������ڴ���Ƿ�����ڵļ�¼��
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  COUNT(*)
                FROM BASS1.G_I_03007_MONTH
                WHERE TIME_ID=$op_month
                    AND TIME_ID<=BIGINT(BILL_CYC_ID)"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'F7',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			0,
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�F7��Ƿ�Ѽ�¼�ӿڵ��ļ������ڣ�Ƿ�����ڳ���
	if { $RESULT_VAL1>0 } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��F7�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
##################################END#######################
	return 0
}