#################################################################
# ��������: INT_CHECK_G56_MONTH.tcl
# ��������:
# �������:
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
#�����ţ�G5,G6
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��G5: ���ſͻ�ͳһ���������±䶯��
#          G6: ������Ϣ�������±䶯��
#����������G5: |���¼��ſͻ�ͳһ��������/���¼��ſͻ�ͳһ�������� - 1| �� 15%
#          G6: |���¼�����Ϣ������/���¼�����Ϣ������ - 1| �� 15%
#У�����
#          BASS1.G_S_03013_MONTH   ���ſͻ�ͳ������
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #��������
        set app_name "INT_CHECK_G56_MONTH.tcl"

#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('G5','G6')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   

##############################G5: ���ſͻ�ͳһ���������±䶯��#########
        #���¼��ſͻ�ͳһ��������
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 SUM(BIGINT(INCOME))/100.0
               FROM  bass1.G_S_03013_MONTH
               WHERE TIME_ID=$op_month"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#���¼��ſͻ�ͳһ��������
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 SUM(BIGINT(INCOME))/100.0
               FROM  bass1.G_S_03013_MONTH
               WHERE TIME_ID=$last_month"
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
			'G5',
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
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
	#�ж�G5��|���¼��ſͻ�ͳһ��������/���¼��ſͻ�ͳһ�������� - 1| �� 15%����
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G5�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##############################G6: ������Ϣ�������±䶯��#########
        #���¼�����Ϣ������
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(INCOME))/100.0
             FROM  bass1.G_S_03013_MONTH
             WHERE TIME_ID=$op_month
             	AND (
                       (ENT_PROD_ID IN ('1001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3)) OR 
                       (ENT_PROD_ID IN ('1008') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1009') AND BIGINT(ACCOUNT_ITEM) IN (2)) OR 
                       (ENT_PROD_ID IN ('2002') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2003') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2004') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2005') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3,4)) OR 
                       (ENT_PROD_ID IN ('1005') AND BIGINT(ACCOUNT_ITEM) IN (2,6,7)) OR 
                       (ENT_PROD_ID IN ('2006') AND BIGINT(ACCOUNT_ITEM) IN (6)) OR 
                       (ENT_PROD_ID IN ('1004') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1006') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2012') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2008') AND BIGINT(ACCOUNT_ITEM) IN (3,5)) OR 
                       (ENT_PROD_ID IN ('2009') AND BIGINT(ACCOUNT_ITEM) IN (3,4)) OR 
                       (ENT_PROD_ID IN ('2010') AND BIGINT(ACCOUNT_ITEM) IN (3,6,7)) OR 
                       (ENT_PROD_ID IN ('2011') AND BIGINT(ACCOUNT_ITEM) IN (1,2)) OR 
                       (ENT_PROD_ID IN ('2014') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2015') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2013') AND BIGINT(ACCOUNT_ITEM) IN (6,7)) OR 
                       (ENT_PROD_ID IN ('3000') AND BIGINT(ACCOUNT_ITEM) IN (9))
                    )"
               
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#���¼�����Ϣ������
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(INCOME))/100.0
             FROM  bass1.G_S_03013_MONTH
             WHERE TIME_ID=$last_month
             	AND (
                       (ENT_PROD_ID IN ('1001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3)) OR 
                       (ENT_PROD_ID IN ('1008') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1009') AND BIGINT(ACCOUNT_ITEM) IN (2)) OR 
                       (ENT_PROD_ID IN ('2002') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2003') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2004') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2005') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3,4)) OR 
                       (ENT_PROD_ID IN ('1005') AND BIGINT(ACCOUNT_ITEM) IN (2,6,7)) OR 
                       (ENT_PROD_ID IN ('2006') AND BIGINT(ACCOUNT_ITEM) IN (6)) OR 
                       (ENT_PROD_ID IN ('1004') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1006') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2012') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2008') AND BIGINT(ACCOUNT_ITEM) IN (3,5)) OR 
                       (ENT_PROD_ID IN ('2009') AND BIGINT(ACCOUNT_ITEM) IN (3,4)) OR 
                       (ENT_PROD_ID IN ('2010') AND BIGINT(ACCOUNT_ITEM) IN (3,6,7)) OR 
                       (ENT_PROD_ID IN ('2011') AND BIGINT(ACCOUNT_ITEM) IN (1,2)) OR 
                       (ENT_PROD_ID IN ('2014') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2015') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2013') AND BIGINT(ACCOUNT_ITEM) IN (6,7)) OR 
                       (ENT_PROD_ID IN ('3000') AND BIGINT(ACCOUNT_ITEM) IN (9))
                    )"

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
	               (
			$op_month,
			'G6',
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
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
        
	#�ж�G6: |���¼�����Ϣ������/���¼�����Ϣ������ - 1| �� 15%����
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G6�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##################################END#######################
	return 0
}