#################################################################
#��������: INT_CHECK_H01234_MONTH.tcl
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
#�����ţ�H0,H1,H2,H3,H4
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��H0: �����ʷ�Ӫ���������ͻ�Ʒ��
#          H1: �ʷ�Ӫ�������ʼ�¼
#          H2: �ʷ�Ӫ�����̶����ü�¼
#          H3: �ʷ�Ӫ�������Լ�¼
#          H4: �ʷ�Ӫ������Ʊ�����¼
#����������H0: �����ʷ�Ӫ���������ͻ�Ʒ�Ʊ�ʶ����Ϊ��
#          H1: ���ʷ�Ӫ�������ʡ��ӿ����ʷ�Ӫ����Ӧ�ڡ��ʷ�Ӫ�������ӿ���
#          H2: ���ʷ�Ӫ�����̶��ѡ��ӿ����ʷ�Ӫ����Ӧ�ڡ��ʷ�Ӫ�������ӿ���
#          H3: ���ʷ�Ӫ�������ԡ��ӿ����ʷ�Ӫ����Ӧ�ڡ��ʷ�Ӫ�������ӿ���
#          H4: ���ʷ�Ӫ������Ʊ������ӿ����ʷ�Ӫ����Ӧ�ڡ��ʷ�Ӫ�������ӿ���
#У�����
#          BASS1.G_I_02001_MONTH   �ʷ�Ӫ����
#          BASS1.G_I_02022_MONTH   �ʷ�Ӫ�����̶�����
#          BASS1.G_I_02021_MONTH   �ʷ�Ӫ��������
#          BASS1.G_I_02024_MONTH   �ʷ�Ӫ��������
#          BASS1.G_I_02030_MONTH   �ʷ�Ӫ������Ʊ���
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #��������
        set app_name "INT_CHECK_H01234_MONTH.tcl"

#ɾ����������
#####        set handle [aidb_open $conn]
#####	set sql_buff "\
#####		delete from bass1.g_rule_check where time_id=$op_month
#####    	              and rule_code  in ('H0','H1','H2','H3','H4')"
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle    
#####
##################################H0: �����ʷ�Ӫ���������ͻ�Ʒ��#############
#####        #�����ʷ�Ӫ���������ͻ�Ʒ�Ʊ�ʶ�ǿյļ�¼��
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02001_MONTH 
#####              WHERE TIME_ID=$op_month
#####                   AND BRAND_ID NOT IN ('1','2','3')"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####    #��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H0',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#�ж�H0�������ʷ�Ӫ���������ͻ�Ʒ�Ʊ�ʶ����Ϊ�ճ���
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "׼ȷ��ָ��H0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H1: �ʷ�Ӫ�������ʼ�¼#############
#####       #H1: �ʷ�Ӫ�������ʼ�¼
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02021_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H1',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#�ж�H1����Ϊ����02021�ӿڻ�û�����ݣ�����ֻҪ�жϸýӿ������ݣ����У���澯
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "׼ȷ��ָ��H1�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####        
##################################H2: �ʷ�Ӫ�����̶����ü�¼#############
#####       #�ʷ�Ӫ�����̶��ѽӿ��������ʷ�Ӫ�����ӿ��е��ʷ�Ӫ��������
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####            SELECT 
#####		COUNT(DISTINCT A.PLAN_ID ) 
#####	    FROM 
#####		BASS1.G_I_02022_MONTH A ,
#####		BASS1.G_I_02001_MONTH B
#####             WHERE A.TIME_ID=$op_month 
#####                  AND B.TIME_ID=$op_month
#####                  AND A.PLAN_ID=B.PLAN_ID
#####                  AND B.START_DATE<='$this_month_last_day'
#####                  AND B.END_DATE>'$this_month_last_day' "
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####      
#####      #�ʷ�Ӫ�����ӿ��е��ʷ�Ӫ��������
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(DISTINCT PLAN_ID)
#####              FROM  BASS1.G_I_02022_MONTH
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL2 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####        #��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H2',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####        
#####        
#####	#�ж�H2��"�ʷ�Ӫ�����̶���"�ӿ����ʷ�Ӫ����Ӧ��"�ʷ�Ӫ����"�ӿ��г���
#####	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
#####	     	set grade 2
#####	        set alarmcontent "׼ȷ��ָ��H2�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H3: �ʷ�Ӫ�������Լ�¼#############
#####       #H3: �ʷ�Ӫ�������Լ�¼
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02024_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H3',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#�ж�H3����Ϊ����02024�ӿڻ�û�����ݣ�����ֻҪ�жϸýӿ������ݣ����У���澯
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "׼ȷ��ָ��H3�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H4: �ʷ�Ӫ������Ʊ�����¼#############
#####       #H4: �ʷ�Ӫ������Ʊ�����¼
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02030_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H4',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#�ж�H4����Ϊ����0230�ӿڻ�û�����ݣ�����ֻҪ�жϸýӿ������ݣ����У���澯
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "׼ȷ��ָ��H4�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################END#######################
	return 0
}