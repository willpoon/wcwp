#################################################################
# ��������: INT_CHECK_G1234_MONTH.tcl
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
#ָ��ժҪ��G1: ʹ�ü��Ŷ��ŵļ��ſͻ��±䶯��
#          G2: ʹ���ƶ��ܻ��ļ��ſͻ��±䶯��
#          G3: ʹ�ü��Ų���ļ��ſͻ��±䶯��
#          G4: ʵ����Ϣ����������ļ��ſͻ��±䶯��
#����������G1: |����ʹ�ü��Ŷ��ŵļ��ſͻ���/����ʹ�ü��Ŷ��ŵļ��ſͻ��� - 1| �� 15%���ҿͻ���������
#          G2: |����ʹ���ƶ��ܻ��ļ��ſͻ���/����ʹ���ƶ��ܻ��ļ��ſͻ��� - 1| �� 20%	
#          G3: |����ʹ�ü��Ų���ļ��ſͻ���/����ʹ�ü��Ų���ļ��ſͻ��� - 1| �� 20%���ҿͻ���������
#          G4: |����ʵ����Ϣ����������ļ��ſͻ���/����ʵ����Ϣ����������ļ��ſͻ��� - 1| �� 15%���ҿͻ���������
#У�����
#          BASS1.G_S_22035_MONTH   ���ſͻ���׼����Ʒʹ�����
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
#�޸ļ�¼
#��	if { $RESULT_VAL1<=0 || $RESULT<-0.20 ||$RESULT>0.20  } ��Ϊ
#   if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  }
#
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #��������
        set app_name "INT_CHECK_G1234_MONTH.tcl"

#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('G1','G2','G3','G4')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

#############################G1: ʹ�ü��Ŷ��ŵļ��ſͻ��±䶯��#########
        #����ʹ�ü��Ŷ��ŵļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='2001'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#����ʹ�ü��Ŷ��ŵļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='2001'"

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
			'G1',
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
        
	#�ж�G1��|����ʹ�ü��Ŷ��ŵļ��ſͻ���/����ʹ�ü��Ŷ��ŵļ��ſͻ��� - 1| �� 15%,�ҿͻ��������㳬��
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G1�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G2: ʹ���ƶ��ܻ��ļ��ſͻ��±䶯��#########
        #����ʹ���ƶ��ܻ��ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='1005'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#����ʹ���ƶ��ܻ��ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='1005'"

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
			'G2',
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
        
#       set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
        
	#�ж�G2��|����ʹ�ü��Ŷ��ŵļ��ſͻ���/����ʹ�ü��Ŷ��ŵļ��ſͻ��� - 1| �� 15%,�ҿͻ��������㳬��
	#��Ϊ��ҵ��Ŀǰû�У����пͻ����жϴ���0����澯
	if { $RESULT_VAL1>0 } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G2�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G3: ʹ�ü��Ų���ļ��ſͻ��±䶯��#########
        #����ʹ�ü��Ų���ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='2006'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#����ʹ�ü��Ų���ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='2006'"

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
			'G3',
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
        
	#�ж�G3��|����ʹ�ü��Ų���ļ��ſͻ���/����ʹ�ü��Ų���ļ��ſͻ��� - 1| �� 20%,�ҿͻ��������㳬��
	if { $RESULT_VAL1<=0 || $RESULT<-0.20 ||$RESULT>0.20  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G3�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G4: ʵ����Ϣ����������ļ��ſͻ��±䶯��#########
        #����ʵ����Ϣ����������ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	COUNT(DISTINCT T.ENTERPRISE_ID)
             FROM  
             (
             	SELECT ENTERPRISE_ID ,COUNT(DISTINCT ENT_PROD_ID)
             	FROM BASS1.G_S_22035_MONTH
             	WHERE TIME_ID=$op_month
             	      AND ENT_PROD_ID IN ('2001','2006','2008')
             	GROUP BY ENTERPRISE_ID
             	HAVING COUNT(DISTINCT ENT_PROD_ID)>=2
             ) T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#����ʵ����Ϣ����������ļ��ſͻ���
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	COUNT(DISTINCT T.ENTERPRISE_ID)
             FROM  
             (
             	SELECT ENTERPRISE_ID ,COUNT(DISTINCT ENT_PROD_ID)
             	FROM BASS1.G_S_22035_MONTH
             	WHERE TIME_ID=$last_month
             	      AND ENT_PROD_ID IN ('2001','2006','2008')
             	GROUP BY ENTERPRISE_ID
             	HAVING COUNT(DISTINCT ENT_PROD_ID)>=2
             ) T"

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
			'G4',
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
        
	#�ж�G4��|����ʵ����Ϣ����������ļ��ſͻ���/����ʵ����Ϣ����������ļ��ſͻ��� - 1| �� 15%,�ҿͻ��������㳬��
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��G4�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
 
 ##################################END#######################
	return 0
}