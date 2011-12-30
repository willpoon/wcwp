#################################################################
# ��������: INT_CHECK_L1K9_TO_DAY.tcl
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
#�����ţ�L1,K9
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��L1����Ե����ҵ����ռ��
#          K9����������ҵ����ռ��
#����������L1��70% �� ��Ե���żƷ���/���żƷ��� �� 90%
#          K9��8% �� �������żƷ���/���żƷ��� �� 28%
#У�����
#          BASS1.G_S_04014_DAY       ���Ż������Ż���
#          BASS1.G_S_04005_DAY       �������Ż���
#          BASS1.G_S_21008_TO_DAY    ��ͨ����ҵ����ʹ��(��ʹ���м��)
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
	#���ϸ��� ��ʽ yyyymm
        set last_month [GetLastMonth [string range $p_timestamp 0 5]]
        #puts $last_month
        #----���������һ��---#,��ʽ yyyymmdd
        set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
        puts $last_day_month
        
        #���µĵ�һ��
        set last_month_one [string range $last_day_month 0 3][string range $last_day_month 4 5]01
        puts $last_month_one
        
         ##--�����죬��ʽyyyymmdd--##
        set yesterday [GetLastDay ${p_timestamp}]
        #puts $yesterday
        
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #��������
        set app_name "INT_CHECK_L1K9_TO_DAY.tcl"
###
###
####ɾ����������
###        set handle [aidb_open $conn]
###	set sql_buff "
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
###    	              AND RULE_CODE IN ('L1','K9') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###
#############################L1����Ե����ҵ����ռ��################
###       if {$today_dd>=32} {
####��Ե���żƷ���
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                    SELECT 
###                 	SUM(BIGINT(SMS_COUNTS))
###                    FROM  bass1.G_S_21008_TO_DAY 
###                    WHERE TIME_ID/100=$this_month
###                       AND END_STATUS ='0'
###                       AND CDR_TYPE_ID IN ('00','10')
###                       AND SVC_TYPE_ID IN ('11','12','13')"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
###       
####���żƷ���
####K9ָ�꽫Ҫ�õ���ָ��
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                     SELECT 
###	               SUM(T.CNT)
###                     FROM (
###	                   SELECT SUM(BIGINT(SMS_COUNTS)) AS CNT FROM bass1.G_S_21008_TO_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND END_STATUS = '0' AND CDR_TYPE_ID IN ('00','10') AND SVC_TYPE_ID IN ('11','12','13')
###                           UNION ALL
###		                   SELECT COUNT(*) AS CNT FROM bass1.G_S_04005_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND SMS_STATUS = '0' AND CALLTYPE_ID IN ('00','01','10','11')
###                           UNION ALL
###                           SELECT COUNT(*) AS CNT FROM bass1.G_S_04014_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND SMS_SEND_STATE = '0' AND SMS_BILL_TYPE IN ('00','01','10','11')
###                      ) T"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL2 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
###       
###       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
###
####��������
###       set CHECK_VAL1 0.00
###       
###    #��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###			($p_timestamp ,
###			'L1',
###			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###			cast ($RESULT as  DECIMAL(18, 5) ),
###			0)"
###	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###		WriteTrace $errmsg 003
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#�ж�L1��70% �� ��Ե���żƷ���/���żƷ��� �� 90%����
###	if { $RESULT<0.7 || $RESULT>0.9 } {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��L1�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
###		} elseif {$RESULT<=0.72 || $RESULT>=0.88 } {
###		set grade 3
###	        set alarmcontent "׼ȷ��ָ��L1�ӽ����ſ��˷�Χ0.7~0.9,�ﵽ ${RESULT}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###		        	
###        } 
#####################################K9��8% �� �������żƷ���/���żƷ��� �� 28%
#### �������żƷ���
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                  SELECT 
###                    COUNT(*)
###                  FROM  BASS1.G_S_04005_DAY 
###                  WHERE TIME_ID/100=$this_month
###                       AND SMS_STATUS = '0'
###                       AND  CALLTYPE_ID IN ('00','01','10','11')"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
###       
####���żƷ���
####ֱ��ȡ��L1��CHECK_VAL2ֵ
###       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
###       
###       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
###
####��������
###       set CHECK_VAL1 0.00
###       
###    #��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###			($p_timestamp ,
###			'K9',
###			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###			cast ($RESULT as  DECIMAL(18, 5) ),
###			0)"
###	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###		WriteTrace $errmsg 003
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#�ж�K9��8% �� �������żƷ���/���żƷ��� �� 28%����
###	if { $RESULT<0.08 || $RESULT>0.28 } {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��K9�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        
###        } elseif {$RESULT<=0.09 || $RESULT>=0.27 } {
###        	set grade 3
###	        set alarmcontent "׼ȷ��ָ��K9�ӽ����ſ��˷�Χ0.08~0.28,�ﵽ ${RESULT}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        
###		       	
###        } 
###}
##################################END#######################
	return 0
}    