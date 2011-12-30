#################################################################
# ��������: INT_CHECK_R142_MONTH.tcl
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
#�����ţ�R142,95
#�������ԣ�������֤
#�������ͣ���(��У��)
#ָ��ժҪ��R142���˾���Ե���żƷ���
#����������R142��|�����굥�˾���Ե���żƷ���/���������˾���Ե���żƷ��� - 1| �� 5%���ҼƷ�����0
#У�����
#          BASS1.G_S_04011_DAY     ������ͨ���Ż���
#          BASS1.G_S_04012_DAY     �������������Ż���
#          BASS1.G_S_21008_MONTH   ��ͨ����ҵ����ʹ��
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
        set db_user $env(DB_USER)
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set app_name "INT_CHECK_R142_MONTH.tcl"
        
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$op_month
        AND RULE_CODE IN ('R142','95')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
 ###########################R142���˾���Ե���żƷ���#######################
	#R142���˾���Ե���żƷ���
        #��������
	     set handle [aidb_open $conn]
	     set sqlbuf "
                  SELECT 
                   	SUM(T.TXL) AS TXL,
                   	COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,COUNT(*) AS TXL  FROM BASS1.G_S_04011_DAY
                         WHERE TIME_ID/100=$op_month
                         	   AND CDR_TYPE IN ('00','10','21')
                         	   AND SVC_TYPE IN ('11','12','13')
                         	   AND SMS_STATUS='0'
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,COUNT(*)AS TXL  FROM BASS1.G_S_04012_DAY
                         WHERE TIME_ID/100=$op_month
                               AND COMM_TYPE='0'
                         GROUP BY PRODUCT_NO
                         )T"

	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
		puts $CHECK_VAL1
		puts $CHECK_VAL2
	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	     
             #��������
	     set handle [aidb_open $conn]
	     set sqlbuf "
                     SELECT 
                     	SUM(BIGINT(SMS_COUNTS)) AS TXL,
                     	COUNT(DISTINCT PRODUCT_NO) AS RS
                     FROM BASS1.G_S_21008_MONTH
                     WHERE TIME_ID=$op_month
                           AND END_STATUS='0'
                           AND CDR_TYPE_ID IN ('00','10','21') 
                           AND SVC_TYPE_ID IN ('11','12','13')"

	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
		puts $CHECK_VAL1
		puts $CHECK_VAL2
	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

#(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		(${op_month} ,
             		'R142',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�R142��|�����굥�˾���Ե���żƷ���/���������˾���Ե���żƷ��� - 1| �� 5%���ҼƷ�����0����
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ�� �˾���Ե���żƷ���R142�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ�� �˾���Ե���żƷ���R142�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    }
	    
    
	      	       
	return 0
}