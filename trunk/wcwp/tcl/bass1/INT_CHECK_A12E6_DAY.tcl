#################################################################
# ��������: INT_CHECK_A12E6_DAY.tcl
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
#�����ţ�A1,A2,E6
#�������ԣ�ָ���춯
#�������ͣ���
#ָ��ժҪ��A1����������ͨ�ŷ�
#          A2��������������ͨ�ŷ�
#          E6���˾��������żƷ���
#����������A1��������������ͨ�ŷѡ�0.15Ԫ
#          A2�����������������е�ͨ�ŷѣ�0Ԫ
#          E6��10 �� �����������żƷ���/������������ʹ���û��� �� 40����/����
#У�����
#          BASS1.G_S_04005_DAY     �������Ż���
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   
        #���� yyyy-mm-dd
        set optime $op_time
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]        
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $timestamp 6 7]
        #��������
        set app_name "INT_CHECK_A12E6_DAY.tcl"
        puts $app_name

##        #ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "\
##		delete from bass1.g_rule_check where time_id=$timestamp
##    	              and rule_code in ('A1','A2','E6') "
##    	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
#####################################A1����������ͨ�ŷ�##########################
##	set handle [aidb_open $conn]
##	set sqlbuf "
##                   SELECT 
##            	     COUNT(*)
##                   FROM 
##                     bass1.G_S_04005_DAY
##                   WHERE 
##                     TIME_ID=$timestamp 
##                     AND INT(SMS_BASEFEE)>15"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
###
##     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]
##
###��������
##     set CHECK_VAL2 "0.00"
##
##    #��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($timestamp ,
##			'A1',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			0,
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#�ж�A1��������������ͨ�ŷѡ�0.15Ԫ����
##	if { $RESULT_VAL1>0 } {	
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��A1�������ſ��˷�Χ"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##            }
#####################################A2��������������ͨ�ŷ�##########################
##	set handle [aidb_open $conn]
##	set sqlbuf "
##            	SELECT 
##            		COUNT(*)
##            	FROM bass1.G_S_04005_DAY
##            	WHERE TIME_ID=$timestamp 
##                  AND CALLTYPE_ID IN ('01','11')
##                  AND INT(SMS_BASEFEE)<>0"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]
##
###��������
##     set CHECK_VAL1 "0.00"
##
##    #��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($timestamp ,
##			'A2',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			0,
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##	#�ж�A2:���������������е�ͨ�ŷ�=0Ԫ����
##	if { $RESULT_VAL1>0 } {	
##		set grade 2
##	        set alarmcontent "׼ȷ��A2�������ſ��˷�Χ"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##            }
###################E6��10 �� �����������żƷ���/������������ʹ���û��� �� 40����/����##########################  
##	#1-24��ֱ�ӷ��أ�25�ſ�ʼ��04005��ȡ�ĵ�������ͳ���ж�
##	if { $today_dd>=32 } {
##	     set handle [aidb_open $conn]
##	     set sqlbuf "
##   	              SELECT 
##   	         	COUNT(*),
##   	                COUNT(DISTINCT PRODUCT_NO) 
##   	              FROM 
##   	                bass1.G_S_04005_DAY
##   	              WHERE 
##   	                TIME_ID/100=$op_month 
##   	                AND CALLTYPE_ID IN ('00','01','10','11')
##   	                AND SMS_STATUS='0'"
##
##	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	      }
##	      while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##		set CHECK_VAL2 [lindex $p_row 1]
##		puts $CHECK_VAL1
##		puts $CHECK_VAL2
##	      }
##	      aidb_commit $conn
##	      aidb_close $handle
##	      
##	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.000/${CHECK_VAL2})]]
##
##             #��У��ֵ����У������
##             set handle [aidb_open $conn]
##             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##             		($timestamp ,
##             		'E6',
##             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##             		0,
##             		0,
##             		0)"
##             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##             	WriteTrace $errmsg 003
##             	return -1
##             }
##             aidb_commit $conn
##             aidb_close $handle
##             
##             #�ж�E6:10<=�����������żƷ���/������������ʹ���û���<=40����/��������
##	     if { $RESULT_VAL1<10 || $RESULT_VAL1>40 } {
##	        set grade 2
##	        set alarmcontent "׼ȷ��E6�������ſ��˷�Χ"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
##             }
##       }
##
  
##################################END#######################

      return 0
}
