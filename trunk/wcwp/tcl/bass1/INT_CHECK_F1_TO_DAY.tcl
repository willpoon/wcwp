#################################################################
# ��������: INT_CHECK_F1_TO_DAY.tcl
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
#�����ţ�F1
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ���˾�WAP��Ϣ��
#����������F1: 10�ܱ���WAP����Ϣ�ѣ��ƴ���Ϣ��+������Ϣ�ѣ�/�Ʒ��û���������Ϣ�ѣ�0����25��Ԫ/����
#У�����
#          1.BASS1.G_S_04006_DAY     ����WAP����
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#################################################################
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
        #puts $last_day_month
         ##--�����죬��ʽyyyymmdd--##
        set yesterday [GetLastDay ${p_timestamp}]
        #puts $yesterday
        
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $p_timestamp 6 7]
        #puts $today_dd

        #��������
        set app_name "INT_CHECK_F1_TO_DAY.tcl"

        #�ж�����
##        if { $today_dd <= 32 } {
##        	    	puts "���� $today_dd �ţ�δ��20�ţ��ݲ�����"
##        	    	return 0
##        	}
##        	       
##        #ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "
##		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
##    	              AND RULE_CODE='F1' "
##    	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        ###����WAP����Ϣ�ѣ��ƴ���Ϣ��+������Ϣ�ѣ�--�Ʒ��û���
##	set handle [aidb_open $conn]
##	set sql_buff "
##               SELECT 
##               	SUM(BIGINT(INFO_FEE)+BIGINT(MONTH_FEE))/100.0,
##               	COUNT(DISTINCT PRODUCT_NO)
##               FROM  bass1.G_S_04006_DAY 
##               WHERE TIME_ID/100=$this_month
##               	     AND BIGINT(INFO_FEE)+BIGINT(MONTH_FEE)>0 
##                     AND CDR_STATUS='00'"
##         puts $sql_buff
##	if [catch {aidb_sql $handle $sql_buff } errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##		set CHECK_VAL2 [lindex $p_row 1]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        puts $CHECK_VAL1
##        puts $CHECK_VAL2
##        
##       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
##       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
##       
##       puts $RESULT_VAL1
##       puts $RESULT_VAL2
##       
##       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
##       
##       puts $RESULT
##    #��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($p_timestamp ,
##			'F1',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
##			cast ($RESULT as  DECIMAL(18, 5) ),
##			0)"
##        puts $sqlbuf	
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#�ж�F1��10�ܱ���WAP����Ϣ�ѣ��ƴ���Ϣ��+������Ϣ�ѣ�/�Ʒ��û���������Ϣ�ѣ�0����25��Ԫ/��������
##	if { $RESULT<10.00 || $RESULT>25.00 } {
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��F1�������ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
##	        
##        } elseif {$RESULT<=11.00 || $RESULT>=24.00 } {
##        	set grade 3
##	        set alarmcontent "׼ȷ��ָ��F1�ӽ����ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	               	
##        } 
                             
	return 0
}    