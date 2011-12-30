#################################################################
# ��������: INT_CHECK_F8_MONTH.tcl
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
#�����ţ�F8
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��F8: �ƶ���������ʹ���û���ռ��
#����������F8: 95% �� �ƶ���������ʹ���û���/�û������� �� 115%
#У�����
#          BASS1.G_I_06001_MONTH   �ƶ��绰����
#          BASS1.G_A_02004_DAY     �û�
#          BASS1.G_A_02008_DAY     �û�״̬
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
# liuzhilong 2009-8-3 ���׳���
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##        #���� yyyymm
##        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
##        #���� yyyy-mm
##        set opmonth $optime_month
##        #�������һ�� yyyymmdd
##        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
##        #��������
##        set app_name "INT_CHECK_F8_MONTH.tcl"
##
###ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "
##		delete from bass1.g_rule_check where time_id=$op_month
##    	              and rule_code='F8' "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle        
##	
##	#�ƶ���������ʹ���û���
##	set handle [aidb_open $conn]
##	set sqlbuf "
##               SELECT 
##             	 COUNT(DISTINCT PRODUCT_NO)
##               FROM  BASS1.G_I_06001_MONTH
##               WHERE TIME_ID=$op_month
##                  AND STATE_ID='03'"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        #�û�������
##        set RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
##        puts $RESULT_VAL2
##
##        #��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($op_month ,
##			'F8',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##        
##        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
##        
##	#�ж�F8��95% �� �ƶ���������ʹ���û���/�û������� �� 115%����
##	if { $RESULT_VAL1==0 || $RESULT<0.95 ||$RESULT>1.15  } {	
##	     	set grade 2
##	        set alarmcontent "׼ȷ��ָ��F8�������ſ��˷�Χ"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
####################################END#######################
	return 0
}