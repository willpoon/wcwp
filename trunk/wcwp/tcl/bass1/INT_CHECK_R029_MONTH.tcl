#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R029_MONTH.tcl
# ** �����ܣ�һ��У�����R029��
# ** ����ʾ����int INT_CHECK_R029_MONTH.tcl
# ** ����ʱ�䣺2009-5-30 15:03:29
# ** �� �� �ˣ�xufr
# ** �����ţ�R029
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��Ƿ�Ѽ�¼���е��ʻ���ʶ
# ** ����������Ƿ�Ѽ�¼���е��ʻ���ʶ��Ӧ�������˻�����
# ** У�����03007 Ƿ�Ѽ�¼
# **           03001 �ʻ�
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ:
# **           �޸�����       �޸���        �޸�����
# **           -------------------------------------------------------------------------------------------------------
# **
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#***********************************************************************************************************************************************
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymm
	set op_month [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]
	#���� yyyy-mm
	set opmonth $optime_month
	#��ȡ�����·�1��yyyy-mm-01
	set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	#��ȡ�����·�����1��yyyy-mm-01
	set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
	#��ȡ�����·ݱ���ĩ��yyyy-mm-31
	set op_month_last_iso [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]
	#��ȡ�����·ݱ���ĩ��yyyymm31
	set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
	#��������
	set app_name "INT_CHECK_R029_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R029'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�Ƿ�Ѽ�¼���е��ʻ���ʶ���������˻���ļ�¼��
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) from 
	(select distinct acct_id from bass1.G_I_03007_MONTH where time_id = $op_month
	except
	select acct_id from bass1.g_i_03001_month where time_id = $op_month group by acct_id) a"
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10010
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#��У��ֵ����У������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R029'
		,$RESULT_VAL1
		,0
		,0
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�R029�����ڼ���ͨ��У��
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R029�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
