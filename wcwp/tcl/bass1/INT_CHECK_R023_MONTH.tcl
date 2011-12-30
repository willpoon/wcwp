#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R023_MONTH.tcl
# ** �����ܣ�һ��У�����R023��
# ** ����ʾ����int INT_CHECK_R023_MONTH.tcl
# ** ����ʱ�䣺2009-5-31 15:33:20
# ** �� �� �ˣ�xufr
# ** �����ţ�R023
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ�������û�����/�ջ��ܹ�ϵ
# ** ����������|(�������û�/��(�������û�����-1) x 100% | �� 3%
# ** У�����02004 �û�
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
	#���� yyyymm01
	set op_month_01 [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]01
	#���� yyyy-mm
	set opmonth $optime_month
	#��ȡ�����·�1��yyyy-mm-01
	set op_month_01_iso [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	#��ȡ�����·�����1��yyyy-mm-01
	set nt_month_01_iso [ clock format [ clock scan "${op_month_01_iso} + 1 months" ] -format "%Y-%m-01" ]
	#��ȡ�����·ݱ���ĩ��yyyy-mm-31
	set op_month_last_iso [ clock format [ clock scan "${nt_month_01_iso} - 1 days" ] -format "%Y-%m-%d" ]
	#��ȡ�����·ݱ���ĩ��yyyymm31
	set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
	#��������
	set app_name "INT_CHECK_R023_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R023'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�������û���
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) 
	from bass1.G_A_02004_DAY
	where create_date >= '$op_month_01' 
	and create_date <= '$op_month_last' 
	and time_id / 100 = $op_month"
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
	
	#�������û�
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) 
	from bass1.G_A_02004_DAY
	where substr(create_date,1,6) = '$op_month' 
	and time_id / 100 = $op_month"
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#��У��ֵ����У������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R023'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * $RESULT_VAL2 / $RESULT_VAL1 - 1
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�R023���ж��Ƿ�С�ڵ���3%
	if { [ expr 1.000 * $RESULT_VAL2 / $RESULT_VAL1 - 1 ] > 0.3 } {
		set grade 2
		set alarmcontent "R023У�鲻ͨ��,��/�������û��������������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
