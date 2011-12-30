#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R047_MONTH.tcl
# ** �����ܣ�һ��У�����R047��
# ** ����ʾ����int INT_CHECK_R047_MONTH.tcl
# ** ����ʱ�䣺2009-5-31 9:45:46
# ** �� �� �ˣ�xufr
# ** �����ţ�R047
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ����Ӫ�����ķ�ҵ���������ͷ�Ʒ�ư��������ƽ���ϵ
# ** �����������ӿڹ淶��Ҫ���ָ��֮��Ӧ�������¹�ϵ�������ͻ������ɷѱ�������ֵҵ���������������ն���������������ҵ����������ȫ��ͨ�û�ҵ�����������������û�ҵ�������������еش��û�ҵ����������
# ** У�����22046 ��Ӫ����ҵ�������
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ:
# **           �޸�����       �޸���        �޸�����
# **           -------------------------------------------------------------------------------------------------------
# **           20100101 liuqf nullֵ��0
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
	set app_name "INT_CHECK_R047_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R047'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�����ͻ������ɷѱ�������ֵҵ���������������ն���������������ҵ��������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(int(NEWCUSTCOUNT) + int(PAYCOUNT) + int(BUSICOUNT) + int(COMINALCOUNT) + int(OTHERCOUNT)),0) from bass1.G_S_22046_MONTH where time_id = $op_month"
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
	
	#ȫ��ͨ�û�ҵ�����������������û�ҵ�������������еش��û�ҵ��������
	set handle [ aidb_open $conn ]
	set sqlbuf "select value(sum(int(QQTOPERATORCOUNT) + int(SZXOPERATORCOUNT) + int(DGDDOPERATORCOUNT)),0) from bass1.G_S_22046_MONTH where time_id = $op_month"
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
		,'R047'
		,$RESULT_VAL1
		,$RESULT_VAL2
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

	#�ж�R047����ָ��Ӧ���
	if { $RESULT_VAL1 != $RESULT_VAL2 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R047�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
