#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R032_MONTH.tcl
# ** �����ܣ�һ��У�����R032��
# ** ����ʾ����int INT_CHECK_R032_MONTH.tcl
# ** ����ʱ�䣺2009-5-31 15:46:22
# ** �� �� �ˣ�xufr
# ** �����ţ�R032
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ���ۺ��˵��е��û����û�ҵ�����ͱ���
# ** �����������ۺ��˵��е��û����û����С��û�ҵ�����ͱ��롱����Ϊ1�����������Ѳ������������û������ֻ�ж��û����е�ҵ�����ͱ����Ƿ��в�Ϊ01�ģ�
# ** У�����03005 �ۺ��ʵ�
# **           02004 �û�
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
	set app_name "INT_CHECK_R032_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R032'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж��û����е�ҵ�����ͱ����Ƿ��в�Ϊ01�ļ�¼��
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) from
	(select user_id,user_bus_typ_id,row_number() over(partition by user_id order by time_id desc) row_id from bass1.G_A_02004_DAY where time_id <= ${op_month_last}) a
	,(select distinct user_id from bass1.G_S_03005_month where time_id = ${op_month}) b
	where a.user_id = b.user_id
	and a.row_id = 1
	and a.user_bus_typ_id <> '01'"
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
		,'R032'
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

	#�ж�R032��������ͨ��У��
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R032�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
