#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R053_MONTH.tcl
# ** �����ܣ�һ��У�����R053��
# ** ����ʾ����int INT_CHECK_R053_MONTH.tcl
# ** ����ʱ�䣺2009-5-31 10:08:05
# ** �� �� �ˣ�xufr
# ** �����ţ�R053
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��ʵ����������
# ** ����������ʵ�������ӿ�����������Ӧ�õ�����Ӫ����������к�����������������������֮��
# ** У�����22044 ��Ӫ�������
# **           22045 ����������
# **           06030 ʵ������
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ:
# **           �޸�����       �޸���        �޸�����
# **           -------------------------------------------------------------------------------------------------------
# **
# **		2009-07-02	zhaolp2		06030�ӿ�����channel_status='1'
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
	set app_name "INT_CHECK_R053_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R053'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#ʵ�������ӿ�����������
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(*) from (
		select channel_id,channel_status,row_number() over(partition by channel_id order by time_id desc ) row_id 
		from  bass1.G_A_06030_DAY
		where time_id<=$op_month_last
		) a 
		where row_id=1 and  channel_status='1' "
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
	
	#��Ӫ�����������������֮��
	set handle [ aidb_open $conn ]
	set sqlbuf "select sum(int(COMPANYCOUNT)+int(ENTERPRISECOUNT)+int(SOCIETYCOUNT)) from bass1.G_S_22044_MONTH where time_id = $op_month"
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
	
	#��������������������֮��
	set handle [ aidb_open $conn ]
	set sqlbuf "select sum(int(COMPANYCOUNT)+int(ENTERPRISECOUNT)+int(SOCIETYCOUNT)+int(COMPANYCOUNT2)+int(ENTERPRISECOUNT2)+int(SOCIETYCOUNT2)+int(COMPANYCOUNT3)+int(ENTERPRISECOUNT3)+int(SOCIETYCOUNT3)+int(HAVEBOSSCOUNT)+int(HAVENOTBOSSCOUNT)+int(HAVEBOSSCOUNT2)+int(HAVENOTBOSSCOUNT2)+int(HAVEBOSSCOUNT3)+int(HAVENOTBOSSCOUNT3)+int(EXCLUSIVECOUNT)+int(UNEXCLUSIVECOUNT)+int(EXCLUSIVECOUNT2)+int(UNEXCLUSIVECOUNT2)+int(EXCLUSIVECOUNT3)+int(UNEXCLUSIVECOUNT3)) from bass1.G_S_22045_MONTH where time_id = $op_month"
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL3 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#��У��ֵ����У������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R053'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�R053��ʵ�������»��ܽӿ���ҵ�������� = ��Ӫ����ҵ��������ӿ�Ʒ��ҵ�������֮�� + �������ҵ������ӿ�Ʒ��ҵ�������֮��
	if { [ expr $RESULT_VAL2 + $RESULT_VAL3 ] != $RESULT_VAL1 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R053�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
