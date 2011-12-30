#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_02010_MONTH.tcl
# ** �����ܣ�һ��У�����R013��
# ** ����ʾ����int -s INT_CHECK_02010_MONTH.tcl
# ** ����ʱ�䣺2009-7-2
# ** �� �� �ˣ�liuzhilong
# ** �����ţ� R012 R014 R013
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��ʵ����������
# ** ����������R013 �û����ϱ��е������û���Ӧ�����û�ѡ���ʷ�Ӫ�������д���
# ** 					 R012 �û�ѡ���ʷ�Ӫ�������е��û���ʶӦ�����û�����
# ** 					 R014 �û�ѡ���ʷ�Ӫ�������е��ʷ�Ӫ������ʶ��Ӧ�����ʷ�Ӫ�������д���
# ** У�����02010 �û�ѡ���ʷ�Ӫ����
# **					 02001 �ʷ�Ӫ����
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ: #  20100120 �޸������û��ھ�usertype_id NOT IN ('2010','2020','2030','9000') ���ų����ݿ� sim_code<>'1'
# **           �޸�����       �޸���        �޸�����
# **           -------------------------------------------------------------------------------------------------------
# **
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
	set app_name "INT_CHECK_02010_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code in ('R012','R014', 'R013' )"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	# R013�û����ϱ��е������û���Ӧ�����û�ѡ���ʷ�Ӫ�������д���
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select user_id 
												,usertype_id
												,row_number() over(partition by user_id order by time_id desc ) row_id
									from bass1.g_a_02008_day
									where time_id <=$op_month_last ) t
					 inner join (select user_id 
															,sim_code
															,usertype_id
														,row_number() over(partition by user_id order by time_id desc ) row_id
												from bass1.g_a_02004_day
												where time_id <=$op_month_last ) f on t.user_id=f.user_id
						where t.row_id=1 and f.row_id=1 
								and t.usertype_id NOT IN ('2010','2020','2030','9000')
								and f.usertype_id<>'3'
						    and t.user_id not in ( select distinct user_id from bass1.G_I_02010_MONTH where time_id=$op_month  ) "
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
		,'R013'
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

	#�ж�R013 �û����ϱ��е������û��в�����ѡ���ʷ�Ӫ��������
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R013У�鲻ͨ��,02004�û����ϱ��е������û�Ӧ�ô���02010���ʷ�Ӫ��������  "
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	

	# R012 �û�ѡ���ʷ�Ӫ�������е��û���ʶӦ�����û�����
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select distinct user_id from bass1.G_I_02010_MONTH where time_id=$op_month) t
						where  user_id not in ( select distinct user_id
					                            from bass1.g_a_02004_day
												              where time_id <=$op_month_last  ) "
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
		,'R012'
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

	#�ж�R012�������ڵĸ�������0 �澯
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R012У�鲻ͨ��,02010�û�ѡ���ʷ�Ӫ�������е��û���ʶӦ�����û���02004��"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}	
	
	# R014 �û�ѡ���ʷ�Ӫ�������е��ʷ�Ӫ������ʶ��Ӧ�����ʷ�Ӫ�������д���
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select distinct plan_id from bass1.G_I_02010_MONTH  where time_id=$op_month) t
						where  plan_id not in ( select  plan_id
					                            from bass1.G_I_02001_MONTH
												              where time_id=$op_month  ) "
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
		,'R014'
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

	#�ж�R014�������ڵĸ�������0 �澯
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R014У�鲻ͨ��,�û�ѡ���ʷ�Ӫ�������е��ʷ�Ӫ������ʶ��Ӧ�����ʷ�Ӫ�������д��� "
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}	
		
	return 0
}
