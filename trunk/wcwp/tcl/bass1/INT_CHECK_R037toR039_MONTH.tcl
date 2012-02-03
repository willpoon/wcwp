#**********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R037toR039_MONTH.tcl
# ** �����ܣ�һ��У�����R037toR039��
# ** ����ʾ����int INT_CHECK_R037toR039_MONTH.tcl
# ** ����ʱ�䣺2009-5-29 9:24:17
# ** �� �� �ˣ�xufr
# ** �����ţ�R037toR039
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��R037 ȫ��ͨҵ�������±䶯��
# **           R038 ������ҵ������䶯��
# **           R039 ���еش�ҵ�������±䶯��
# ** ����������R037 | (����ȫ��ͨ��ƽ��ҵ������ / ����ȫ��ͨ��ƽ��ҵ������ - 1) x 100% | �� 10%
# **           R038 | (������������ƽ��ҵ������ / ������������ƽ��ҵ������ - 1) x 100% | �� 10%
# **           R039 | (���¶��еش���ƽ��ҵ������ / ���¶��еش���ƽ��ҵ������ - 1) x 100% | �� 15%
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
	#��ȡ�����·�����ĩ��yyyymm31
	set ls_month_last [ clock format [ clock scan "${op_month_01} - 1 days" ] -format "%Y%m%d" ]
	
	puts $ls_month_last

	#��ȡ�����·�����yyyymm
	set ls_month [ string range $ls_month_last 0 5 ]
	
	set last_days [ string range $op_month_last_iso 8 9 ]
	
	puts $last_days
	
	set last_last_days [ string range $ls_month_last 6 7 ]
	
	puts $last_last_days
	
	
	#��������
	set app_name "INT_CHECK_R037toR039_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code in ('R037','R038','R039')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#������ʱ��1
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.INT_CHECK_R037toR039_MONTH_TMP_1
	(
		user_id              varchar(20)
		,brand_id            char(1)
	)
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#������ʱ��2
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.INT_CHECK_R037toR039_MONTH_TMP_2
	(
		brand_id             char(1)
		,should_fee          decimal(16,2)
	)
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#������ʱ��3
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.INT_CHECK_R037toR039_MONTH_TMP_3
	(
		brand_id             char(1)
		,should_fee          decimal(16,2)
	)
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#������Ʒ�����ݲ�����ʱ��1
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.INT_CHECK_R037toR039_MONTH_TMP_1
	(
		user_id
		,brand_id
	)
	select
		a.user_id
		,b.brand_id
	from
		(select user_id,max(time_id) time_id from G_A_02004_DAY where time_id <= $ls_month_last group by user_id) a
	left join
		(select time_id,user_id,brand_id from G_A_02004_DAY where time_id <= $ls_month_last) b
	on a.time_id = b.time_id and a.user_id = b.user_id"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�����·��ò�����ʱ��2
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.INT_CHECK_R037toR039_MONTH_TMP_2
	(
		brand_id
		,should_fee
	)
	select 
		a.brand_id
		,sum(b.should_fee) 
	from 
		session.INT_CHECK_R037toR039_MONTH_TMP_1 a
		,(select user_id,sum(bigint(should_fee)) should_fee from g_s_03005_month where item_id in ('0100','0200','0300','0400','0500','0600','0700','0900')  and time_id = $ls_month group by user_id) b
	where 
		a.user_id = b.user_id
	group by a.brand_id"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�����ʱ��1
	set handle [ aidb_open $conn ]
	set sqlbuf "
	delete from session.INT_CHECK_R037toR039_MONTH_TMP_1"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#������Ʒ�����ݲ�����ʱ��1
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.INT_CHECK_R037toR039_MONTH_TMP_1
	(
		user_id
		,brand_id
	)
	select
		a.user_id
		,b.brand_id
	from
		(select user_id,max(time_id) time_id from G_A_02004_DAY where time_id <= $op_month_last group by user_id) a
	left join
		(select time_id,user_id,brand_id from G_A_02004_DAY where time_id <= $op_month_last) b
	on a.time_id = b.time_id and a.user_id = b.user_id"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#�����·��ò�����ʱ��3
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.INT_CHECK_R037toR039_MONTH_TMP_3
	(
		brand_id
		,should_fee
	)
	select 
		a.brand_id
		,sum(b.should_fee) 
	from 
		session.INT_CHECK_R037toR039_MONTH_TMP_1 a
		,(select user_id,sum(bigint(should_fee)) should_fee from g_s_03005_month where item_id in ('0100','0200','0300','0400','0500','0600','0700','0900')  and time_id = $op_month group by user_id) b
	where 
		a.user_id = b.user_id
	group by a.brand_id"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#��У��ֵ����У������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	select
		$op_month
		,case
			when a.brand_id = '1' then 'R037'
			when a.brand_id = '2' then 'R038'
			when a.brand_id = '3' then 'R039'
		end
		,b.should_fee
		,a.should_fee
		,1.000 *( b.should_fee/bigint(${last_days})) / (a.should_fee/bigint(${last_last_days})) - 1
		,0
	from
		session.INT_CHECK_R037toR039_MONTH_TMP_2 a
		,session.INT_CHECK_R037toR039_MONTH_TMP_3 b
	where
		a.brand_id = b.brand_id
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#ͳ�Ƴ���ָ��R037
	set handle [ aidb_open $conn ]
	set sqlbuf "select count(*) from bass1.G_RULE_CHECK where time_id = $op_month and rule_code in ('R037') and abs(target3) > 0.1"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	#ͳ�Ƴ���ָ��R038
	set handle [ aidb_open $conn ]
	set sqlbuf "select count(*) from bass1.G_RULE_CHECK where time_id = $op_month and rule_code in ('R038') and abs(target3) > 0.1"
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
	#ͳ�Ƴ���ָ��R039
	set handle [ aidb_open $conn ]
	set sqlbuf "select count(*) from bass1.G_RULE_CHECK where time_id = $op_month and rule_code in ('R039') and abs(target3) > 0.15"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL3 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж�R037toR039�������
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R037�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
		puts "error"
	}
	if { $RESULT_VAL2 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R038�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
		puts "error"
	}
	if { $RESULT_VAL3 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R039�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
		puts "error"
	}
	return 0
}
