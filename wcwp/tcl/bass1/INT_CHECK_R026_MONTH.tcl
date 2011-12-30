#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R026_MONTH.tcl
# ** �����ܣ�һ��У�����R026��
# ** ����ʾ����int INT_CHECK_R026_MONTH.tcl
# ** ����ʱ�䣺2009-5-30 10:52:53
# ** �� �� �ˣ�xufr
# ** �����ţ�R026
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��ʹ���ն�ͨ���û�Ϊ����ʷ�����û�
# ** �����������û�ʹ���ն�ͨ��������е��û���Ӧ�����û����ϱ��е���ʷ�����û�
# ** У�����02047 �û�ʹ���ն�ͨ�����
# **           02004 �û�
# **           02008 �û�״̬
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ: 20100124�����ھ��޸�Ϊ usertype_id in ('2010','2020','2030','9000') 
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
	#��������
	set app_name "INT_CHECK_R026_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R026'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�ж��û�ʹ���ն�ͨ��������е��û������û����ϱ��е���ʷ�����û�
	set handle [ aidb_open $conn ]
##	set sqlbuf "
##	select count(*) from bass1.G_S_02047_MONTH a
##	where time_id = $op_month
##	and int(call_cnt) > 0
##	and exists
##	(select 1 from (
##	select
##	    a.user_id
##	from
##	    bass1.G_A_02008_DAY a
##	where
##	    time_id <= ${ls_month_last} and
##	    usertype_id in ('2010','2020','2030','9000') and
##	    exists (select 1 from (select max(time_id) as time_id,user_id as user_id from bass1.G_A_02008_DAY group by user_id) b where a.time_id = b.time_id and a.user_id = b.user_id and b.time_id <= ${ls_month_last})) b
##	    where a.user_id = b.user_id)"
	set sqlbuf "
		select count(*) 
		from bass1.G_S_02047_MONTH a
		inner join (select user_id ,usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id 
			  	   	  from bass1.G_A_02008_DAY  
					      where time_id <=${ls_month_last}) b on a.user_id=b.user_id
		where a.time_id = $op_month	 
			    and  b.usertype_id in ('2010','2020','2030','9000') 
		      and  b.row_id=1
		with ur"
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
		,'R026'
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

	#�ж�R026�����ڼ�¼��ͨ��
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R026�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
