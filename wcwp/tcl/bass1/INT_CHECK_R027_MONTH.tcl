#***********************************************************************************************************************************************
# ** �������ƣ�INT_CHECK_R027_MONTH.tcl
# ** �����ܣ�һ��У�����R027��
# ** ����ʾ����int INT_CHECK_R027_MONTH.tcl
# ** ����ʱ�䣺2009-5-31 9:14:10
# ** �� �� �ˣ�xufr
# ** �����ţ�R027 
# ** �������ԣ�ҵ���߼�
# ** �������ͣ���
# ** ָ��ժҪ��R027 IMEI��ռ�����û�������
# ** ����������R027 85�� �� IMEI��ռ�����û������� �� 105��
# ** У�����02047 �û�ʹ���ն�ͨ�����
# **           02004 �û�
# **           02008 �û�״̬
# ** �������������ֵ:   0 �ɹ�; -1 ʧ��
# ** �޸���ʷ: 20100127 �޸������û��ھ� usertype_id not in ('2010','2020','2030','9000') ���ų����ݿ� sim_code<>'1'
# **           �޸�����       �޸���        �޸�����
# **           -------------------------------------------------------------------------------------------------------
# **
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
# **  20091202 ���ݼ����·�10�·ݵ�У��ֵ��������д��У���߼�
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
	#��ȡ�����·�����yyyymm
	set ls_month [ string range $op_month 0 5 ]
	#��������
	set app_name "INT_CHECK_R027_MONTH.tcl"

	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code in ('R027')"
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
	declare global temporary table session.product_bass1_tmp1
		(
	   user_id        CHARACTER(20)
	  ,product_no     CHARACTER(15)
	  ,test_flag      CHARACTER(1)
	  ,sim_code       CHARACTER(15) 
	  ,usertype_id    CHARACTER(4)
		)                            
		partitioning key           
		 (
		   user_id    
		 ) using hashing           
		with replace on commit preserve rows not logged in tbs_user_temp
	"
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
declare global temporary table session.product_bass1_tmp2
	(
   user_id        CHARACTER(20)
  ,product_no     CHARACTER(15)
  ,test_flag      CHARACTER(1)
  ,sim_code       CHARACTER(15) 
  ,usertype_id    CHARACTER(4)
	)                            
	partitioning key           
	 (
	   user_id    
	 ) using hashing           
	with replace on commit preserve rows not logged in tbs_user_temp 
	"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###������ʱ��1
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.product_bass1_tmp1 (
				 user_id    
				,product_no 
				,test_flag  
				,sim_code   
				,usertype_id  )
	select trim(e.user_id)
				,e.product_no  
				,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
				,e.sim_code
				,f.usertype_id           
	from (select user_id , create_date ,product_no,sim_code,usertype_id
				 			,row_number() over(partition by user_id order by time_id desc ) row_id   
	      from bass1.g_a_02004_day
	      where time_id<=$ls_month_last ) e 
	inner join ( select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
				       from bass1.g_a_02008_day
				       where time_id<=$ls_month_last ) f on f.user_id=e.user_id 
	where e.row_id=1 and f.row_id=1 
	"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###������ʱ��2
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.product_bass1_tmp2 (
				 user_id    
				,product_no 
				,test_flag  
				,sim_code   
				,usertype_id  )
	select trim(e.user_id)
				,e.product_no  
				,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
				,e.sim_code
				,f.usertype_id           
	from (select user_id , create_date ,product_no,sim_code,usertype_id
				 			,row_number() over(partition by user_id order by time_id desc ) row_id   
	      from bass1.g_a_02004_day
	      where time_id<=$op_month_last ) e 
	inner join ( select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
				       from bass1.g_a_02008_day
				       where time_id<=$op_month_last ) f on f.user_id=e.user_id 
	where e.row_id=1 and f.row_id=1 
	"
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
	declare global temporary table session.INT_CHECK_R027_MONTH_TMP_1
	(
		user_id              varchar(20)
	)
	with replace on commit preserve rows not logged in tbs_user_temp
	"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#������ʱ��3(�˽ӿ�200904�·ݵ�һ��ȫ��)
	set handle [ aidb_open $conn ]
	set sqlbuf "
	insert into session.INT_CHECK_R027_MONTH_TMP_1
	( user_id )
	select distinct user_id 
	from bass1.G_S_02047_MONTH where time_id>=200904
	"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�����ͨ����������Ϣ������ʱ��1
##	set handle [ aidb_open $conn ]
##	set sqlbuf "
##	insert into session.INT_CHECK_R027_MONTH_TMP_1
##	(
##		user_id
##		,imei
##	)
##	select
##		b.user_id
##		,rtrim(b.imei)
##	from
##		(select user_id,max(call_cnt) call_cnt from bass1.G_I_02047_MONTH where time_id = $op_month group by user_id) a
##	left join
##		(select user_id,imei,call_cnt from bass1.G_I_02047_MONTH where time_id = $op_month) b
##	on
##		a.user_id = b.user_id and
##		a.call_cnt = b.call_cnt"
##	puts $sqlbuf
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle

	#ͳ�������û�����imei��
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(distinct a.user_id)
	from session.INT_CHECK_R027_MONTH_TMP_1 a
	left join session.product_bass1_tmp2 b on a.user_id=b.user_id
	where  b.usertype_id not in ('2010','2020','2030','9000')
		   and b.test_flag='0'
	       and a.user_id not in (select user_id 
	                             from session.product_bass1_tmp1 
	                             where usertype_id in ('2010','2020','2030','9000'))
	"
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

	#ͳ�������û���
	set handle [ aidb_open $conn ]
##	set sqlbuf "
##	select
##		count(a.user_id)
##	from
##		bass1.G_A_02008_DAY a
##	where
##		time_id <= $op_month_last and
##		usertype_id not in ('2010','2020','2030','1040','1021','9000') and
##		exists (select 1 from (select max(time_id) as time_id,user_id as user_id from bass1.G_A_02008_DAY where time_id <= $op_month_last group by user_id) b where a.time_id = b.time_id and a.user_id = b.user_id);"
##	set sqlbuf "
##		select count(distinct user_id )
##		from  (select user_id ,usertype_id
##								  ,row_number() over(partition by user_id order by time_id desc ) row_id 
##			  	 from bass1.G_A_02008_DAY  
##					 where time_id <=$op_month_last) b
##		where b.row_id=1 and  b.usertype_id not in ('2010','2020','2030','1040','1021','9000')
##  "

	set sqlbuf "
	select count(distinct c.user_id)
	from session.product_bass1_tmp2 c
	where c.usertype_id not in ('2010','2020','2030','9000')
	  and c.test_flag='0'
  "
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
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$op_month
		,'R027'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * $RESULT_VAL1 / $RESULT_VAL2
		,0
	)
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#�жϳ������
	if { [ expr 1.000 * $RESULT_VAL1 / $RESULT_VAL2 ] < 0.85 || [ expr 1.000 * $RESULT_VAL1 / $RESULT_VAL2 ] > 1.05 } {
		set grade 2
		set alarmcontent "׼ȷ��ָ��R027�������ſ��˷�Χ"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
