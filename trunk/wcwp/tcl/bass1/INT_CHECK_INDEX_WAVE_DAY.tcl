######################################################################################################
#�������ƣ�INT_CHECK_INDEX_WAVE_DAY.tcl
#У��ӿڣ�G_S_22012_DAY.tcl
#
#R161_1:�����ͻ���                         ��15%
#R161_2:�ͻ�������                         ��2%
#R161_3:�����ͻ���                         ��100%
#R161_4:ͨ�ſͻ���                         ��5%
#R161_5:�����ۼ�ͨ�ſͻ���                 ��5%
#R161_6:ʹ��TD����Ŀͻ���                 ��5%
#R161_7:�����ۼ�ʹ��TD������ֻ��ͻ���     ��5%
#R161_8:�����ۼ�ʹ��TD�������Ϣ���ͻ���   ��5%
#R161_9:�����ۼ�ʹ��TD��������ݿ��ͻ���   ��5%
#R161_10:�����ۼ�ʹ��TD������������ͻ���  ��5%
#R161_11:��ͨ�ƶ��ͻ�����                  ��2%
#R161_12:�����ƶ��ͻ�����                  ��2%
#R161_13:��ͨ�ƶ������ͻ���                ��8%
#R161_14:�����ƶ������ͻ���                ��8%
#R161_15:ʹ��TD����Ŀͻ���T���ϼƷ�ʱ��   ��20%
#R161_16:ʹ��TD����Ŀͻ���T���ϵ��������� ��20%
#R161_17:�����ͻ���                        ��70%
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�LIUQF
#��дʱ�䣺2010-07-13
#�����¼��
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        puts $l_timestamp
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        puts $last_day
        #ǰ����
        set last_last_day [GetLastDay [string range $last_day 0 7]]
        puts $last_last_day
        #������
        set app_name "INT_CHECK_INDEX_WAVE_DAY.tcl"


	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp 
	                    and rule_code in ('R161_1','R161_2','R161_3','R161_4','R161_5','R161_6','R161_7','R161_8','R161_9','R161_10','R161_11','R161_12','R161_13','R161_14','R161_15','R161_16','R161_17')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#������ʱ��
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.check_user_status_1
				(
			   user_id        CHARACTER(15),
			   product_no     CHARACTER(15),
			   test_flag      CHARACTER(1),
			   sim_code       CHARACTER(15),
			   usertype_id    CHARACTER(4),
			   create_date    CHARACTER(15),
			   time_id        int
				)                            
				partitioning key           
				 (
				   user_id    
				 ) using hashing           
				with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#���ǰһ����û�����
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.check_user_status_1 (
								 user_id    
								,product_no 
								,test_flag  
								,sim_code   
								,usertype_id  
								,create_date
								,time_id )
					select e.user_id
								,e.product_no  
								,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
								,e.sim_code
								,f.usertype_id  
								,e.create_date  
								,f.time_id       
					from (select user_id,create_date,product_no,sim_code,usertype_id
								 			,row_number() over(partition by user_id order by time_id desc ) row_id   
					      from bass1.g_a_02004_day
					      where time_id<=$last_day ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$last_day ) f on f.user_id=e.user_id
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



	#������ʱ��
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.check_user_status_2
				(
			   user_id        CHARACTER(15),
			   product_no     CHARACTER(15),
			   test_flag      CHARACTER(1),
			   sim_code       CHARACTER(15),
			   usertype_id    CHARACTER(4),
			   create_date    CHARACTER(15),
			   time_id        int
				)                            
				partitioning key           
				 (
				   user_id    
				 ) using hashing           
				with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#���ǰ������û�����
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.check_user_status_2 (
								 user_id    
								,product_no 
								,test_flag  
								,sim_code   
								,usertype_id  
								,create_date
								,time_id )
					select e.user_id
								,e.product_no  
								,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
								,e.sim_code
								,f.usertype_id  
								,e.create_date  
								,f.time_id       
					from (select user_id,create_date,product_no,sim_code,usertype_id
								 			,row_number() over(partition by user_id order by time_id desc ) row_id   
					      from bass1.g_a_02004_day
					      where time_id<=$last_last_day ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$last_last_day ) f on f.user_id=e.user_id
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



    #########################################################################
    #R161_1:�����ͻ���

	#��22012����KPI���ӿ�ȡ�á������ͻ������ֶ�ֵ
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_new_users)) from bass1.g_s_22012_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ����һ����ȡ�û���ʷ������������ent_dtΪ�ϸ�ͳ���գ�����Ч����eff_dt���ϸ�ͳ�����ڣ�ʧЧ����end_dt����
    #�ϸ�ͳ���գ������û����ͱ��벻����3�����ԣ���������SIM���û���־������1�����ݣ���Ϊ�������û�������ͳ�ơ��ڶ�������ʡ��˾��
    #ʶ����ָ����Ϊ�ϸ�ͳ���յ������û�����
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
		where create_date = '$last_day'
		  and test_flag='0'
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
		$timestamp
		,'R161_1'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.15 || $RESULT_VAL3<=-0.15 } {
		set grade 2
	    set alarmcontent "R161_1 �����Լ�������ͻ�������15%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

    puts "R161_1 ---------------------------------------"



    #R161_2:�ͻ�������

	#��22012����KPI���ӿ�ȡ�á��ͻ����������ֶ�ֵ
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_dao_users)) from bass1.g_s_22012_day where time_id=$timestamp"
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

    ##ȡ�ϸ�ͳ���յ�ָ��ֵ����һ����ȡ�û���ʷ������Ч����eff_dt���ϸ�ͳ��������ʧЧ����end_dt�����ϸ�ͳ���գ�
    ##�û����ͱ��벻����3�����ԣ���������SIM���û���־������1���û���ʶ��Ʒ�ơ�
    ##�ڶ����������û�״̬��ʷ��tb_svc_subs_stat_hist�����ж��û�״̬���ͱ��루Subs_Stat_Typ_Cd)������2010���������ţ�
    ##��2020���������ţ���2030���䶳�ڣ���1040�������ڣ���1021������Ԥ���ţ���9000����Ч������ʱ��
    ##�������û���������count(distinct tb_svc_subs_hist.subs_id)����
    ##����������ʡ��˾��ʶ����ָ�ꡣ
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
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
		$timestamp
		,'R161_2'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_2 �����Լ��ͻ�����������2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_2 ---------------------------------------"



    #R161_3:�����ͻ���

	#ȡͳ���յ�ָ��ֵ����һ������ʡ��˾�ϱ���22012��KPI�еġ��ͻ���������ȡͳ���ա�
	#�ڶ�������ʡ��˾�ϱ���22012��KPI�еġ��ͻ���������ȡ�ϸ�ͳ���ա�
	#����������ʡ��˾��ʶ�õ�һ���Ľ����ȥ�ڶ����Ľ����
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_dao_users)) from bass1.g_s_22012_day where time_id=$timestamp"
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

	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_dao_users)) from bass1.g_s_22012_day where time_id=$last_day"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ����һ����ȡKPI���ݱ�dm_kpi_value���д�������Ϊ�ϸ�ͳ���յĿͻ���������
    #�ڶ�����ȡKPI���ݱ�kpi_value_new���д�������Ϊ���ϸ�ͳ���յĿͻ���������
    #����������ʡ��˾��ʶ�õ�һ���Ľ����ȥ�ڶ����Ľ�������õ������ͻ�����
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
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
	

	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_2
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL4 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	

	#��У��ֵ����У������
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R161_3'
		,$RESULT_VAL1-$RESULT_VAL3
		,$RESULT_VAL2-$RESULT_VAL4
		,1.000 * (($RESULT_VAL1-$RESULT_VAL3) - ($RESULT_VAL2-$RESULT_VAL4)) / ($RESULT_VAL2-$RESULT_VAL4)
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

    set  RESULT_VAL5 [expr 1.000 * (($RESULT_VAL1-$RESULT_VAL3) - ($RESULT_VAL2-$RESULT_VAL4)) / ($RESULT_VAL2-$RESULT_VAL4) ]
    puts  $RESULT_VAL5
	if {$RESULT_VAL5>=1.00 || $RESULT_VAL5<=-1.00 } {
		set grade 2
	    set alarmcontent "R161_3 �����Լ�龻���ͻ�������100%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_3 ---------------------------------------"



    #R161_4:ͨ�ſͻ���

	#ȡͳ���յ�ָ��ֵ���ӷ�Ʒ����KPI��TB_SUM_DAILY_KPI_BRND������ȡ��������ڵ���ͳ���յĸ�Ʒ�Ƶġ�����ͨ�ſͻ��������ݣ�
	#�ٽ�����Ʒ����ӵõ��ܵ�ͨ�ſͻ�����
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(d_comm_users)) from bass1.g_s_22038_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ���ӷ�Ʒ����KPI��TB_SUM_DAILY_KPI_BRND������ȡ��������ڵ����ϸ�ͳ���յĸ�Ʒ��
    #�ġ�����ͨ�ſͻ��������ݣ��ٽ�����Ʒ����ӵõ��ܵ�ͨ�ſͻ�����
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(d_comm_users)) from bass1.g_s_22038_day where time_id=$last_day"
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
		$timestamp
		,'R161_4'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_4 �����Լ��ͨ�ſͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_4 ---------------------------------------"




    #R161_5:�����ۼ�ͨ�ſͻ���

	#ȡͳ���յ�ָ��ֵ���ӷ�Ʒ����KPI��TB_SUM_DAILY_KPI_BRND������ȡ��������ڵ���ͳ���յĸ�Ʒ�Ƶġ������ۼ�ͨ�ſͻ��������ݣ�
	#�ٽ�����Ʒ����ӵõ��ܵĵ����ۼ�ͨ�ſͻ���
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_comm_users)) from bass1.g_s_22038_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ���ӷ�Ʒ����KPI��TB_SUM_DAILY_KPI_BRND������ȡ��������ڵ����ϸ�ͳ���յĸ�Ʒ�Ƶġ������ۼ�ͨ�ſͻ��������ݣ�
    #�ٽ�����Ʒ����ӵõ��ܵĵ����ۼ�ͨ�ſͻ���
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(m_comm_users)) from bass1.g_s_22038_day where time_id=$last_day"
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
		$timestamp
		,'R161_5'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_5 �����Լ�鵱���ۼ�ͨ�ſͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_5 ---------------------------------------"



    #R161_6:ʹ��TD����Ŀͻ���

	#ȡͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
	#�������Ϊͳ���յġ�ʹ��TD����Ŀͻ�������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(td_customer_cnt)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ�ʹ��TD����Ŀͻ�������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(td_customer_cnt)) from bass1.g_s_22201_day where time_id=$last_day"
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
		$timestamp
		,'R161_6'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_6 �����Լ��ʹ��TD����Ŀͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_6 ---------------------------------------"


    #R161_7:�����ۼ�ʹ��TD������ֻ��ͻ���

	#ȡͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
	#�������Ϊͳ���յġ������ۼ�ʹ��TD������ֻ��ͻ�����
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_usage_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ������ۼ�ʹ��TD������ֻ��ͻ�������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(mtl_td_usage_mark)) from bass1.g_s_22201_day where time_id=$last_day"
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
		$timestamp
		,'R161_7'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_7 �����Լ�鵱���ۼ�ʹ��TD������ֻ��ͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_7 ---------------------------------------"


    #R161_9:�����ۼ�ʹ��TD��������ݿ��ͻ���

	#ȡͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
	#�������Ϊͳ���յġ������ۼ�ʹ��TD��������ݿ��ͻ�������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_datacard_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ȡʹ��TD����Ŀͻ��ջ���(tb_sum_td_usd_net_cust_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ������ۼ�ʹ��TD��������ݿ��ͻ�������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(mtl_td_datacard_mark)) from bass1.g_s_22201_day where time_id=$last_day"
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
		$timestamp
		,'R161_9'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_9 �����Լ�鵱���ۼ�ʹ��TD��������ݿ��ͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_9 ---------------------------------------"



    #R161_10:�����ۼ�ʹ��TD������������ͻ���

	#ȡͳ���յ�ָ��ֵ����22201��ʹ��TD����Ŀͻ��ջ��ܣ��ӿ�ȡ�á������ۼ�ʹ��TD������������ͻ������ֶ�ֵ
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_3gbook_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ����һ����ѡȡ�ϸ�ͳ����ȥ��������ר�úŶ�����Ч�ķǲ����û��������û�״̬ȡ�����û���
    #�ڶ�����ȡ������ʹ���ջ��ܱ�(TB_SUM_NTBK_SUBS_D)�У�������������Ϊ1�����ú���С��14744000000����� 14744005999��
    #���³����ϸ�ͳ������������ʹ����Ϊ���û���ͨ���ñ�󶨺���ǰ7λ��8λ��ǰ9λ�ڹ�������й�˾��MSISDN�ŶεĶ�Ӧ��
    #ϵ��TB_SUM_PRVD_MSISDN_RELATION����ȡ�Ŷα�ʡ��˾��
    #���������ڶ���������һ���û�״̬�����������ʡ��˾���ܸ�ʡ�����ۼ�ʹ��T�����������ͻ���
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(target2)) from bass1.G_RULE_CHECK where rule_code='R159_3' and time_id=$last_day"
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
		$timestamp
		,'R161_10'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_10 �����Լ�鵱���ۼ�ʹ��TD������������ͻ�������5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_10 ---------------------------------------"



    #R161_11:��ͨ�ƶ��ͻ�����

	#ȡͳ���յ�ָ��ֵ��ȡ����������KPI(tb_sum_compt_kpi_daily)���У��������Ϊͳ���յġ���ͨGSM�ͻ���������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(union_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ȡ����������KPI(tb_sum_compt_kpi_daily)���У��������Ϊ�ϸ�ͳ���յġ���ͨGSM�ͻ���������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(union_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		$timestamp
		,'R161_11'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_11 �����Լ����ͨ�ƶ��ͻ���������2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_11 ---------------------------------------"



    #R161_12:�����ƶ��ͻ�����

	#ȡͳ���յ�ָ��ֵ��ȡ����������KPI(tb_sum_compt_kpi_daily)���У��������Ϊͳ���յġ�����CDMA�ͻ���������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tel_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ȡ����������KPI(tb_sum_compt_kpi_daily)���У��������Ϊ�ϸ�ͳ���յġ�����CDMA�ͻ���������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tel_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		$timestamp
		,'R161_12'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_12 �����Լ������ƶ��ͻ���������2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_12 ---------------------------------------"





    #R161_13:��ͨ�ƶ������ͻ���

	#ȡͳ���յ�ָ��ֵ�����ܾ���������KPI(tb_sum_compt_kpi_daily)���У����������ͳ�����ڵġ���ͨGSM�����ͻ�������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(union_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ�����ܾ���������KPI(tb_sum_compt_kpi_daily)���У�����������ϸ�ͳ�����ڵġ���ͨGSM�����ͻ�������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(union_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		$timestamp
		,'R161_13'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.08 || $RESULT_VAL3<=-0.08 } {
		set grade 2
	    set alarmcontent "R161_13 �����Լ����ͨ�ƶ������ͻ�������8%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_13 ---------------------------------------"



    #R161_14:�����ƶ������ͻ���

	#ȡͳ���յ�ָ��ֵ�����ܾ���������KPI(tb_sum_compt_kpi_daily)���У����������ͳ�����ڵġ�����CDMA�����ͻ�������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tel_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ�����ܾ���������KPI(tb_sum_compt_kpi_daily)���У�����������ϸ�ͳ�����ڵġ�����CDMA�����ͻ�������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tel_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		$timestamp
		,'R161_14'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.08 || $RESULT_VAL3<=-0.08 } {
		set grade 2
	    set alarmcontent "R161_14 �����Լ������ƶ������ͻ�������8%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_14 ---------------------------------------"



    #R161_15:ʹ��TD����Ŀͻ���T���ϼƷ�ʱ��

	#ȡͳ���յ�ָ��ֵ��ʹ��TD����Ŀͻ�ͨ������ջ���(tb_sum_td_usd_net_cust_call_d)���У�
	#�������Ϊͳ���յġ�ʹ��TD����Ŀͻ���T���ϼƷ�ʱ������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ʹ��TD����Ŀͻ�ͨ������ջ���(tb_sum_td_usd_net_cust_call_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ�ʹ��TD����Ŀͻ���T���ϼƷ�ʱ������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$last_day"
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
		$timestamp
		,'R161_15'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.2 || $RESULT_VAL3<=-0.2 } {
		set grade 2
	    set alarmcontent "R161_15 �����Լ��ʹ��TD����Ŀͻ���T���ϼƷ�ʱ������20%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_15 ---------------------------------------"




    #R161_16:ʹ��TD����Ŀͻ���T���ϵ���������

	#ȡͳ���յ�ָ��ֵ��ʹ��TD����Ŀͻ����������ջ���(tb_sum_td_usd_net_cust_data_d)���У�
	#�������Ϊͳ���յġ�ʹ��TD����Ŀͻ���T���ϵ�������������
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ��ʹ��TD����Ŀͻ����������ջ���(tb_sum_td_usd_net_cust_data_d)���У�
    #�������Ϊ�ϸ�ͳ���յġ�ʹ��TD����Ŀͻ���T���ϵ�������������
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$last_day"
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
		$timestamp
		,'R161_16'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.2 || $RESULT_VAL3<=-0.2 } {
		set grade 2
	    set alarmcontent "R161_16 �����Լ��ʹ��TD����Ŀͻ���T���ϵ�������������20%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_16 ---------------------------------------"






    #R161_17:ʹ��TD����Ŀͻ���T���ϵ���������

	#ȡͳ���յ�ָ��ֵ����22012����KPI���ӿ�ȡ�á������ͻ������ֶ�ֵ
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	    select sum(bigint(m_off_users)) from bass1.g_s_22012_day where time_id=$timestamp"
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

    #ȡ�ϸ�ͳ���յ�ָ��ֵ����һ����ȡ�û�����ʷ������Ч����eff_dt���ϸ�ͳ��������ʧЧ����end_dt�����ϸ�ͳ���գ�
    #�û����ͱ��벻����3�����ԣ���������SIM���û���־������1�����ݡ�
    #�ڶ����������û�״̬��ʷ��tb_svc_subs_stat_hist����״̬��Ч����eff_dt���ϸ�ͳ��������ʧЧ����end_dt
    #�����ϸ�ͳ����, �û�״̬���ͱ���Ϊ2010���������ţ���2020���������ţ���2030���䶳�ڣ����û�����
    #����������ʡ��˾��ʶ���ܳ���ʡ��˾�������û�����
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
			select count(distinct user_id) from session.check_user_status_1
			where usertype_id IN ('2010','2020','2030','9000')
			  and test_flag='0'
			  and time_id=$last_day
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
		$timestamp
		,'R161_17'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
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

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.7 || $RESULT_VAL3<=-0.7 } {
		set grade 2
	    set alarmcontent "R161_17 �����Լ�������ͻ�������70%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_17 ---------------------------------------"



	

	
	return 0
}

