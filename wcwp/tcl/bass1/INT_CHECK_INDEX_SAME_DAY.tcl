######################################################################################################
#�������ƣ�INT_CHECK_INDEX_SAME_DAY.tcl
#У��ӿڣ�G_S_22012_DAY.tcl,G_S_22201_DAY.tcl
#          G_A_02004_DAY.tcl,G_A_02008_DAY.tcl,G_S_04018_DAY.tcl,G_I_06031_DAY.tcl
#          R159_1:�����ͻ�����R159_2���ͻ���������R159_3�������ۼ�ʹ��TD������������ͻ�����R159_4�������ͻ���
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�LIUQF
#��дʱ�䣺2009-10-27
#�����¼��
#�޸���ʷ: 20100125 �����ͻ��ھ��䶯 usertype_id NOT IN ('2010','2020','2030','9000')
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        #���� yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#            
#        #����  yyyymm
#        set last_month [GetLastMonth [string range $op_month 0 5]]
#        
#        #��Ȼ�µ�һ�� yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
#        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        puts $l_timestamp
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_INDEX_SAME_DAY.tcl"


	#ɾ����������
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp and rule_code in ('R159_1','R159_2','R159_3','R159_4')"
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
	declare global temporary table session.int_check_user_status
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

	#��������û�����
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.int_check_user_status (
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
					      where time_id<=$timestamp ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$timestamp ) f on f.user_id=e.user_id
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
							
  #####################
  #R159_1:�����ͻ���

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

	#ȡͳ���յ�һ������õ���ָ��ֵ����һ����ȡ�û���ʷ������������ent_dtΪͳ���գ�����Ч����eff_dt�ڵ�ǰͳ�����ڣ�
  #ʧЧ����end_dt����ͳ���գ������û����ͱ��벻����3�����ԣ���������SIM���û���־������1�����ݣ���Ϊ�������û�������ͳ�ơ�
  #�ڶ�������ʡ��˾��ʶ����ָ����Ϊͳ���������û���
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.int_check_user_status
		where create_date = '$timestamp'
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
		,'R159_1'
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
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_1 һ���Լ�������ͻ�������1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_1 ---------------------------------------"
	
	
	
  #####################
  #R159_2:�ͻ�������

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

	#ȡͳ���յ�һ������õ���ָ��ֵ����һ����ȡ�û���ʷ������Ч����eff_dt�ڵ�ǰͳ��������ʧЧ����end_dt����ͳ�����ڣ�
	#�û����ͱ��벻����3�����ԣ���������SIM���û���־������1���û���ʶ��Ʒ�ơ��ڶ����������û�״̬��ʷ��tb_svc_subs_stat_hist��
	#���ж��û�״̬���ͱ��루Subs_Stat_Typ_Cd)
	#������2010���������ţ���2020���������ţ���2030���䶳�ڣ���9000����Ч������ʱ��
	#�������û���������count(distinct tb_svc_subs_hist.subs_id)����
	#����������ʡ��˾��ʶ����ָ�ꡣ
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.int_check_user_status
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
		,'R159_2'
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
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_2 һ���Լ��ͻ�����������1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_2 ---------------------------------------"
	
		
	
	
  #####################
  #R159_3:�����ۼ�ʹ��TD������������ͻ���

	#ȡͳ���յ�ʡ��˾�ϱ�ָ��ֵ����22201��ʹ��TD����Ŀͻ��ջ��ܣ��ӿ�ȡ�á������ۼ�ʹ��TD������������ͻ������ֶ�ֵ
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

 ##ȡͳ���յ�һ������õ���ָ��ֵ����һ����ѡȡͳ����ȥ��������ר�úŶ�����Ч�ķǲ����û��������û�״̬ȡ�����û���
 ##�ڶ�����ȡ������ʹ���ջ��ܱ�(TB_SUM_NTBK_SUBS_D)�У�������������Ϊ1�����ú���С��14744000000����� 14744005999�Ҵ��³�
 ##��ͳ������������ʹ����Ϊ���û���ͨ���ñ�󶨺���ǰ7λ��8λ��ǰ9λ�ڹ�������й�˾��MSISDN�ŶεĶ�Ӧ��ϵ
 ##��TB_SUM_PRVD_MSISDN_RELATION����ȡ�Ŷα�ʡ��˾��
 ##���������ڶ���������һ���û�״̬�����������ʡ��˾���ܸ�ʡ�����ۼ�ʹ��T�����������ͻ���.
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from 
		(
    select a.product_no from 
		    (
		    select product_no,substr(product_no,1,7) msisdn from bass1.G_S_04018_DAY
		     where time_id between $l_timestamp and $timestamp
		       and MNS_TYPE='1'
		       and bigint(INTRA_PRODUCT_NO) not between 14744000000 and 14744005999
		    ) a,
		    (  
		    select  distinct ltrim(rtrim(msisdn)) msisdn from bass1.g_i_06031_day  
		     where time_id between $l_timestamp and $timestamp
		    ) b
        where a.msisdn=b.msisdn
			) aa,
			(
			select user_id,product_no from session.int_check_user_status
			 where usertype_id NOT IN ('2010','2020','2030','9000')
			   and test_flag='0'
			) bb
  where aa.product_no=bb.product_no
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
		,'R159_3'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05} {
		set grade 2
	        set alarmcontent "R159_3 һ���Լ�鵱���ۼ�ʹ��TD������������ͻ�������5%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_3 ---------------------------------------"
		


  #####################
  #R159_4:�����ͻ���

	#ȡͳ���յ�ʡ��˾�ϱ�ָ��ֵ����22012����KPI���ӿ�ȡ�á������ͻ������ֶ�ֵ
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

 ##ȡͳ���յ�һ������õ���ָ��ֵ����һ����ȡ�û�����ʷ������Ч����eff_dt�ڵ�ǰͳ��������ʧЧ����end_dt����ͳ�����ڣ�
 ##�û����ͱ��벻����3�����ԣ���������SIM���û���־������1�����ݡ��ڶ����������û�״̬��ʷ��tb_svc_subs_stat_hist��
 ##��״̬��Ч����eff_dt�ڵ�ǰͳ��������ʧЧ����end_dt����ͳ������, �û�״̬���ͱ���Ϊ2010���������ţ���2020���������ţ���2030���䶳�ڣ����û�����
 ##����������ʡ��˾��ʶ���ܳ���ʡ��˾�������û�����
	set handle [ aidb_open $conn ]
	set sqlbuf "
			select count(distinct user_id) from session.int_check_user_status
			where usertype_id IN ('2010','2020','2030','9000')
			  and test_flag='0'
			  and time_id=$timestamp
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
		,'R159_4'
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
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_4 һ���Լ�������ͻ�������1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_4 ---------------------------------------"
		
	

	
	return 0
}

