######################################################################################################
#�ӿ����ƣ�����100ҵ�����ջ���
#�ӿڱ��룺22301
#�ӿ�˵����
#��������: G_S_22301_DAY.tcl
#��������: ����22301������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-07-06
#�����¼��1.
#�޸���ʷ: 1.�µļ���ID ��89103001033332
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_S_22301_DAY where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#3000	����100
	#3010	  ͨ�Ŷ���
	#3011	    ͨ�Ŷ���ҵ���1
	#3012	    ͨ�Ŷ���ҵ���2
	#3013	    ����ͨ�Ŷ���ҵ���
	#3020	  Ӫ������
	#3021	    Ӫ������ҵ���1
	#3022	    Ӫ������ҵ���2
	#3023	    ����Ӫ������ҵ���
	#3030	  �칫����
	#3031	    �칫����ҵ���1
	#3032	    �칫����ҵ���2
	#3033	    �����칫����ҵ���
	
	  #01:������ʱ��1(װ������)
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.G_S_22301_DAY_tmp1
	              (
									ent_busi_type		  char(4)  not null,
									enterprise_id			char(20) not null,
									use_numbers		    bigint,
									sms_counts			  bigint,
									mms_counts			  bigint,
									gprs_flow				  bigint,
									wap_counts				bigint,
									web_counts				bigint,
									email_counts			bigint,
									bill_duration_m		bigint
	              )
	              partitioning key
	              (enterprise_id,ent_busi_type)
	              using hashing	              
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle 
	
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    b.bass1_value as ent_busi_type,
			    a.enterprise_id,
			    count(distinct a.user_id) as use_numbers,
			    0 as sms_counts,
			    0 as mms_counts,
			    0 as gprs_flow,
			    0 as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    0 as bill_duration_m
			from
			    bass2.dw_enterprise_extsub_rela_ds a,
			    (select * from bass1.all_dim_lkp_163 where bass1_tbid='BASS_STD1_0108') b
			where a.service_id=b.xzbas_value
			  and a.enterprise_id not in ('891910006274','891880005002')
			  and a.enterprise_id in ('89103001033332')
			  and b.bass1_value in ('3000','3010','3011','3012','3013','3020','3021','3022','3023','3030','3031','3032','3033')
			group by b.bass1_value,a.enterprise_id
  "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  ## �����������
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    '3011',
			    '89103001033332',
			    0 as use_numbers,
			    sum(b.counts) as sms_counts,
			    0 as mms_counts,
			    0 as gprs_flow,
			    0 as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    0 as bill_duration_m
			from bass2.dw_enterprise_member_mid_$timestamp a,bass2.dw_newbusi_ismg_$timestamp b,
         (select service_id,int(plan_id) plan_id from bass2.dim_ent_group_plan where service_id in ('912')
          ) c
			where a.user_id = b.user_id and b.plan_id = c.plan_id
			  and a.enterprise_id in ('89103001033332')
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  ## �����������
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    '3011',
			    '89103001033332',
			    0 as use_numbers,
			    0 as sms_counts,
			    sum(b.counts) as mms_counts,
			    0 as gprs_flow,
			    0 as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    0 as bill_duration_m
			from bass2.dw_enterprise_member_mid_$timestamp a,bass2.dw_newbusi_mms_$timestamp b,
         (select service_id,int(plan_id) plan_id from bass2.dim_ent_group_plan where service_id in ('912')
          ) c
			where a.user_id = b.user_id and b.plan_id = c.plan_id
			  and a.enterprise_id in ('89103001033332')
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  ## ����GPRS����
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    '3011',
			    '89103001033332',
			    0 as use_numbers,
			    0 as sms_counts,
			    0 as mms_counts,
			    sum(rating_res) as gprs_flow,
			    0 as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    0 as bill_duration_m
			from bass2.dw_enterprise_member_mid_$timestamp a,bass2.dw_newbusi_gprs_$timestamp b,
         (select service_id,int(plan_id) plan_id from bass2.dim_ent_group_plan where service_id in ('912')
          ) c
			where a.user_id = b.user_id and b.plan_id = c.plan_id
			  and a.enterprise_id in ('89103001033332')
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  ## ����WAPҵ����
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    '3011',
			    '89103001033332',
			    0 as use_numbers,
			    0 as sms_counts,
			    0 as mms_counts,
			    0 as gprs_flow,
			    sum(b.counts) as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    0 as bill_duration_m
			from bass2.dw_enterprise_member_mid_$timestamp a,bass2.dw_newbusi_wap_$timestamp b,
         (select service_id,int(plan_id) plan_id from bass2.dim_ent_group_plan where service_id in ('912')
          ) c
			where a.user_id = b.user_id and b.plan_id = c.plan_id
			  and a.enterprise_id in ('89103001033332')
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  ## ����Ʒ�ʱ��
  set handle [aidb_open $conn]
	set sql_buff "insert into session.G_S_22301_DAY_tmp1
			(
         ent_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    '3011',
			    '89103001033332',
			    0 as use_numbers,
			    0 as sms_counts,
			    0 as mms_counts,
			    0 as gprs_flow,
			    0 as wap_counts,
			    0 as web_counts,
			    0 as email_counts,
			    sum(b.call_duration_m) as bill_duration_m
			from bass2.dw_enterprise_member_mid_$timestamp a,bass2.dw_call_$timestamp b,
         (select service_id,int(plan_id) plan_id from bass2.dim_ent_group_plan where service_id in ('912')
          ) c
			where a.user_id = b.user_id and b.plan_id = c.plan_id
			  and a.enterprise_id in ('89103001033332')
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  ## ����Ŀ���
  set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_S_22301_DAY
			(  
			   time_id
        ,enterprise_busi_type
				,enterprise_id
				,use_numbers
				,sms_counts
				,mms_counts
				,gprs_flow
				,wap_counts
				,web_counts
				,email_counts
				,bill_duration_m
			)
			select
			    $timestamp,
			    ent_busi_type,
			    enterprise_id,
			    char(value(sum(use_numbers),0))      as use_numbers,
			    char(value(sum(sms_counts),0))       as sms_counts,
			    char(value(sum(mms_counts),0))       as mms_counts,
			    char(value(sum(gprs_flow),0))        as gprs_flow,
			    char(value(sum(wap_counts),0))       as wap_counts,
			    char(value(sum(web_counts),0))       as web_counts,
			    char(value(sum(email_counts),0))     as email_counts,
			    char(value(sum(bill_duration_m),0))  as bill_duration_m
			from session.G_S_22301_DAY_tmp1
		group by ent_busi_type,enterprise_id
    "

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle







	return 0
}