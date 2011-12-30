######################################################################################################
#�ӿ����ƣ���Ʒ���û���KPI
#�ӿڱ��룺22038
#�ӿ�˵������¼��Ʒ���û�����KPI��Ϣ
#��������: G_S_22038_DAY.tcl
#��������: ����22038������
#��������: ��
#Դ    ��1.bass2.dw_acct_shoulditem_today_yyyymmdd
#          2.bass2.dw_product_yyyymmdd 
#          3.bass1.int_22038_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.�ýӿڳ�����Ϊ�õ���Dw_comp_cust_dt�����Բ���������ǰ�����ݡ�
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $op_time 8 9]
        #���µ�һ�� yyyymmdd
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        set this_month_first_day [string range $op_month 0 5]01

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22038_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#01:������ʱ��(��������ʱ��)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22038_day_tmp1
                     (
			  brand_id          varchar(1),
			  income            bigint,
			  d_comm_users      bigint,
			  m_comm_users      bigint,
			  d_voice_users     bigint,
			  m_voice_users     bigint,
			  d_incr_users      bigint,
			  m_incr_users      bigint,
			  w_comm_nums       bigint,
			  d_incr_fee        bigint,
			  d_voice_incr_fee  bigint,
			  d_sms_fee         bigint,
			  d_other_fee       bigint		
                      )
                      partitioning key 
                      (brand_id)
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
					
	#04�����ճ��ʵ�����֮��
        set handle [aidb_open $conn]
        set sql_buff "insert into session.g_s_22038_day_tmp1
                 select
                   coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2'),
                   bigint(sum(a.fact_fee)*100),
                   0,0,0,0,0, 0,0,0,0,0, 0
                 from 
                    bass2.dw_acct_shoulditem_today_$timestamp a,
                    bass2.dw_product_$timestamp  b
                 where
                   a.user_id=b.user_id
                 group by
                   b.brand_id "               
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#05������ͨ���û���
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,
                       count (distinct user_id),
                       0,0,0,0,0, 0,0,0,0,0
                     from 
                       bass1.int_22038_$op_month 
                     where 
                       op_time=$timestamp
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle       
	
	#06:�����ۼ�ͨ���û��� 
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,
                        count (distinct user_id),
                       0,0,0,0,0, 0,0,0,0
                     from
                        bass1.int_22038_$op_month 
                     where 
                        op_time<=$timestamp
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        #07:���������û���
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,
                       count(distinct user_id),
                       0,0,0,0,0, 0,0,0
                     from 
                       bass2.dw_product_$timestamp
                     where 
                       day_call_mark=1
                     group by
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#08:�����ۼ������ͻ���
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0, 0,0
	             from
	               bass2.dw_product_$timestamp
                     where
                       month_call_mark=1 
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#09:������ֵҵ��ʹ�ÿͻ���
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0,0
	             from
	               bass2.dw_product_$timestamp
                     where
                       day_newbusi_mark =1 
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
		
	#10:������ֵҵ��ʹ�ÿͻ���
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0
	             from  bass2.dw_product_$timestamp
              where  month_newbusi_mark=1 
              group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
			
	#11:����ͨ�ſͻ���(��ָ��Ϊ��ָ�꣬ÿ��ֻ�����壨�������ڣ���һ�Σ���������0)
	set friday [ clock format [ clock scan $optime ] -format "%A" ]
	if { ${friday} == "Friday" } {
	  set Thursday $last_day
	  puts $Thursday
	  set Wednesday [GetLastDay [string range $Thursday 0 7]]
	  puts $Wednesday
	  set Tuesday   [GetLastDay [string range $Wednesday 0 7]]
	  puts $Tuesday
	  set Monday    [GetLastDay [string range $Tuesday 0 7]]
	  puts $Monday
	  set Sunday    [GetLastDay [string range $Monday 0 7]]
	  puts $Sunday
	  set Saturday  [GetLastDay [string range $Sunday 0 7]]
	  puts $Saturday
	  
          set handle [aidb_open $conn]
          set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(t.brand_id)),'2'),
	               0,0,0,0,0,0,0,
                       count (distinct t.user_id),
                        0,0,0,0
                     from
                       (
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$timestamp
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Thursday
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Wednesday
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Tuesday
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Monday 
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Sunday 
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Saturday                                                               
                       )t
                     group by
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(t.brand_id)),'2') "
          puts $sql_buff
          if [catch { aidb_sql $handle $sql_buff } errmsg ] {
        		WriteTrace "$errmsg" 2020
        		puts $errmsg
        		aidb_close $handle
        		return -1
	  }
	  aidb_commit $conn
	  aidb_close $handle 	  
		
        }
        
  #12:������ֵҵ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0,0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100 in (4,5,6,7)
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   
	
	#����������ֵҵ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=5
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle 
        
        #���ն�����ҵ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=6
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle 
	
	#���շǶ�����ҵ������(���շǶ�����ҵ������=������ҵ������-����������ֵҵ������-���ն�����ҵ������)
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   brand_id,
                   0,0,0,0,0,0,0,0,0,0,0,
                   sum(d_incr_fee-d_voice_incr_fee-d_sms_fee)
                from
                  session.g_s_22038_day_tmp1
                group by 
                  brand_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    
	
        #13:���ܵ������
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22038_day 
	             select
	               $timestamp
	               ,'$timestamp'
	               ,char(brand_id)
                       ,char(sum(income))           
                       ,char(sum(d_comm_users))     
                       ,char(sum(m_comm_users))     
                       ,char(sum(d_voice_users))    
                       ,char(sum(m_voice_users))    
                       ,char(sum(d_incr_users))     
                       ,char(sum(m_incr_users))     
                       ,char(sum(w_comm_nums))      
                       ,char(sum(d_incr_fee))       
                       ,char(sum(d_voice_incr_fee)) 
                       ,char(sum(d_sms_fee))        
                       ,char(sum(d_other_fee)) 
	             from
	               session.g_s_22038_day_tmp1 
	             group by
	               char(brand_id) "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	      
#####################################
	return 0
}