######################################################################################################
#�ӿ����ƣ����ſͻ�ͳ������
#�ӿڱ��룺03013
#�ӿ�˵������¼�ɼ��ſͻ�ͳһ���ѵĸ������룬�������������ڼ��ſͻ��˵�������һ���Է�������֮�ͣ�
#          ��Ҫ��ʡ������ϵͳ�����߽��кϲ����ϴ���
#��������: G_S_03013_MONTH.tcl
#��������: ����03013������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.vpmn�����ͳ���������ظ� 2.������һЩ���������� standard_product <> '0000' and enterprise_id is not null and  enterprise_id <> ''
#�޸���ʷ: 1.��ԭ�� standard_product='0000' ��Ϊ standard_product='3001'     zhanght 20090610
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_03013_MONTH"
        #set db_user $env(DB_USER)
       
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

        #����G_S_03013_MONTH��session������session.tmp03013
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.tmp03013
	        (
	          enterprise_id          char(20)   not null , 
                  standard_product       char(4)    not null , 
                  standard_project       char(3)    not null , 
                  account_item           varchar(2) not null,
                  income                 bigint 
                )
               partitioning key (ENTERPRISE_ID) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
         puts $sql_buff                       
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����G_S_03013_MONTH��session������session.tmp03013"	

	#��ͨVPMN����ͳһ��������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                  enterprise_id, 
                  standard_product, 
                  standard_project,
                  account_item,
                  income
                  from
                  (
                       select
                          c.enterprise_id
                          ,'1000' as standard_product
                          ,'999'  as standard_project
                          ,'02' as account_item
                          , sum(a.fact_fee)*100 as income
                       from 
                          bass2.dw_acct_shoulditem_${op_month} a,
                          bass2.dw_enterprise_account_${op_month} b,
                          bass2.dw_enterprise_member_mid_${op_month} c
                       where 
                          c.free_mark =0
                          and c.test_mark = 0
                          and a.user_id = c.user_id
                          and a.acct_id = b.acct_id
                          and b.rec_status = 1
                          and a.item_id not in (80000027,80000032,80000033,80000101)
                       group by 
                          c.enterprise_id
                   )aa"        
        puts $sql_buff                                           
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "��ͨVPMN����ͳһ��������"	
#c.vpmn_mark = 1
#                          and 
#ȡ��vpmn�ھ�                          
                          
                          	
	#һ���Է�������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    enterprise_id, 
                    standard_product, 
                    '999',
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id
                     ,'3000' as standard_product
                     ,'09' as account_item
                     ,sum(b.pay_fee)*100 as income
                   from
                     bass2.dw_enterprise_msg_${op_month} a, 
                     bass2.dw_enterprise_oneoff_pay_${op_month} b
                   where 
                     a.enterprise_id = b.enterprise_id
                   group by 
                     a.enterprise_id
                  )t"        
        puts $sql_buff                                          
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "һ���Է�������"	
	
	
	#����ͳ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    enterprise_id, 
                    ent_prod_id, 
                    '999',
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'3001') as ent_prod_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from 
                     bass2.dw_enterprise_unipay_${op_month} a
                   where 
                     a.test_mark = 0 and service_id not in ('936')
                   group by 
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'3001')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') 
                  )t"              
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ͳ������"	
	
	
#����ͨ����ͳ��
	#����ͳ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end, 
                    '2033', 
                    '999',
                    '06',
                    sum(a.unipay_fee*100) as fact_fee
            from bass2.DW_ENTERPRISE_UNIPAY_$op_month a
							   left outer join 
                 bass2.DW_ENTERPRISE_MSG_$op_month b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
							   left join 
							   bass2.dw_enterprise_account_his_$op_month c on a.acct_id=c.acct_id 
           where a.enterprise_id not in ('891910006274','891950005002') and 
                 a.service_id in ('936') and a.TEST_MARK = 0 and 
                 case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end is not null
        group by case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end; "              
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ͳ������"	


#����ͨͳ������
	
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                    select
                    c.enterprise_id, 
                    '3001', 
                    '100',
                    '01',
                    sum(bigint(b.fee*100)) as income
               from bass2.dw_enterprise_industry_apply b left outer join
			              (select distinct enterprise_id,user_id from bass2.dw_enterprise_membersub_$op_month ) c
			              on b.user_id = c.user_id
              where  b.op_time = '$ThisMonthFirstDay' and b.apptype_id in (5) and b.user_id is not null and b.enterprise_id is not null 
               group by c.enterprise_id "
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ͳ������"	



                      
                          	

#    set sal_buf00 "insert into session.stat_enterprise_0028_tmp(city_id,s_index_id,result)
#                   select case
#                                       when b.level_def_mode = 1 then 888
#                                       else value(int(b.ent_city_id),891)
#                                  end as city_id,
#                          case a.apptype_id
#                              when 1 then 'H041'
#                              when 2 then 'H056'
#                              when 3 then 'H081'
#                              when 4 then 'H011'
#                              when 5 then 'H026'
#                          end,
#                          sum(a.fee)
#                   from $dw_enterprise_industry_apply a left outer join DW_ENTERPRISE_MEMBER_MID_${year}${month} b
#                   on  a.user_id=b.user_id
#                   where a.op_time = '$year-$month-$day'
#                     and a.apptype_id in (1,2,3,4,5)
#                   group by case
#                                       when b.level_def_mode = 1 then 888
#                                       else value(int(b.ent_city_id),891)
#                                  end,
#                          case a.apptype_id
#                              when 1 then 'H041'
#                              when 2 then 'H056'
#                              when 3 then 'H081'
#                              when 4 then 'H011'
#                              when 5 then 'H026'
#                          end"
#
#    puts $sal_buf00
#    if [catch {aidb_sql $handle $sal_buf00} errmsg] {
#      trace_sql $errmsg 1300
#      puts "errmsg:$errmsg"
#      return -1
#    }
#    aidb_close $handle
#    if [catch {set handle [aidb_open $conn]} errmsg] {
#        trace_sql $errmsg 1302
#        return -1
#      }
#    aidb_commit $conn
#    
#    
#    	
##������ҵӦ�ý��������
##������ҵӦ��
##H041	УѶͨ	УѶͨ����������            	  Ԫ	
##H056	����ͨ	��������ͨ����ͨ����������	    Ԫ	
##H081	ũ��ͨ	ũ��ͨ����������	              Ԫ	
##H011 ����ͨ	����ͨ����������    	          Ԫ
##H026 ����ͨ	����ͨ����������	              Ԫ	
#100-����ͨ
#110-У��ͨ
#120-����ͨ
#130-����ͨ
#140-ũ��ͨ
#150-�ǹ�ͨ
#160-��óͨ
#170-ҽ��ͨ
#180-����ͨ
#190-����ͨ
#200-����ͨ
#210-����ͨ
#
#
#	set handle [aidb_open $conn]
#	set sql_buff "insert into session.tmp03013
#                  select
#                    enterprise_id, 
#                    ent_prod_id, 
#                    '999',
#                    account_item,
#                    income
#                  from
#                  (
#                   select
#                     a.enterprise_id as enterprise_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'0000') as ent_prod_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') as account_item
#                     ,sum(a.unipay_fee)*100 as income
#                   from 
#                     bass2.dw_enterprise_unipay_${op_month} a
#                   where 
#                     a.test_mark = 0
#                   group by 
#                     a.enterprise_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'0000')
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') 
#                  )t"              
#        puts $sql_buff                                    
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	puts "����ͳ������"	




	#============����ʽ���в�������============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.$objecttable 
                  select
                    ${op_month},
                    '${op_month}',
                    enterprise_id,
                    standard_product, 
                    standard_project, 
                    account_item,
                    char(income      )
                  from
                  (
                   select
                     enterprise_id ,
                     standard_product, 
                     standard_project, 
                     account_item,
                     sum(income) as income              
                  from 
                      session.tmp03013
                  where enterprise_id is not null and  enterprise_id <> '' 
                  group by
                      enterprise_id , 
                      standard_product,
                      standard_project,
                      account_item
                )aa"   
           puts $sql_buff                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ʽ���в�������"
	
	aidb_close $handle

	return 0
}	