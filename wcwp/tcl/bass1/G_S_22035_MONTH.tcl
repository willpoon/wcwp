######################################################################################################
#�ӿ����ƣ����ſͻ���׼����Ʒʹ�����
#�ӿڱ��룺22035
#�ӿ�˵������¼���ſͻ���ʹ�õĸ���ű�׼����Ʒ��ʹ�������
#          ���Ų�Ʒ��ҵ����ָ��ȽϷ��ӣ����ʡ���ռ��ſͻ����������Ʒ��Ӧҵ������
#��������: G_S_22035_MONTH.tcl
#��������: ����22035������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.����MAS���� 20071202 �Ļ�ѧ
#          2.���ڼ��Ŷ�����פADCƽ̨�����Ŷ���ҵ�����������Ϊ�㣬������ҵ����������ͨ��ADC����ͳ�ơ�
#           �����Ŷ���������ҵ������һ���ӿ�һֱ��Ϊ�����������ϴ������ţ����ݼ��ſͻ����ĵ�Ҫ��Ŀǰ�Ѿ��Ѽ��Ŷ���������ҵ����ȫ������ADC������ҵ������
#             �Ӷ����Ŷ���������ҵ��������Ϊ0 ������ᵼ��һ���ϴ������ظ�����  2008-06-27  �� 2001 ����Ϊ 2038
#          3.�����������ݿ��ͳ���ͨҵ�� 20080904 �Ļ�ѧ
#          4.���Ӽ���'891891000209'MASҵ�� 20090608 zhanght
#          5.�޸�ADC���Ŷ�������ADC�����û��� 20090701 liuqf
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2009-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        #----���������һ��---#,��ʽ yyyymmdd
        puts $last_month_day
        
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_22035_MONTH"
        #set db_user $env(DB_USER)
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


  #����G_S_22035_MONTH��session������session.tmp22035
	set sql_buff "declare global temporary table session.tmp22035
	        (
	          enterprise_id          char(20) not null , 
                  standard_product       char(4) not null , 
                  member_counts          int not null,
                  bill_duration          bigint ,
                  info_number            bigint,
                  z_convert_f            bigint,
                  flow                   bigint,
                  bandwidth              bigint,
                  netlines               bigint,
                  ipvpn                  bigint,
                  server_num             bigint,
                  virtualhost            int,
                  vpmn_netin_income      bigint,
                  vpmn_netin_standard    bigint,
                  z_vpmn_month_income    bigint,
                  z_vpmn_standard        bigint,
                  income                 bigint
                )
               partitioning key (ENTERPRISE_ID) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����G_S_22035_MONTH��session������session.tmp22035"
	
	
	#���� 1000��ͨVPMN �ĳ�Ա������,�Ʒ�ʱ��
	set sql_buff "insert into session.tmp22035 (
	          enterprise_id,
                  standard_product, 
                  member_counts,
                  bill_duration,
                  info_number ,
                  z_convert_f ,
                  flow ,
                  bandwidth ,
                  netlines ,
                  ipvpn ,
                  server_num ,
                  virtualhost ,
                  vpmn_netin_income ,
                  vpmn_netin_standard ,
                  z_vpmn_month_income ,
                  z_vpmn_standard ,
                  income )
                  select
                  enterprise_id,
                  standard_product, 
                  member_counts,
                  bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income ,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                        select
                          enterprise_id
                          ,'1000' as standard_product
                          ,count(distinct user_id) as member_counts
                          ,sum(call_duration_m) as bill_duration
                        from 
                          bass2.dw_enterprise_member_mid_${op_month}
                        group by 
                          enterprise_id
                   )a"          
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���� 1000��ͨVPMN �ĳ�Ա������,�Ʒ�ʱ��"
#                        where 
#                          vpmn_mark=1  
#ȡ����vpmn_mark���ƿھ������ֺͱ�������һֱ��20090508  �Ļ�ѧ
	

        
	#���� 1000��ͨVPMN �� VPMN������������ 
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                   select
                     b.enterprise_id as enterprise_id
                     ,'1000' as standard_product
                     ,sum(a.fact_fee)*100 as vpmn_netin_income
                   from 
                     bass2.dw_acct_shoulditem_${op_month} a,
                     bass2.dw_enterprise_member_mid_${op_month} b
                   where 
                     a.item_id in (80000011,80000053) 
                     and a.user_id = b.user_id
                     and b.free_mark = 0
                     and b.test_mark = 0
                   group by 
                     b.enterprise_id
                  )aa"         
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���� 1000��ͨVPMN �� VPMN������������"
#                     and b.vpmn_mark = 1
#ȡ����vpmn_mark���ƿھ������ֺͱ�������һֱ��20090508  �Ļ�ѧ

	
	#VPMN����ͳһ��������
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                       select
                          b.enterprise_id
                          ,'1000' as standard_product
                          , sum(a.fact_fee)*100 as vpmn_netin_standard
                       from 
                          bass2.DW_ACCT_SHOULDITEM_${op_month} a,
                          bass2.dw_enterprise_account_${op_month} b,
                          bass2.dw_enterprise_member_mid_${op_month} c
                       where 
			  c.free_mark = 0
			  and c.test_mark = 0
			  and a.user_id = c.user_id
			  and a.acct_id = b.acct_id
			  and b.rec_status = 1
			  and a.item_id in (80000011,80000053)
                       group by 
                          b.enterprise_id
                   )aa"       
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����VPMN����ͳһ��������"
#c.vpmn_mark =1
#			  and 
#ȡ����vpmn_mark���ƿھ������ֺͱ�������һֱ��20090508  �Ļ�ѧ



	
	#VPMN����������
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income,
                  0 as z_vpmn_standard ,
                  incomo
                  from
                  (
                       select
                          enterprise_id
                          ,'1000' as standard_product
                          , sum(stat_fee)*100 as incomo
                       from 
                          bass2.dw_enterprise_member_mid_${op_month} 
                       where 
                          free_mark = 0
                          and test_mark = 0
                       group by 
                          enterprise_id
                   )aa"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "VPMN����������"
#vpmn_mark = 1
#                          and 
#ȡ����vpmn_mark���ƿھ������ֺͱ�������һֱ��20090508  �Ļ�ѧ




  #���Ų���ʹ�ü��ſͻ������� 
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                    select
                      a.enterprise_id,
                      '2006' as standard_product
                    from 
                      bass2.dw_enterprise_msg_${op_month} a,
                      bass2.dw_enterprise_extsub_rela_${op_month} b
                    where 
                      b.service_id = '933'
                      and b.user_id <> '-1'
                      and b.rec_status = 1
                      and a.enterprise_id = b.enterprise_id
                  )aa"         
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���Ų���ʹ�ü��ſͻ�������  "
        
  #2006	���Ų���ʹ�ó�Ա������ service_id = 933	���Ų���ҵ��
  #2011  ��ҵ��E��SIM���� 
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  ( 
                    select
                      a.enterprise_id,
                      case b.service_id
                          when '933' then '2006'
                          when '934' then '2011'
                      end standard_product,
                      count(distinct b.user_id) as member_counts
                    from 
                      bass2.dw_enterprise_msg_${op_month} a,
                      bass2.dw_enterprise_extsub_rela_${op_month} b                      
                    where 
                      b.service_id in ('933','934')
                      and b.user_id <> '-1'
                      and b.city_id <> '-1'
                      and b.rec_status=1
                      and a.enterprise_id = b.enterprise_id 
                      and a.enterprise_id not in ('891910006274')
                    group by 
                      a.enterprise_id,
                      case b.service_id
                          when '933' then '2006'
                          when '934' then '2011'
                      end                       
                  )aa"         
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���Ų���ҵ��ʹ�ó�Ա������/��ҵ��E��SIM���� "
	        	        		
	#2001	���Ŷ���ʹ�ó�Ա������ service_id = 910	���Ŷ���ҵ��
        #2008	������ר��ʹ�ó�Ա������  918	ר��ҵ�� 912	����ר�߲�Ʒ����
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                    select
                      a.enterprise_id,
                      case 
                        when a.service_id = '910'          then '2001'
                        when a.service_id in ('918','912') then '2008'
                      end as standard_product,
                      count(distinct a.user_id) as member_counts
                    from 
                      bass2.dw_enterprise_extsub_rela_${op_month} a,
                      bass2.dw_enterprise_msg_${op_month} b
                    where 
                      service_id in ('910','918','912') and 
                      a.user_id <> '-1' and 
                      a.enterprise_id = b.enterprise_id
                    group by 
                      a.enterprise_id,
                      case 
                        when a.service_id = '910'          then '2001'
                        when a.service_id in ('918','912') then '2008'
                       end
                  )aa"         
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���Ŷ���ҵ��,���Ų���ҵ��,ר��ҵ��,��ҵ��e��ҵ��ʹ�ó�Ա������ "
	
		
	#2001	���Ŷ��ŵ���ҵ����
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                   select
                     a.enterprise_id
                     ,'2001' as standard_product
                     ,sum(b.counts)   as info_number
                  from 
                    bass2.dw_enterprise_member_mid_${op_month} a,
                    bass2.dw_newbusi_ismg_${op_month} b
                  where 
                    b.SER_CODE='06666' and 
                    a.user_id = b.user_id 
                  group by 
                    a.enterprise_id
                 )aa"            
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���Ŷ��ŵ���ҵ����"

#JT2110        ���Ŷ���ʹ�ü��ſͻ�������        service_id = 910        ���Ŷ���ҵ��                    
#JT2230        ��ҵ��E��ʹ�ü��ſͻ�������       service_id = 934        ��ҵ��e��ҵ�� 
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  0 as income 
                  from
                  (
                   select
                     a.enterprise_id,
                     case 
                       when a.level_def_mode = 1 then 888
                       else int(a.ent_city_id)
                     end,
                     case 
                       when b.service_id = '910' then '2001'
                       when b.service_id = '934' then '2011'
                       when b.service_id in ('944','946','947')  then '2016'
                     end as standard_product
                  from 
                    bass2.dw_enterprise_msg_${op_month} a,
                    bass2.dw_enterprise_extsub_rela_${op_month} b
                  where 
                    b.service_id in ('910','934','944','946','947') and b.REC_STATUS = 1
                    and a.enterprise_id = b.enterprise_id and b.enterprise_id not in ('891880005002','891910006274') 
                  group by 
                    a.enterprise_id,
                    case 
                      when a.level_def_mode = 1 then 888
                      else int(a.ent_city_id)
                   end,
                   case 
                      when b.service_id = '910' then '2001'
                      when b.service_id = '934' then '2011'
                      when b.service_id in ('944','946','947')  then '2016'
                   end
                 )aa"            
 	puts $sql_buff
	exec_sql $sql_buff
	puts "���Ŷ���\��ҵ��E��ʹ�ü��ſͻ�������"
	      	
	#2001	���Ŷ���ͳһ�������� service_id = 910	���Ŷ���ҵ��
        #2004	���Ų���ͳһ�������� service_id = 933	���Ų���ҵ��
        #2008	������ר��ͳһ��������  918	ר��ҵ�� 912	����ר�߲�Ʒ����
        #2011	��ҵ��E��ͳһ��������   934	��ҵ��e��ҵ��
	set sql_buff "insert into session.tmp22035 
                  select
                  enterprise_id, 
                  standard_product, 
                  0 as member_counts,
                  0 as bill_duration,
                  0 as info_number ,
                  0 as z_convert_f ,
                  0 as flow ,
                  0 as bandwidth ,
                  0 as netlines ,
                  0 as ipvpn ,
                  0 as server_num ,
                  0 as virtualhost ,
                  0 as vpmn_netin_income,
                  0 as vpmn_netin_standard ,
                  0 as z_vpmn_month_income ,
                  0 as z_vpmn_standard ,
                  income 
                  from
                  (
                    select
                          a.enterprise_id as enterprise_id,
                          case 
                              when a.service_id = '933'          then '2006'
                              when a.service_id in ('918','912') then '2008'
                              when a.service_id in ('934')       then '2011'
                              end as standard_product,
                          sum(a.unipay_fee+a.non_unipay_fee)*100 as income
                    from 
                          bass2.dw_enterprise_unipay_${op_month} a
                    where 
                          a.service_id in ('933','918','912','934') and test_mark = 0 and value(a.enterprise_id,'') <> '' 
                    group by 
                          a.enterprise_id,
                          case 
                              when a.service_id = '933'          then '2006'
                              when a.service_id in ('918','912') then '2008'
                              when a.service_id in ('934')       then '2011'
                              end
                  )aa"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "ͳһ��������"
	
	
	
	#3000	һ���Է�������
	set sql_buff "insert into session.tmp22035 
                  select
                   t.enterprise_id, 
                   t.standard_product, 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   t.income 
                  from
                  (
                   select
                     a.enterprise_id
                     ,'3000' as standard_product
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
	exec_sql $sql_buff
	puts "һ���Է�������"
	



  #MASҵ����
  #2017	MAS������
  #2018	�ֻ����䣨MAS)
  #2019	�ƶ�OA��MAS��
  #2020	�ƶ������棨MAS)
  #2021	�ƶ�����MAS��
  #2022	�ƶ�CRM��MAS��
  #2023	������վ��MAS��
  #2031	BlackBerry��MAS��
  #2032	������MAS)
  
  #MASҵ����
	set sql_buff "insert into session.tmp22035 
                  select
                      b.ENTERPRISE_ID, 
                      case when b.service_id = '941' then '2017'
                           when b.service_id = '142' then '2022'
                      end,
                      0 as member_counts,
                      0 as bill_duration,
                      sum(c.COUNTS) as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   bass2.dw_enterprise_sub_$op_month b,
                             bass2.dw_newbusi_ismg_$op_month c
                      where  b.user_id = c.user_id and b.enterprise_id not in ('891910006274','891880005002') 
					         and b.rec_status = 1 and b.service_id in ('142','941') and bigint(b.prod_id) = bigint(c.plan_id)
                      group by 
                          b.enterprise_id,c.CALLTYPE_ID,
                          case when b.service_id = '941' then '2017'
                               when b.service_id = '142' then '2022'
                          end;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "MASҵ������"	


  #	�ƶ�OA(MAS)
	set sql_buff "insert into session.tmp22035 
                  select
                      b.ENTERPRISE_ID, 
                      '2019',
                      0 as member_counts,
                      0 as bill_duration,
                      sum(c.COUNTS) as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   bass2.dw_enterprise_sub_$op_month b,
                             bass2.dw_newbusi_ismg_$op_month c
                      where  b.user_id = c.user_id and b.enterprise_id not in ('891910006274','891880005002') 
					         and b.rec_status = 1 and c.plan_id = 93901001 and bigint(b.prod_id) = bigint(c.plan_id)
                      group by 
                          b.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ�OA(MAS)ҵ������"	


  #select * from bass2.dim_acct_item where item_name like '%MAS%' with ur
  #80000448	OA��MAS�����ܷ�
  #80000178	CRM��MAS�����ܷ�
  #80000179	CRM��MAS��ͨ�ŷ�
  #80000200	MAS���������ܷ�
  #	�ƶ�OA(MAS)ҵ������
	set sql_buff "insert into session.tmp22035 
                  select
                      b.ENTERPRISE_ID, 
                      '2019',
                      0 as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      sum(a.FACT_FEE*100) as income 
                  from
                      bass2.DW_ACCT_SHOULDITEM_$op_month a
                    left outer join
                      bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                    on
                      a.user_id = b.user_id
                  where
                      a.item_id in (80000448) and
                      a.acct_id not in ('1001261088') and
                      b.user_id in (select user_id from bass2.dw_product_sprom_$op_month  
                                     where sprom_id in (99001610,99001611,99001612,99001619) 
                                           and plan_id in (93901001) and active_mark=1)
                  group by
                      b.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ�OA(MAS)ҵ������(ͳ��)"	


	set sql_buff "insert into session.tmp22035 
                  select
                      b.ENTERPRISE_ID, 
                      '2019',
                      0 as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      sum(a.FACT_FEE*100) as income 
                  from
                      bass2.DW_ACCT_SHOULDITEM_$op_month a
                    left outer join
                      bass2.DW_ENTERPRISE_MEMBERSUB_$op_month b
                    on
                      a.user_id = b.user_id
                  where
                      a.item_id in (80000448) and
                      a.acct_id not in ('1001261088') and
                      b.user_id not in (select user_id from bass2.dw_product_sprom_$op_month  
                                     where sprom_id in (99001610,99001611,99001612,99001619) 
                                           and plan_id in (93901001) and active_mark=1)
                  group by
                      b.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ�OA(MAS)ҵ������(��ͳ��)"	




  #	2017	MAS������ҵ������ 2022	�ƶ�CRM(MAS)ҵ������ 
	set sql_buff "insert into session.tmp22035 
                  select
                      b.ENTERPRISE_ID, 
                      case when a.item_id in (80000200) then '2017'
                           when a.item_id in (80000178,80000179) then '2022'
                      end,
                      0 as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      sum(a.FACT_FEE*100) as income 
                  from
                      bass2.DW_ACCT_SHOULDITEM_$op_month a
                    left outer join
                      bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                    on
                      a.user_id = b.user_id
                  where
                      a.item_id in (80000178,80000179,80000200) and
                      a.acct_id not in ('1001261088') 
                  group by
                      b.enterprise_id,
                      case when a.item_id in (80000200) then '2017'
                           when a.item_id in (80000178,80000179) then '2022'
                      end;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "2017	MAS������ҵ������ 2022	�ƶ�CRM(MAS)ҵ������"	





#  #	2017	MAS������ҵ������ 2022	�ƶ�CRM(MAS)ҵ������ 
#	set sql_buff "insert into session.tmp22035 
#                  select
#                      b.ENTERPRISE_ID, 
#                      case when a.item_id in (80000200) then '2017'
#                           when a.item_id in (80000178,80000179) then '2022'
#                           when a.item_id in (80000448) then '2019'
#                      end,
#                      0 as member_counts,
#                      0 as bill_duration,
#                      0 as info_number ,
#                      0 as z_convert_f ,
#                      0 as flow ,
#                      0 as bandwidth ,
#                      0 as netlines ,
#                      0 as ipvpn ,
#                      0 as server_num ,
#                      0 as virtualhost ,
#                      0 as vpmn_netin_income,
#                      0 as vpmn_netin_standard ,
#                      0 as z_vpmn_month_income ,
#                      0 as z_vpmn_standard ,
#                      sum(a.FACT_FEE) as income 
#                  from
#                      bass2.DW_ACCT_SHOULDITEM_$op_month a
#                    left outer join
#                      bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
#                    on
#                      a.user_id = b.user_id
#                  where
#                      a.item_id in (80000448,80000178,80000179,80000200) and
#                      a.acct_id not in ('1001261088') and
#                      b.user_id in (select user_id from bass2.dw_product_sprom_$op_month  
#                                     where sprom_id in (90008100,90008101,90008102,99001616,99001617,99001619,99001620,99001622) 
#                                           and plan_id in (93901001,90700001,94101001) and active_mark=1)
#                  group by
#                      b.enterprise_id,
#                      case when a.item_id in (80000200) then '2017'
#                           when a.item_id in (80000178,80000179) then '2022'
#                           when a.item_id in (80000448) then '2019'
#                      end;"        
# 	puts $sql_buff
#	exec_sql $sql_buff
#	puts "2017	MAS������ҵ������ 2022	�ƶ�CRM(MAS)ҵ������"	
















  #2017	MAS�������û���
	set sql_buff "insert into session.tmp22035 
                  select
                      ENTERPRISE_ID, 
                      '2017',
                      count(distinct user_id) as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   
                      bass2.dw_enterprise_membersub_$op_month
                  where order_id in(select order_id from bass2.dw_enterprise_sub_$op_month  
                                     where service_id='941' and rec_status=1) and 
                        rec_status=1 
                  group by
                      ENTERPRISE_ID;"        
 	puts $sql_buff
	exec_sql $sql_buff


  #2017�ƶ�CRM�û���
	set sql_buff "insert into session.tmp22035 
                  select
                      ENTERPRISE_ID, 
                      '2022',
                      count(distinct user_id) as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   
                      bass2.dw_enterprise_membersub_$op_month
                  where order_id in(select order_id from bass2.dw_enterprise_sub_$op_month  
                                     where service_id='142' and rec_status=1) and 
                        rec_status=1 
                  group by
                      ENTERPRISE_ID;"        
 	puts $sql_buff
	exec_sql $sql_buff


#                  select
#                      b.ENTERPRISE_ID, 
#                      case when a.prod_id = '94101001' then '2017'
#                           when a.prod_id = '90700001' then '2022'
#                           when a.prod_id = '93901001' then '2019'
#                      end,
#                      count(distinct b.user_id) as member_counts,
#                      0 as bill_duration,
#                      0 as info_number ,
#                      0 as z_convert_f ,
#                      0 as flow ,
#                      0 as bandwidth ,
#                      0 as netlines ,
#                      0 as ipvpn ,
#                      0 as server_num ,
#                      0 as virtualhost ,
#                      0 as vpmn_netin_income,
#                      0 as vpmn_netin_standard ,
#                      0 as z_vpmn_month_income ,
#                      0 as z_vpmn_standard ,
#                      0 as income 
#                  from   
#                      bass2.dw_enterprise_sub_$op_month a,
#                      bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
#                  where  
#                      a.prod_id in ('93901001','90700001','94101001') and 
#                      a.user_id in (select user_id from bass2.dw_product_sprom_$op_month 
#                                    where sprom_id in (90008100,90008101,90008102,99001616,99001617,99001619,99001620,99001622) 
#                                                                 and plan_id in (93901001,90700001,94101001) and active_mark=1) and 
#                      a.rec_status=1 and 
#                      a.acct_id=b.acct_id and 
#                      a.enterprise_id not in ('891910006274','891891000209')
#                  group by
#                      b.enterprise_id,
#                      case when a.prod_id = '94101001' then '2017'
#                           when a.prod_id = '90700001' then '2022'
#                           when a.prod_id = '93901001' then '2019'
#                      end;"        
# 	puts $sql_buff
#	exec_sql $sql_buff
  
  #�ƶ�OA(MAS)�û���
	set sql_buff "insert into session.tmp22035 
                  select
                      ENTERPRISE_ID, 
                      '2019',
                      count(distinct user_id) as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   
                      bass2.dw_enterprise_membersub_$op_month
                  where order_id in(select order_id from bass2.dw_enterprise_sub_$op_month  
                                     where user_id in (select  user_id from bass2.dw_product_sprom_$op_month  
                                                         where sprom_id in (99001610,99001611,99001612,99001619) and 
                                                                plan_id=93901001 and active_mark=1)                                 
                                           and prod_id='93901001' and rec_status=1) and 
                        rec_status=1 
                  group by
                      ENTERPRISE_ID;"        
 	puts $sql_buff
	exec_sql $sql_buff
  
















#951	����ADC����ͨ��Ʒ
#952	ҽ��ͨ(����ADC)
#953	ũ��ͨ(����ADC)
#910	����ͨ(ADC)
#909	ADC������
#906	�ƶ�CRM��ADC��
#904	�ƶ������棨ADC��
#903	�ƶ�OA��ADC��
#
#
#ADC������	909	����ͳ����Ʒ	BASS_STD1_0103	2024	ADC������
#�ƶ�OA��ADC��	903	����ͳ����Ʒ	BASS_STD1_0103	2026	�ƶ�OA��ADC��
#�ƶ������棨ADC��	904	����ͳ����Ʒ	BASS_STD1_0103	2027	�ƶ������棨ADC)
#�ƶ�CRM��ADC��	906	����ͳ����Ʒ	BASS_STD1_0103	2029	�ƶ�CRM��ADC��
#����ͨ	936	����ͳ����Ʒ	BASS_STD1_0103	2033	����ͨ��ADC)
#����ADC����ͨ��Ʒ	951	����ͳ����Ʒ	BASS_STD1_0103	2034	����ͨ��ADC)
#У��ͨҵ��	911	����ͳ����Ʒ	BASS_STD1_0103	2035	УѶͨ��ADC)
#����ͨ(ADC)	910	����ͳ����Ʒ	BASS_STD1_0103	2038	����ͨ��ADC��
#ADC��ҵ��
# �������涩�������û�������ADC������
#ADC���Ŷ�����
	set sql_buff "insert into session.tmp22035
	                select
                      b.ENTERPRISE_ID, 
                      case when b.service_id = '909' then '2024'
                           when b.service_id = '903' then '2026'
                           when b.service_id = '904' then '2027'
                           when b.service_id = '906' then '2029'
                           when b.service_id = '936' then '2033'
                           when b.service_id = '951' then '2034'
                           when b.service_id = '910' then '2038'
                           when b.service_id in ('924','952') then '2037'
                      end,
                      0 as user_count,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   bass2.dw_enterprise_sub_$op_month b
                  where b.rec_status=1 and 
                        b.service_id in ('951','936','910','909','906','904','903','924','952') and
                        b.ENTERPRISE_ID not in ('891910006274','891880005002')                        
                  group by
                      b.ENTERPRISE_ID,
                      case when b.service_id = '909' then '2024'
                           when b.service_id = '903' then '2026'
                           when b.service_id = '904' then '2027'
                           when b.service_id = '906' then '2029'
                           when b.service_id = '936' then '2033'
                           when b.service_id = '951' then '2034'
                           when b.service_id = '910' then '2038'
                           when b.service_id in ('924','952') then '2037'
                      end;"
 	puts $sql_buff
	exec_sql $sql_buff
	puts "ADC���Ŷ�����"	
                      
                      
                      
  #ADC�û���
	set sql_buff "insert into session.tmp22035 
                  select
                      a.ENTERPRISE_ID, 
                      case when b.service_id = '909' then '2024'
                           when b.service_id = '903' then '2026'
                           when b.service_id = '904' then '2027'
                           when b.service_id = '906' then '2029'
                           when b.service_id = '936' then '2033'
                           when b.service_id = '951' then '2034'
                           when b.service_id = '910' then '2038'
                           when b.service_id in ('924','952') then '2037'
                      end,
                      count(distinct a.user_id) as member_counts,
                      0 as bill_duration,
                      0 as info_number ,
                      0 as z_convert_f ,
                      0 as flow ,
                      0 as bandwidth ,
                      0 as netlines ,
                      0 as ipvpn ,
                      0 as server_num ,
                      0 as virtualhost ,
                      0 as vpmn_netin_income,
                      0 as vpmn_netin_standard ,
                      0 as z_vpmn_month_income ,
                      0 as z_vpmn_standard ,
                      0 as income 
                  from   
                      bass2.dw_enterprise_membersub_$op_month a,
					            bass2.dw_enterprise_sub_$op_month b
                  where a.order_id = b.order_id and 
                        b.rec_status=1 and 
                        a.rec_status=1 and 
                        b.service_id in ('951','936','910','909','906','904','903','924','952') and
                        a.ENTERPRISE_ID not in ('891910006274','891880005002')
                  group by
                      a.ENTERPRISE_ID,
                      case when b.service_id = '909' then '2024'
                           when b.service_id = '903' then '2026'
                           when b.service_id = '904' then '2027'
                           when b.service_id = '906' then '2029'
                           when b.service_id = '936' then '2033'
                           when b.service_id = '951' then '2034'
                           when b.service_id = '910' then '2038'
                           when b.service_id in ('924','952') then '2037'
                      end;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "ADC�û���"	
	
	



	#2026	�ƶ�OA��ADC��
	set sql_buff "insert into session.tmp22035 
                  select
                   c.enterprise_id, 
                   '2026', 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   bigint((sum(c.unipay_fee)+sum(c.non_unipay_fee))*100)
              from 
                   bass2.dw_enterprise_unipay_$op_month     c
             where 
                   c.service_id in ('903')  and c.test_mark = 0
          group by c.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ�OA��ADC��"	
	
	
	#2027	�ƶ������棨ADC)
	set sql_buff "insert into session.tmp22035 
                  select
                   c.enterprise_id, 
                   '2027', 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   bigint((sum(c.unipay_fee)+sum(c.non_unipay_fee))*100)
              from 
                   bass2.dw_enterprise_unipay_$op_month     c
             where 
                   c.service_id in ('904')  and c.test_mark = 0 
          group by c.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ������棨ADC)"	

	#2029	�ƶ�CRM��ADC��
	set sql_buff "insert into session.tmp22035 
                  select
                   c.enterprise_id, 
                   '2029', 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   bigint((sum(c.unipay_fee)+sum(c.non_unipay_fee))*100)
              from 
                   bass2.dw_enterprise_unipay_$op_month     c
             where 
                   c.service_id in ('906')  and c.test_mark = 0
          group by c.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�ƶ�CRM��ADC��"	




	#2038	����ͨ��ADC������
	set sql_buff "insert into session.tmp22035 
                  select
                   c.enterprise_id, 
                   '2038', 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   bigint((sum(c.unipay_fee)+sum(c.non_unipay_fee))*100)
              from 
                   bass2.dw_enterprise_unipay_$op_month     c
             where 
                   c.service_id in ('910')  and c.test_mark = 0
          group by c.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "	����ͨ��ADC��"	



	#2033	����ͨ��ADC������
	set sql_buff "insert into session.tmp22035 
                  select
                   c.enterprise_id, 
                   '2033', 
                   0 as member_counts,
                   0 as bill_duration,
                   0 as info_number ,
                   0 as z_convert_f ,
                   0 as flow ,
                   0 as bandwidth ,
                   0 as netlines ,
                   0 as ipvpn ,
                   0 as server_num ,
                   0 as virtualhost ,
                   0 as vpmn_netin_income,
                   0 as vpmn_netin_standard ,
                   0 as z_vpmn_month_income ,
                   0 as z_vpmn_standard ,
                   bigint((sum(c.unipay_fee)+sum(c.non_unipay_fee))*100)
              from 
                   bass2.dw_enterprise_unipay_$op_month     c
             where 
                   c.service_id in ('936')  and c.test_mark = 0
          group by c.enterprise_id;"        
 	puts $sql_buff
	exec_sql $sql_buff
	puts "	����ͨ��ADC��"	




            
                
                
	#�����ݲ�׼ȷ
	#3001   �������Ÿ�������   #dw_enterprise_unipay_yyyymm����service_id = '999'�ķ���
	set sql_buff "insert into session.tmp22035 
                  select
                    t.enterprise_id, 
                    t.standard_product, 
                    0 as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    t.income 
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,'3001' as standard_product
                     ,sum(a.unipay_fee + a.non_unipay_fee)*100 as income
                   from 
                     bass2.dw_enterprise_unipay_${op_month} a,
                     bass2.dw_enterprise_msg_${op_month} b
                   where 
                     a.service_id not in ('933','912','934','944','941','939','142','909','903','904','906',
                                          '936','951','911','910','942','946','947','949','918') and a.test_mark = 0 
                     and a.enterprise_id = b.enterprise_id
                   group by 
                     a.enterprise_id
                  )t"              
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�������Ÿ�������"
	
	
	
	
                                                                                                     
                                                                                                                                              

    #��ҵӦ�ÿ�2016 ����POS
    set sql_buff "insert into session.tmp22035
                   select
                    b.enterprise_id, 
                    '2016', 
                    count(*) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(fact_fee*100))
              from bass2.DW_ACCT_SHOULDITEM_$op_month a,
                   bass2.dw_enterprise_sub_$op_month b
             where a.acct_id in (select acct_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('944') and rec_status = 1 and enterprise_id not in ('891910006274') ) and a.acct_id = b.acct_id and rec_status = 1  
             group by b.enterprise_id "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "��ҵӦ�ÿ�2016"


    #�������ݿ�2016
    set sql_buff "insert into session.tmp22035
                   select
                    case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end as enterprise_id, 
                    '2016', 
                    count(distinct a.user_id) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(a.fact_fee)*100)
              from (select user_id,cust_id,acct_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_$op_month
               	   where item_id=80000104 
               	   group by user_id,cust_id,acct_id) a left join bass2.dw_enterprise_account_his_$op_month b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_$op_month c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_$op_month d on a.cust_id=d.cust_id,
               		                            bass2.DW_ENTERPRISE_MEMBERSUB_$op_month e 
               		                            where a.user_id=e.user_id and e.order_id in (
               		                            select order_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('947') and rec_status = 1 and enterprise_id not in ('891910006274') ) and e.rec_status=1
             group by case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end  "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�������ݿ�2016"

               		                            
    #�������ݿ�2016
    set sql_buff "insert into session.tmp22035
                   select
                    case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end as enterprise_id, 
                    '2016', 
                    count(distinct a.user_id) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(a.fact_fee)*100)
              from (select user_id,cust_id,acct_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_$op_month
               	   where item_id=80000104 
               	   group by user_id,cust_id,acct_id) a left join bass2.dw_enterprise_account_his_$op_month b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_$op_month c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_$op_month d on a.cust_id=d.cust_id,
               		                            bass2.DW_ENTERPRISE_MEMBERSUB_$op_month e 
               		                            where a.user_id=e.user_id and e.order_id  in (
               		                            select order_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('946') and rec_status = 1 and enterprise_id not in ('891910006274') ) and e.rec_status=1
             group by case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end  "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�������ݿ�2016"






    #�������ݿ�
    set sql_buff "insert into session.tmp22035
                   select
                    case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end as enterprise_id, 
                    '2016', 
                    count(distinct a.user_id) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(a.fact_fee)*100)
              from (select user_id,cust_id,acct_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_$op_month
               	   where item_id=80000104 
               	   group by user_id,cust_id,acct_id) a left join bass2.dw_enterprise_account_his_$op_month b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_$op_month c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_$op_month d on a.cust_id=d.cust_id,
               		                            bass2.DW_ENTERPRISE_MEMBERSUB_$op_month e 
               		                            where a.user_id=e.user_id and e.order_id  in (
               		                            select order_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('947') and rec_status = 1 and enterprise_id not in ('891910006274') ) and e.rec_status=1
             group by case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end  "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�������ݿ�"







    #ȫ������ͨ
    set sql_buff "insert into session.tmp22035
                   select
                    case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end as enterprise_id, 
                    '2016', 
                    count(distinct a.user_id) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(a.fact_fee)*100)
              from (select user_id,cust_id,acct_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_$op_month
               	   where item_id in (80000435,80000419)
               	   group by user_id,cust_id,acct_id) a left join bass2.dw_enterprise_account_his_$op_month b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_$op_month c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_$op_month d on a.cust_id=d.cust_id,
               		                            bass2.DW_ENTERPRISE_MEMBERSUB_$op_month e 
               		                            where a.user_id=e.user_id and e.order_id  in (
               		                            select order_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('942') and rec_status = 1 and enterprise_id not in ('891910006274') ) and e.rec_status=1
             group by case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end  "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "ȫ������ͨ"





    #�������ݿ�
    set sql_buff "insert into session.tmp22035
                   select
                    case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end as enterprise_id, 
                    '2016', 
                    count(distinct a.user_id) as member_counts,
                    0 as bill_duration,
                    0 as info_number ,
                    0 as z_convert_f ,
                    0 as flow ,
                    0 as bandwidth ,
                    0 as netlines ,
                    0 as ipvpn ,
                    0 as server_num ,
                    0 as virtualhost ,
                    0 as vpmn_netin_income,
                    0 as vpmn_netin_standard ,
                    0 as z_vpmn_month_income ,
                    0 as z_vpmn_standard ,
                    bigint(sum(a.fact_fee)*100)
              from (select user_id,cust_id,acct_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_$op_month
               	   where item_id=80000104 
               	   group by user_id,cust_id,acct_id) a left join bass2.dw_enterprise_account_his_$op_month b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_$op_month c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_$op_month d on a.cust_id=d.cust_id,
               		                            bass2.DW_ENTERPRISE_MEMBERSUB_$op_month e 
               		                            where a.user_id=e.user_id and e.order_id  in (
               		                            select order_id from bass2.DW_ENTERPRISE_SUB_$op_month where service_id in ('949') and rec_status = 1 and enterprise_id not in ('891910006274') ) and e.rec_status=1
             group by case when c.enterprise_id is not null then c.enterprise_id when d.enterprise_id is not null then d.enterprise_id
	            	   	when b.enterprise_id is not null then b.enterprise_id 
                           else '' end  "                 
 	puts $sql_buff
	exec_sql $sql_buff
	puts "�������ݿ�"











	
	#============����ʽ���в�������============================
	set sql_buff "insert into bass1.$objecttable 
                  select
                  ${op_month},
                  '${op_month}',
                  enterprise_id,
                  case when standard_product = '2001' then '2038' else standard_product end, 
                  char(member_counts      ),
                  char(bill_duration      ),
                  char(info_number        ),
                  char(z_convert_f        ),
                  char(flow               ),
                  char(bandwidth          ),
                  char(netlines           ),
                  char(ipvpn              ),
                  char(server_num         ),
                  char(virtualhost        ),
                  char(vpmn_netin_income  ),
                  char(vpmn_netin_standard),
                  char(z_vpmn_month_income),
                  char(z_vpmn_standard    ),
                  char(income             ),
                  '0',
                  '0',
                  '0',
                  '0'  
                  from
                  (
                   select
                     enterprise_id ,
                     standard_product, 
                     sum(member_counts       ) as member_counts       ,
                     sum(bill_duration       ) as bill_duration       ,
                     sum(info_number         ) as info_number         ,
                     sum(z_convert_f         ) as z_convert_f         ,
                     sum(flow                ) as flow                ,
                     sum(bandwidth           ) as bandwidth           ,
                     sum(netlines            ) as netlines            ,
                     sum(ipvpn               ) as ipvpn               ,
                     sum(server_num          ) as server_num          ,
                     sum(virtualhost         ) as virtualhost         ,
                     sum(vpmn_netin_income   ) as vpmn_netin_income   ,
                     sum(vpmn_netin_standard ) as vpmn_netin_standard ,
                     sum(z_vpmn_month_income ) as z_vpmn_month_income ,
                     sum(z_vpmn_standard     ) as z_vpmn_standard     ,
                     sum(income              ) as income                      
                                     
                  from 
                      session.tmp22035
                  where enterprise_id is not null and  rtrim(enterprise_id) <> ''
                  group by
                      enterprise_id , 
                      standard_product
                )aa"   
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����ʽ���в�������"
        



#���Ų���ҵ��	933	����ͳ����Ʒ	BASS_STD1_0103	2006	���Ų���
#����ר�߲�Ʒ����	912	����ͳ����Ʒ	BASS_STD1_0103	2008	������ר��
#��ҵ��e��ҵ��	934	����ͳ����Ʒ	BASS_STD1_0103	2011	��ҵ��E��
#����POS	944	����ͳ����Ʒ	BASS_STD1_0103	2016	��ҵӦ�ÿ�
#MAS������	941	����ͳ����Ʒ	BASS_STD1_0103	2017	MAS������
#MAS����(�����ƶ�MAS,�ֻ�����(MAS)��	939	����ͳ����Ʒ	BASS_STD1_0103	2018	�ֻ����䣨MAS)
#�ƶ�CRM��MAS��	142	����ͳ����Ʒ	BASS_STD1_0103	2022	�ƶ�CRM��MAS��
#ADC������	909	����ͳ����Ʒ	BASS_STD1_0103	2024	ADC������
#�ƶ�OA��ADC��	903	����ͳ����Ʒ	BASS_STD1_0103	2026	�ƶ�OA��ADC��
#�ƶ������棨ADC��	904	����ͳ����Ʒ	BASS_STD1_0103	2027	�ƶ������棨ADC)
#�ƶ�CRM��ADC��	906	����ͳ����Ʒ	BASS_STD1_0103	2029	�ƶ�CRM��ADC��
#����ͨ	936	����ͳ����Ʒ	BASS_STD1_0103	2033	����ͨ��ADC)
#����ADC����ͨ��Ʒ	951	����ͳ����Ʒ	BASS_STD1_0103	2034	����ͨ��ADC)
#У��ͨҵ��	911	����ͳ����Ʒ	BASS_STD1_0103	2035	УѶͨ��ADC)
#����ͨ(ADC)	910	����ͳ����Ʒ	BASS_STD1_0103	2038	����ͨ��ADC��
set sql_buff " insert into bass1.$objecttable 
               select     $op_month,'$op_month',
                               b.enterprise_id,
                               case when a.service_id = '903' then '2026'
                                    when a.service_id = '904' then '2027'
                                    when a.service_id = '906' then '2029'
                                    when a.service_id = '910' then '2038'
                                    when a.service_id = '936' then '2033'
                                    when a.service_id = '951' then '2034'
                               end,'0','0',
                               value(case when c.CALLTYPE_ID = 1 then char(sum(c.COUNTS))
                               end,'0'),'0','0','0','0','0','0','0','0','0','0','0','0',
                               value(case when c.CALLTYPE_ID = 0 then char(sum(c.COUNTS))
                               end,'0'),'0','0','0'
                        from   bass2.dw_enterprise_sub_$op_month a, 
                               bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b,
                               bass2.dw_newbusi_ismg_$op_month c
                       where   a.service_id in ('906','903','904','910','951') and a.enterprise_id not in ('891910005785','891910005863','891910006274','891880005002')
                               and a.rec_status=1 and a.acct_id=b.acct_id and b.user_id=c.user_id
                      group by b.enterprise_id,
                               case when a.service_id = '903' then '2026'
                                    when a.service_id = '904' then '2027'
                                    when a.service_id = '906' then '2029'
                                    when a.service_id = '910' then '2038'
                                    when a.service_id = '936' then '2033'
                                    when a.service_id = '951' then '2034'
                               end,
                               c.CALLTYPE_ID "                     
                              
 	puts $sql_buff
	exec_sql $sql_buff

    
#����ͨҵ��������
set sql_buff " insert into bass1.$objecttable 
               select     $op_month,'$op_month',
                               b.enterprise_id,
                               '2033','0','0',
                               value(char(sum(counts)),'0'),'0','0','0','0','0','0','0','0','0','0','0','0',
                               '0','0','0','0'
                        from   bass2.dw_newbusi_ismg_$op_month a,
                               bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                       where   sp_code in ('600000','901870','801160','900141') and    a.user_id = b.user_id and oper_code like  'AJT%' and calltype_id = 1
                      group by b.enterprise_id"                     
                              
 	puts $sql_buff
	exec_sql $sql_buff
	
#����ͨҵ��������
set sql_buff " insert into bass1.$objecttable 
               select     $op_month,'$op_month',
                               b.enterprise_id,
                               '2033','0','0',
                               '0','0','0','0','0','0','0','0','0','0','0','0','0',
                               value(char(sum(counts)),'0'),'0','0','0'
                        from   bass2.dw_newbusi_ismg_$op_month a,
                               bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                       where   sp_code in ('600000','901870','801160','900141') and    a.user_id = b.user_id and oper_code like  'AJT%'  and calltype_id = 0 
                      group by b.enterprise_id"                     
                              
 	puts $sql_buff
	exec_sql $sql_buff
	
	

#ͳһ����
    set sql_buff " update G_S_22035_MONTH set time_id = 888888 where time_id = $op_month"
    puts $sql_buff      
    exec_sql $sql_buff      

    set sql_buff " insert into G_S_22035_MONTH 
                    select $op_month,
                           '$op_month',
                           ENTERPRISE_ID,
                           ENT_PROD_ID,
                           char(sum(bigint(ENT_ARRIVES))),
                           char(sum(bigint(BILL_DURING))),
                           char(sum(bigint(DOWNMESSAGES))),
                           char(sum(bigint(FORWARDS))),
                           char(sum(bigint(DOWNFLOW))),
                           char(sum(bigint(BRND_WIDTH))),
                           char(sum(bigint(LINES))),
                           char(sum(bigint(PORTS))),
                           char(sum(bigint(SERVERS))),
                           char(sum(bigint(VIR_HOSTS))),
                           char(sum(bigint(VPMN_INCOME))),
                           char(sum(bigint(VPMN_FEE))),
                           char(sum(bigint(VPMN_INTEG_INC))),
                           char(sum(bigint(VPMN_INTEG_FEE))),
                           char(sum(bigint(TOTAL_FEE))),
                           char(sum(bigint(UPMESSAGES))),
                           char(sum(bigint(UPFLOW))),
                           char(sum(bigint(REVMAILNUM))),
                           char(sum(bigint(SENDMAILNUM)))
                      from G_S_22035_MONTH 
                     where time_id = 888888 
                     group by  ENTERPRISE_ID,ENT_PROD_ID"
    puts $sql_buff      
    exec_sql $sql_buff      

    set sql_buff " delete from  G_S_22035_MONTH where  time_id = 888888 "
    puts $sql_buff      
    exec_sql $sql_buff  
        
    #��ʱ����
    set sql_buff " update G_S_22035_MONTH set downmessages = '0'
                          where time_id = $op_month and ent_prod_id in ('2026','2027')"
    puts $sql_buff      
    exec_sql $sql_buff      
    
    #�޳����Լ���
    set sql_buff " delete from  G_S_22035_MONTH where time_id = $op_month and enterprise_id in ('891910006274')"
    puts $sql_buff      
    exec_sql $sql_buff      



	aidb_close $handle

	return 0
}




#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------


