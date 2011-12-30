######################################################################################################
#�ӿ����ƣ����ſͻ���ͳ������
#�ӿڱ��룺03018
#�ӿ�˵������¼�ɼ��ſͻ�ͳһ���ѵĸ������룬�������������ڼ��ſͻ��˵�������һ���Է�������֮�ͣ�
#          ��Ҫ��ʡ������ϵͳ�����߽��кϲ����ϴ���
#��������: G_S_03018_MONTH.tcl
#��������: ����03018������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-07-02
#�����¼������ԭ��03014��ҵ��ͳ�ƿھ�
#          20090727 �޸�82��152�д��� ���û����͹���
#          2011-04-14 ����and a.service_id <> '926' ���޳��ֻ��������롣
#					 2011-05-26 21:41:41 ���� and exists (select 1 from (select distinct user_id bass1.g_a_02004_day )u where a.user_id = u.user_id)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_03018_MONTH"
        #set db_user $env(DB_USER)

        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

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

#01	�û���ʶ	�μ��ӿڵ�Ԫ���û����û���ʶ��	char(20)
#02	����ҵ������	�μ�ά��ָ��˵���е�BASS_STD1_0108��	char(4)
#03 Ӧ�ù���ģʽ 1-����mas���� 2-����adc���� 3-����	char(1)
#04	���˷Ƿ�ͳ����Ŀ��Ŀ����	0-ͨ�ŷ� 1-���ܷ� 2-���� 	char(1)
#05	Ӧ�ս��	�Żݺ��Ӧ�ս���λ:�� 	number(12)



 #����G_S_03018_MONTH��session������session.tmp03018
	set sql_buff "declare global temporary table session.tmp03018
	        (
	                user_id                char(20)   not null,
                  ent_busi_id            char(4)    not null,
                  manage_mod             char(1)    not null,
                  acct_type              char(1)    not null,
                  income                 bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
  puts $sql_buff
  exec_sql $sql_buff
	puts "����G_S_03018_MONTH��session������session.tmp03018"


  #���ɼ��Ų�Ʒ��ͳ������
  #2011-04-14 ����and a.service_id <> '926' ���޳��ֻ��������롣
	set sql_buff "insert into session.tmp03018
               	select  b.user_id       as user_id
                       ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')     as ent_busi_id
                       ,case
												  when d.BASS1_VALUE='1520' then '3'
													when upper(c.config_value)='MAS' then '1'
													when upper(c.config_value)='ADC' then '2'
													else '3'
											  end             as manage_mod
                       ,'1'             as acct_type
               	       ,sum(bigint(non_unipay_fee))  as income
                  from bass2.dw_enterprise_unipay_$op_month a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b,
                       (select * from bass2.dim_service_config where config_id=1000027)  c,
				               (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') d
                 where a.acct_id = b.acct_id 
                   and a.service_id =c.service_id
                   and a.service_id = d.xzbas_value
                   and a.test_mark = 0 
                   and a.service_id not in ( '926','717')
                 group by b.user_id,
                 coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002'),
                 case
									when d.BASS1_VALUE='1520' then '3'
									when upper(c.config_value)='MAS' then '1'
									when upper(c.config_value)='ADC' then '2'
									else '3'
							   end"
  puts $sql_buff
  exec_sql $sql_buff
	puts "��������ͨ��ͳ������(����)"

  #2011-05-26 21:41:41 ����                 			 and exists (select 1 from (select distinct user_id bass1.g_a_02004_day )u where a.user_id = u.user_id)

 #���ɲ���ͨ1300��УѶͨ1310������ͨ1380��ũ��ͨ1320����
 set sql_buff "insert into session.tmp03018
               	select  a.user_id   as user_id
                       ,case when apptype_id =1 then  '1310'
                       			 when apptype_id =2 then  '1380'
                       			 when apptype_id =3 then  '1320'
                       			 when apptype_id =4 then  '1300'
                         end as ent_busi_id
                       ,case when apptype_id =3 then '2'
                       			 when apptype_id =4 then '1'
                       	else '3' end as manage_mod
                       ,'1'         as acct_type
               	       ,sum(fee)    as income
                  from bass2.dw_enterprise_industry_apply  a
                 where a.apptype_id in (1,2,3,4) 
		 and a.op_time = '$this_month_firstday' 
                 and a.user_id is not null 
                 and exists (select 1 from (select distinct user_id from bass1.g_a_02004_day )u where a.user_id = u.user_id)
                 group by a.user_id,
		                 case when apptype_id =1 then  '1310'
		               			  when apptype_id =2 then  '1380'
		               			  when apptype_id =3 then  '1320'
		               			  when apptype_id =4 then  '1300'
		                  end,
		                 case when apptype_id =3 then '2'
		                 			when apptype_id =4 then '1'
		                 	    else '3' 
		                 	end"

  exec_sql $sql_buff

#20110805 ���� 1230 blackberry ��ͳ��
set sql_buff "insert into session.tmp03018
               	select 
		 USER_ID
		 ,'1230' ENT_BUSI_ID
		 ,'3' MANAGE_MOD
		 ,'1' acct_type
		 ,sum(bigint(non_unipay_fee))  as income
		 from  bass2.dw_enterprise_new_unipay_$op_month a
		 ,(select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b
		 where a.test_mark = 0
		 and a.acct_id = b.acct_id
		 and a.ITEM_ID in  (80000180,80000184,80000598,80000599,80000599,82000111)
		 group by USER_ID
		 with ur
		"

  exec_sql $sql_buff



	#============����ʽ���в�������============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03018_MONTH
                  select
                    ${op_month},
                    aa.user_id,
                    aa.ent_busi_id,
                    aa.manage_mod,
                    aa.acct_type,
                    char(aa.income)
                  from
                  (
                   select
                     a.user_id         as user_id ,
                     a.ent_busi_id     as ent_busi_id,
                     a.manage_mod      as manage_mod,
                     a.acct_type       as acct_type,
                     sum(a.income*100) as income
                  from
                      session.tmp03018 a
                    , bass2.dw_product_$op_month b
                  where a.user_id=b.user_id
                  		and a.ent_busi_id  <> '4002'
                  		and b.usertype_id in (1,2,9)
                  group by
                      a.user_id ,
                      a.ent_busi_id,
                      a.manage_mod,
                      a.acct_type
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

