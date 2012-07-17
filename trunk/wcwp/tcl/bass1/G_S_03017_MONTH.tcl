######################################################################################################
#�ӿ����ƣ����ſͻ�ͳ������
#�ӿڱ��룺03017
#�ӿ�˵������¼�ɼ��ſͻ�ͳһ���ѵĸ������룬�������������ڼ��ſͻ��˵�������һ���Է�������֮�ͣ�
#          ��Ҫ��ʡ������ϵͳ�����߽��кϲ����ϴ���
#��������: G_S_03017_MONTH.tcl
#��������: ����03017������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-07-02
#�����¼�� 20100907 ���������̻�ͳ������ͳ��
#           20101008 liuzhilong ICTϵͳ���ɷ�service_id = '966' ��������ͳ��
#           20101116 liuzhilong ��������绰�ķ���ԭΪ022 ��Ϊ024
#           2011-04-14  a.service_id not in ('936','966') �ĳ� a.service_id not in ('936','966','926'),�޳��ֻ���������
#           2011-05-27 15:12:07	add : and exists (select 1 from (select distinct enterprise_id from bass1.G_A_01004_DAY )u where a.enterprise_id = u.enterprise_id)
#           20110805 �޸� 1520 / 1230  �ƶ�400 / blackberry
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set optime_month 2012-05
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_03017_MONTH"
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

  #����G_S_03017_MONTH��session������session.tmp03017
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.tmp03017
	        (
	          enterprise_id         char(20)   not null,
            ent_busi_id       		char(4)    not null,
            manage_mod       			char(1)    not null,
            account_item          varchar(3) not null,
            income                bigint
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
	puts "����G_S_03017_MONTH��session������session.tmp03017"

  #������Ŀ��ͳ�Ʊ� ����session.t_dw_enterprise_unipay  ����ͳ��ict���ɷ�
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.t_dw_enterprise_unipay
			( ENTERPRISE_ID  VARCHAR(20),
			  ACCT_ID        VARCHAR(20),
			  ITEM_ID        INTEGER,
			  SPECIAL_MARK   SMALLINT,
			  SERVICE_ID     VARCHAR(20),
			  UNIPAY_FEE     DECIMAL(9, 2),
			  NON_UNIPAY_FEE DECIMAL(9, 2) )
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

	# ����ICTϵͳ���ɷ� ����Ŀͳ����
	set handle [aidb_open $conn]
	set sql_buff " insert into session.t_dw_enterprise_unipay
	            ( enterprise_id ,
	            	acct_id,
	            	ITEM_ID,
	            	special_mark,
	            	service_id ,
	            	unipay_fee,
	             	non_unipay_fee)
		         select case
		         		when b.enterprise_id is not null then b.enterprise_id
		         		when c.enterprise_id is not null then c.enterprise_id
		         		when d.enterprise_id is not null then d.enterprise_id
                           else '' end as enterprise_id,
                      a.acct_id,
                      a.item_id,
                      case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end as special_mark,
                      '966'    ,
               	    sum(case when b.acct_id is not null then a.fact_fee else 0 end) as unipay_fee,
               	    sum(case when b.acct_id is null then a.fact_fee else 0 end) as non_unipay_fee
               from (select cust_id,acct_id,item_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_${op_month}
               	   where item_id in (80000618,80000619)
               	   group by cust_id,acct_id,item_id) a left join bass2.dw_enterprise_account_his_${op_month} b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_${op_month} c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_${op_month} d on a.cust_id=d.cust_id
               group by case
               			when b.enterprise_id is not null then b.enterprise_id
               			when c.enterprise_id is not null then c.enterprise_id
               			when d.enterprise_id is not null then d.enterprise_id
                             else '' end,
                        a.acct_id,
                        a.item_id,
                        case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end
                        "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


###   925	��ҵ���䣨ADC��	ADC
###   926	�ֻ����䣨ADC��	ADC
###   910	����ͨ(ADC)	ADC
###   953	ũ��ͨ(����ADC)	ADC
###   924	��ҵ��վ��ADC��	ADC
###   952	ҽ��ͨ(����ADC)	ADC
###   903	�ƶ�OA��ADC��	ADC
###   904	�ƶ������棨ADC��	ADC
###   906	�ƶ�CRM��ADC��	ADC
###   909	ADC������	ADC
###   939	MAS����(�����ƶ�MAS,�ֻ�����(MAS)��������վ(MAS))	MAS
###   936	����ͨ	MAS
###   935	B-MAS�ƻ�	MAS
###   951	����ADC����ͨ��Ʒ	MAS
###   142	�ƶ�CRM��MAS��	MAS
# case
#     when service_id in ('142','935','936','939','951') then '1'
#     when service_id in ('903','904','906','909','910','924','925','926','952','953') then '2'
# else '3' end
#

	#��ͨVPMN����ͳһ��������(����Ϊ������������)
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                  select
                  enterprise_id,
                  ent_busi_id,
                  manage_mod,
                  account_item,
                  income
                  from
                  (
                   select
                      c.enterprise_id
                      ,'1000' as ent_busi_id
                      ,'3'    as manage_mod
                      ,e.account_item_id  as account_item
                      ,sum(a.fact_fee)*100 as income
                   from
                      bass2.dw_acct_shoulditem_${op_month} a,
                      bass2.dw_enterprise_account_${op_month} b,
                      bass2.dw_enterprise_member_mid_${op_month} c,
                      bass2.dim_feetype_item d,
                      bass1.map_feetype_account_item e
                   where c.free_mark =0
                     and c.test_mark = 0
                     and a.user_id = c.user_id
                     and a.acct_id = b.acct_id
                     and a.item_id = d.item_id
                     and d.feetype_id = e.feetype_id
                     and b.rec_status = 1
                     and a.item_id not in (80000027,80000032,80000033,80000101)
                   group by
                      c.enterprise_id,e.account_item_id
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


	#һ���Է������룬����Ҫ���ϸһ�㣬��Ҫ����ȷ�Ͽھ�
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                  select
                    enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id
                     ,'4002' as ent_busi_id
                     ,'3' as manage_mod
                     ,'101' as account_item
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
	# 20101008 ICTϵͳ���ɷ�service_id = '966' ��������ͳ��
	#2011-04-14  a.service_id not in ('936','966') �ĳ� a.service_id not in ('936','966','926'),�޳��ֻ���������
	#2011-08-05 �޳� 1230 blackberry �����롣�ں��������ͳ����ȡ��
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                  select
                    enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when C.BASS1_VALUE='1520' then '3'
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  bass2.dw_enterprise_unipay_${op_month} a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.test_mark = 0
                     and a.service_id not in ('936','966','926','717','931') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when C.BASS1_VALUE='1520' then '3'                     
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090')
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

#ICTϵͳ���ɷ�service_id = '966'
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                  select
                    enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when C.BASS1_VALUE='1520' then '3'
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,case when a.item_id=80000618 then '105'
                     			 when a.item_id=80000619 then '120'
                     	 		 else  '090' 
                     	end  as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  session.t_dw_enterprise_unipay a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.enterprise_id not in ('89102999670396','89103000041929')
                     and a.service_id  in ('966') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when C.BASS1_VALUE='1520' then '3'
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,case when a.item_id=80000618 then '105'
                     			 when a.item_id=80000619 then '120'
                     	 		 else  '090' 
                      end
                  )t
									 "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "ICTϵͳ���ɷ�"


#����ͨ����ͳ��
	#����ͳ������
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                  select
                    case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end,
                    '1290' as ent_busi_id,
                    '1'    as manage_mod,
                    '060'  as account_item,
                    sum(a.unipay_fee*100) as fact_fee
            from bass2.DW_ENTERPRISE_UNIPAY_$op_month a
							   left outer join
                 bass2.DW_ENTERPRISE_MSG_$op_month b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
							   left join
							   bass2.dw_enterprise_account_his_$op_month c on a.acct_id=c.acct_id
           where a.enterprise_id not in ('891910006274','891950005002') and
                 a.service_id in ('936') and a.TEST_MARK = 0 and
                 case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end is not null
        group by case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end"
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
	set sql_buff "insert into session.tmp03017
                    select
                    c.enterprise_id,
                    '1360' as ent_busi_id,
                    '3'    as manage_mod,
                    '010'  as account_item,
                    sum(bigint(b.fee*100)) as income
               from bass2.dw_enterprise_industry_apply b
               left outer join (select distinct enterprise_id,user_id
			                            from bass2.dw_enterprise_membersub_$op_month ) c
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



#�����̻�ͳ������
#           20101116 liuzhilong ��������绰�ķ���ԭΪ022 ��Ϊ024
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03017
                 select
									  a.ENTERPRISE_ID,
									  '1040' ent_busi_id,
									  '3',
									  '024',
									  sum(d.fact_fee*100)
									from bass2.dw_enterprise_member_mid_$op_month a,
									     bass2.dw_enterprise_msg_$op_month b,
									     bass2.dw_wireless_phone_$op_month c ,
									     bass2.dw_product_$op_month d
									where a.enterprise_id = b.enterprise_id
									  and a.dummy_mark = 0
									  and a.user_id=c.user_id
									  and c.user_id=d.user_id
									  and c.user_flag=2
									  and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
									group by a.ENTERPRISE_ID
									 "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ͳ������"

##2011-08-05  1230 blackberry
	set sql_buff "
	insert into session.tmp03017
	select
	a.enterprise_id as enterprise_id
	,'1230' ent_busi_id
	,'3' manage_mod
	,case 
	  when a.ITEM_ID in (80000180,80000598) then  '060' 
	  when a.ITEM_ID in (80000184,80000599) then  '024' 
	  else '090' end account_item
	,sum(a.unipay_fee)*100 as income
       from  bass2.dw_enterprise_new_unipay_$op_month a
	where a.test_mark = 0
	and a.ITEM_ID in  (80000180,80000184,80000598,80000599,80000599,82000111)
	group by
	a.enterprise_id
	,case 
	  when a.ITEM_ID in (80000180,80000598) then  '060' 
	  when a.ITEM_ID in (80000184,80000599) then  '024' 
	  else '090' end
	with ur
	"
	exec_sql $sql_buff




##2012-06-29  1180 ����ר�� for R292 У��
	set sql_buff "
	insert into session.tmp03017
	select
	a.enterprise_id as enterprise_id
	,'1180' ent_busi_id
	,'3' manage_mod
	,'030'  account_item
	,sum(a.unipay_fee)*100 as income
       from  bass2.dw_enterprise_new_unipay_$op_month a
	where a.test_mark = 0
	and a.service_id in  (912002)
	group by
	a.enterprise_id
	,'030'
	with ur
	"
	exec_sql $sql_buff



##2011-08-05  1230 400ҵ��
	set sql_buff "
	insert into session.tmp03017
	select
	a.enterprise_id as enterprise_id
	,'1520' ent_busi_id
	,'3' manage_mod
	,case 
		when a.ITEM_ID in (80000518,80000661) then '010'
		when a.ITEM_ID in (80000517) then '021'
		when a.ITEM_ID in (80000549) then '022'
		when a.ITEM_ID in (80000550) then '023'
		when a.ITEM_ID in (80000519,80000713) then '060'
		else '090' end  account_item
	,sum(a.unipay_fee)*100 as income
       from  bass2.dw_enterprise_new_unipay_$op_month a
	where a.test_mark = 0
	and a.ITEM_ID in  (80000518,80000661,80000517,80000549,80000550,80000519,80000713,80000540,80000657,80000658,80000659,80000660)
	group by
	a.enterprise_id
	,case 
		when a.ITEM_ID in (80000518,80000661) then '010'
		when a.ITEM_ID in (80000517) then '021'
		when a.ITEM_ID in (80000549) then '022'
		when a.ITEM_ID in (80000550) then '023'
		when a.ITEM_ID in (80000519,80000713) then '060'
		else '090' end
	with ur
	"
	exec_sql $sql_buff




	#============����ʽ���в�������============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.$objecttable
                  select
                    ${op_month},
                    enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item,
                    char(income)
                  from
                  (
                   select
                     enterprise_id ,
                     ent_busi_id,
                     manage_mod,
                     account_item,
                     sum(income) as income
                  from
                      session.tmp03017 a
                  where enterprise_id is not null and  enterprise_id <> ''
                  		and exists (select 1 from (select distinct enterprise_id from bass1.G_A_01004_DAY where time_id / 100 <= $op_month )u where a.enterprise_id = u.enterprise_id)
                  group by
                      enterprise_id ,
                      ent_busi_id,
                      manage_mod,
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

	return 0
}


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