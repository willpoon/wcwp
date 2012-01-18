#/*******************************************************************************
#	Դ��������Dw_product_ds_2.tcl
#	��Ҫ˵���������û������ձ�(�����)
#	�������̣�dwd_product_yyyymmdd:dw_call_dt:dw_call_yyyymmdd:dw_vip_cust_yyyymmdd:dw_vpmn_member_yyyymmdd:dw_enterprise_member_yyyymmdd:dw_product_no_press_stop_ds --> dw_product_yyyymmdd:dw_product_yyymm
#  ����    �����
#  ���з�����ds Dw_product_ds_2.tcl 2007-03-31 20070331
#  ��ע    ��'2006-02-19'�Ǹ������ڲ���Ҫ����
#          ��create_date --����ʱ�䣺sts_date --״̬ʱ�䣺valid_date --��Ч���ڣ�expire_date	--ʧЧ����
#          ��ʧЧ����ֻ��һ��ֵ=2030-01-01,�û�����ʱ����ʱ��=״̬ʱ��=��Ч����,�û�����ʱ״̬ʱ��=��Ч����,
#          �������װ�͹���ʱ����ʱ��=״̬ʱ��=��Ч����=�������װ/����������ʱ��,��user_id����仯��
#          ���û��ӱ����ڻ��߱���״̬�������������ʱ��,״̬ʱ��Ҳ��仯
#          ���û�������״̬�������ʷ״̬,״̬ʱ��Ҳ��仯
#          �����û��䴴��ʱ��Ҳ����Ϊ���»��գ�״̬�ı�ʱ��Ҳ����Ϊ���»��յ��䲻�������û���Ҳ���������û���ͨ��������ISNORMAL�������ӱ�
#          ����Ʒ��������Ʒ(�̿�����Ʒ�ͷǳ̿�����Ʒ(�������Ļ����ʷ�))�ʹ�����Ʒ(������Ʒ�ʷѵ��ض���,Ŀǰ�޳̿ش�����Ʒ)
#          ���̿�����Ʒ��select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=2
#          ���ǳ̿�����Ʒ��select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=1 and a.is_prom=0
#          ��������Ʒ(�ǳ̿أ���select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=1 and a.is_prom=1
#          ��������Ʒ(�̿أ���select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=2 and a.is_prom=1
#          ��plan_id�Ƕ��ַǳ̿�����Ʒ�ļ��ϣ���������������Ʒ����������Ʒ�ȣ�һ��plan_idֻ��һ�ַ����һ�ַǳ̿�����Ʒ
#�޸ļ�¼��1.2010-05-20 By fuzl  ���� user_id ���ֶγ��� 
#*******************************************************************************/
proc deal {p_optime p_timestamp} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg  1000
	  	return -1
	}

	if { [exec_sql $p_optime $p_timestamp] != 0 } {
		aidb_close $handle
		aidb_roll $conn
		return -1
	}

	return 0
}

proc exec_sql {p_optime p_timestamp} {

	global env

	global conn

	global handle
	source  $env(AITOOLSPATH)/bin/base_bass.tcl
	set op_time $p_optime

	set timestamp $p_timestamp

	set db_user $env(DB_USER)
	
	set tbs_3h $env(TBS_3H)
	
	set optmonth [string range $p_timestamp 0 5]
	
	set optmonth_01 [string range $p_optime 0 7]01
	
	set lastmonth [GetLastMonth [string range $p_timestamp 0 5]]
	
	set nextday [ai_adddays $handle ${p_optime} 1]
	
	set handle [aidb_open $conn]
	
	set lastday [GetLastDay $p_timestamp]
	
	set last_day_month [GetLastDay [GetNextMonth [string range $p_timestamp 0 5]]01]
	
	set optday [string range $p_timestamp 6 7]
	
	#	M1��ɾ�������Ѵ��ڵ����ݱ�
	
	set sqlbuf "drop table ${db_user}.dw_product_${p_timestamp}_2;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		#trace_sql $errmsg 1001
	}
	
	aidb_commit $conn
	
	#	M2�������������ݱ�
	
   set sqlbuf "create table ${db_user}.dw_product_${p_timestamp}_2 like ${db_user}.dw_product_yyyymmdd_2 in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially;"
   
   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1002
		return -1
	}

   aidb_commit $conn
   
	#	M3��������ʱ��1
	
	set sqlbuf "declare global temporary table session.t_dw_product_1
               (
               	user_id              varchar(20),
               	day_new_mark         smallint,
   	            day_off_mark         smallint,
   	            day_recreate_mark    smallint
               )
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
	
	#	M4�������������,����������װ��־������ʱ��
	
	set sqlbuf "insert into session.t_dw_product_1
	            (user_id,day_new_mark,day_off_mark,day_recreate_mark)
	            select a.user_id,
	                   case when a.usertype_id in (1,2,9) and b.user_id is null and a.userstatus_id in (1,2,3,6,8) and l.user_id is null then 1 else 0 end as day_new_mark,
                      case when a.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and a.userstatus_id not in (1,2,3,6,8) and l.user_id is null then 1 else 0 end as day_off_mark,
                      case when a.usertype_id in (1,2,9) and b.userstatus_id not in (1,2,3,6,8) and a.userstatus_id in (1,2,3,6,8) and l.user_id is null then 1 else 0 end as day_recreate_mark
               from ${db_user}.dw_product_${p_timestamp}_1 a left join ${db_user}.dw_product_${lastday} b on a.user_id=b.user_id
               		left join (select user_id from ${db_user}.dw_product_test_phone_ds 
                                                                                  where op_time=date('${p_optime}') and sts=1 and char(date(valid_date),iso)<'${nextday}' and
                                                                                        char(date(expire_date),iso)>='${nextday}'
                                                                                  group by user_id) l on a.user_id=l.user_id"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1004
		return -1
	}
	
	aidb_commit $conn
	
	#	M5��������
	
	set sqlbuf "create index session.idx_product_1_1 on session.t_dw_product_1(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1005
		return -1
	}

	aidb_commit $conn
	
	#	M6��������ʱ��2
	
	set sqlbuf "declare global temporary table session.t_dw_product_2
               (
               	user_id              varchar(20),
               	day_new_mark         smallint,
   	            day_off_mark         smallint,
   	            day_recreate_mark    smallint,
               	month_new_count      smallint,
   	            month_off_count      smallint,
   	            month_recreate_count smallint
               )
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1006
		return -1
	}
	
	aidb_commit $conn
	
	if { $optday == 01 } {
	
		#	M7�������µ�����,����������װ��������ʱ��
		
		set sqlbuf "insert into session.t_dw_product_2
		            (user_id             ,day_new_mark,day_off_mark,day_recreate_mark,month_new_count,month_off_count,
		             month_recreate_count)
		            select user_id             ,day_new_mark,day_off_mark,day_recreate_mark,
		                   day_new_mark as month_new_count,
		                   day_off_mark as month_off_count,
		                   day_recreate_mark as month_recreate_count
	               from session.t_dw_product_1;"
		
	  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	  		puts $errmsg
			trace_sql $errmsg 1007
			return -1
		}
		
		aidb_commit $conn	
		
	} else {
	
		#	M8�������µ�����,����������װ��������ʱ��
		
		set sqlbuf "insert into session.t_dw_product_2
		            (user_id             ,day_new_mark,day_off_mark,day_recreate_mark,month_new_count,month_off_count,
		             month_recreate_count)
		            select a.user_id             ,a.day_new_mark,a.day_off_mark,a.day_recreate_mark,
		                   a.day_new_mark+coalesce(b.month_new_count,0) as month_new_count,
		                   a.day_off_mark+coalesce(b.month_off_count,0) as month_off_count,
		                   a.day_recreate_mark+coalesce(b.month_recreate_count,0) as month_recreate_count
	               from session.t_dw_product_1 a left join ${db_user}.dw_product_${lastday} b on a.user_id=b.user_id;"
		
	  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	  		puts $errmsg
			trace_sql $errmsg 1008
			return -1
		}
		
		aidb_commit $conn		
	}
	
	#	M9��������
	
	set sqlbuf "create index session.idx_product_2_1 on session.t_dw_product_2(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1009
		return -1
	}

	aidb_commit $conn
	
	#	M10�������������ҵ���¼������ʱ��
		
	set sqlbuf "insert into ${db_user}.dw_product_${p_timestamp}_2
               (user_id        ,cust_id             ,acct_id       ,age           ,age_id             ,sex_id         ,
                education_id   ,custtype_id         ,custclass_id  ,credit        ,credit_id          ,product_no     ,
                imsi           ,sim_id              ,sim_type      ,city_id       ,county_id          ,channel_id     ,
                channeltype_id ,warn_fee            ,so_nbr        ,accttype_id   ,user_online        ,user_online_id ,
                brand_id       ,crm_brand_id1       ,crm_brand_id2 ,crm_brand_id3 ,plan_id            ,userstatus_id  ,
                stopstatus_id  ,usertype_id         ,month_new_mark,month_off_mark,month_recreate_mark,month_new_count,
                month_off_count,month_recreate_count,day_new_mark  ,day_off_mark  ,day_recreate_mark  ,vip_mark       ,
                single_mark    ,create_date         ,valid_date    ,sts_date      ,expire_date)
	            select a.user_id        ,a.cust_id             ,a.acct_id             ,a.age           ,a.age_id             ,a.sex_id           ,
                      a.education_id   ,a.custtype_id         ,a.custclass_id        ,a.credit        ,a.credit_id          ,a.product_no       ,
                      a.imsi           ,a.sim_id              ,a.sim_type            ,a.city_id       ,a.county_id          ,a.channel_id       ,
                      a.channeltype_id ,a.warn_fee            ,a.so_nbr              ,a.accttype_id   ,a.user_online        ,a.user_online_id   ,
                      a.brand_id       ,a.crm_brand_id1       ,a.crm_brand_id2       ,a.crm_brand_id3 ,a.plan_id            ,a.userstatus_id    ,
                      a.stopstatus_id  ,a.usertype_id         ,
                      case when b.month_new_count>0 then 1 else 0 end as month_new_mark,
                      case when (b.month_off_count-b.month_recreate_count)>0 then 1 else 0 end as month_off_mark,
                      case when (b.month_recreate_count-b.month_off_count)>0 then 1 else 0 end as month_recreate_mark,
                      b.month_new_count,b.month_off_count     ,b.month_recreate_count,b.day_new_mark  ,b.day_off_mark       ,b.day_recreate_mark,
                      a.vip_mark       ,a.single_mark         ,a.create_date         ,a.valid_date    ,a.sts_date           ,a.expire_date
               from ${db_user}.dw_product_${p_timestamp}_1 a,session.t_dw_product_2 b
               where a.user_id=b.user_id;"
	
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1010
		return -1
	}
	
	aidb_commit $conn
	
	#	M11��������
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_2_1 on ${db_user}.dw_product_${p_timestamp}_2(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	#	M11��������
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_2_2 on ${db_user}.dw_product_${p_timestamp}_2(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	aidb_close $handle
	
	#	��runstats��
	aidb_runstats ${db_user}.dw_product_${p_timestamp}_2 1	
	
   return 0
}