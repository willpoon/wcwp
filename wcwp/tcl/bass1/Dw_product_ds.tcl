#/*******************************************************************************
#	源程序名：Dw_product_ds.tcl
#	简要说明：生成用户资料日表
#	程序流程：dwd_product_yyyymmdd:dw_call_dt:dw_call_yyyymmdd:dw_vip_cust_yyyymmdd:dw_vpmn_member_yyyymmdd:dw_enterprise_member_yyyymmdd:dw_product_no_press_stop_ds --> dw_product_yyyymmdd:dw_product_yyymm
#  作者    ：杨菲
#  运行方法：ds Dw_product_ds.tcl 2007-03-31 20070331
#  备注    ：'2006-02-19'是个死日期不需要更改
#          ：create_date --创建时间：sts_date --状态时间：valid_date --生效日期：expire_date	--失效日期
#          ：失效日期只有一个值=2030-01-01,用户开户时创建时间=状态时间=生效日期,用户销户时状态时间=生效日期,
#          ：拆机复装和过户时创建时间=状态时间=生效日期=（拆机复装/过户）受理时间,但user_id不会变化。
#          ：用户从保留期或者保号状态变更到离网销户时间,状态时间也会变化
#          ：用户从销户状态变更到历史状态,状态时间也会变化
#          ：拆单用户其创建时间也可以为当月或当日，状态改变时间也可以为当月或当日但其不是新增用户，也不是离网用户，通过工单的ISNORMAL进行区加别
#          ：产品包括主产品(程控主产品和非程控主产品(代表服务的基本资费))和促销产品(对主产品资费的重定义,目前无程控促销产品)
#          ：程控主产品：select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=2
#          ：非程控主产品：select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=1 and a.is_prom=0
#          ：促销产品(非程控）：select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=1 and a.is_prom=1
#          ：促销产品(程控）：select * from dim_product_item a,dim_service_item b where a.service_id=b.service_id and b.service_type_id=2 and a.is_prom=1
#          ：plan_id是多种非程控主产品的集合，包括了语音主产品，短信主产品等，一种plan_id只有一种服务的一种非程控主产品
#	   : modify by zhaolp2 增加user_id唯一性判定,出现user_id重复时程序退出并告警
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
	
	#	M1、删除当日已存在的数据表
	
	set sqlbuf "drop table ${db_user}.dw_product_$p_timestamp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		#trace_sql $errmsg 1001
	}
	
	aidb_commit $conn
	
	#	M2、创建当日数据表
	
   set sqlbuf "create table ${db_user}.dw_product_$p_timestamp like ${db_user}.dw_product_yyyymmdd in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially;"
   
   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1002
		return -1
	}

   aidb_commit $conn
   
   #	M3、建立临时表1
	
	set sqlbuf "declare global temporary table session.t_dw_product_1
               (
               	user_id              varchar(20)
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
	
	#	M4、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_1(user_id)
	            select user_id
	            from (select user_id
                     from ${db_user}.dw_newbusi_call_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_sms_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_ismg_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wap_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_gprs_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_mms_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wlan_$p_timestamp
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_kj_$p_timestamp) a
               group by user_id;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1004
		return -1
	}
	
	aidb_commit $conn
	
	#	M5、建索引
	
	set sqlbuf "create index session.idx_product_1_1 on session.t_dw_product_1(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1005
		return -1
	}
	
	aidb_commit $conn
	
	#	M6、建立临时表2
	
	set sqlbuf "declare global temporary table session.t_dw_product_2
               (
               	user_id              varchar(20),
               	call_counts          integer,
               	call_duration        integer,
               	call_duration_m      integer,
                  call_duration_s      integer,
                  basecall_fee         decimal(9,2),
                  toll_fee             decimal(9,2),
                  info_fee             decimal(9,2),
                  other_fee            decimal(9,2)
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1006
		return -1
	}
	
	aidb_commit $conn
	
	#	M7、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_2
	            (user_id ,call_counts,call_duration,call_duration_m,call_duration_s,basecall_fee,
                toll_fee,info_fee   ,other_fee)
	            select user_id,
                      sum(call_counts) as call_counts,
                      sum(call_duration) as call_duration,
                      sum(call_duration_m) as call_duration_m,
                      sum(call_duration_s) as call_duration_s,
                      sum(basecall_fee) as basecall_fee,
                      sum(toll_fee) as toll_fee,
                      sum(info_fee) as info_fee,
                      sum(other_fee) as other_fee
               from ${db_user}.dw_call_$p_timestamp
               group by user_id;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1007
		return -1
	}
	
	aidb_commit $conn
	
	#	M8、建索引
	
	set sqlbuf "create index session.idx_product_2_1 on session.t_dw_product_2(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1008
		return -1
	}

	aidb_commit $conn
	
	#	M9、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_3
               (
               	user_id              varchar(20),
               	call_counts          integer,
               	call_duration        integer,
               	call_duration_m      integer,
                  call_duration_s      integer,
                  basecall_fee         decimal(9,2),
                  toll_fee             decimal(9,2),
                  info_fee             decimal(9,2),
                  other_fee            decimal(9,2)
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1009
		return -1
	}
	
	aidb_commit $conn
	
	#	M10、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_3
	            (user_id ,call_counts,call_duration,call_duration_m,call_duration_s,basecall_fee,
                toll_fee,info_fee   ,other_fee)
	            select user_id,
                      sum(call_counts) as call_counts,
                      sum(call_duration) as call_duration,
                      sum(call_duration_m) as call_duration_m,
                      sum(call_duration_s) as call_duration_s,
                      sum(basecall_fee) as basecall_fee,
                      sum(toll_fee) as toll_fee,
                      sum(info_fee) as info_fee,
                      sum(other_fee) as other_fee
               from ${db_user}.dw_call_dt
               where op_time=date('${p_optime}')
               group by user_id;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1010
		return -1
	}
	
	aidb_commit $conn
	
	#	M11、建索引
	
	set sqlbuf "create index session.idx_product_3_1 on session.t_dw_product_3(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	#	M12、建立临时表4
	
	set sqlbuf "declare global temporary table session.t_dw_product_4
               (
               	cust_id              varchar(20),
               	level_def_mode       smallint
               )
               partitioning key (cust_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1012
		return -1
	}
	
	aidb_commit $conn
	
	#	M13、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_4(cust_id,level_def_mode)
	            select cust_id,level_def_mode
               from ${db_user}.dw_enterprise_member_ds
               where op_time=date('${p_optime}')
               group by cust_id,level_def_mode;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1013
		return -1
	}
	
	aidb_commit $conn
	
	#	M14、建索引
	
	set sqlbuf "create index session.idx_product_4_1 on session.t_dw_product_4(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1014
		return -1
	}
	
	aidb_commit $conn
	
	#	M15、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_5
               (
               	user_id              varchar(20)
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1015
		return -1
	}
	
	aidb_commit $conn
	
	#	M16、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_5(user_id)
	            select user_id
	            from (select user_id
                     from ${db_user}.dw_newbusi_call_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_sms_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_ismg_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wap_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_gprs_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_mms_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wlan_dt
                     where op_time=date('${p_optime}')
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_kj_dt
                     where op_time=date('${p_optime}')) a
               group by user_id;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1016
		return -1
	}
	
	aidb_commit $conn
	
	#	M17、建索引
	
	set sqlbuf "create index session.idx_product_5_1 on session.t_dw_product_5(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1017
		return -1
	}
	
	aidb_commit $conn
	
	#	M18、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_6
               (
               	acct_id              varchar(20),
               	owe_fee              decimal(9,2)
               )
               partitioning key (acct_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1018
		return -1
	}
	
	aidb_commit $conn
	
	#	M19、将当天的数据业务记录插入临时表
	
#	set sqlbuf "insert into session.t_dw_product_6(acct_id,owe_fee)
#	            select a.acct_id,-decimal((a.owe_fee-coalesce(b.owe_fee,0)),11,2)/100.00 as owe_fee
#               from (select a.acct_id,sum(a.prepay_available-a.unpay_fee) as owe_fee
#                     from ${db_user}.dwd_acc_balance_$p_timestamp a,(select acct_id,max(create_time) as create_time
#                                                                     from ${db_user}.dwd_acc_balance_$p_timestamp
#                                                                     group by acct_id) b
#                     where a.acct_id=b.acct_id and a.create_time=b.create_time
#                     group by a.acct_id) a left join (select acct_id,sum(unpay_fee) as owe_fee
#                                                      from ${db_user}.dwd_acct_bill_$p_timestamp
#                                                      where pay_sts_id=0
#                                                      group by acct_id) b on a.acct_id=b.acct_id;"
#	
#  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#  		puts $errmsg
#		trace_sql $errmsg 1019
#		return -1
#	}
#	
#	aidb_commit $conn
	
	#	M20、建索引
	
	set sqlbuf "create index session.idx_product_6_1 on session.t_dw_product_6(acct_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1020
		return -1
	}
	
	aidb_commit $conn
	
	
	
	#	M21、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_7
               (
               	user_id              varchar(20),
               	month_sms_mark		   smallint,
               	month_p2p_sms_mark	 smallint
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1021
		return -1
	}
	
	aidb_commit $conn
	
	
	#	M22、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_7(user_id,month_sms_mark,month_p2p_sms_mark)
	            select user_id,
	                   1 as month_sms_mark,
				             case when sum(a.month_p2p_sms_mark)>=1 then 1 else 0 end as month_p2p_sms_mark
	            from (select user_id,
	                         case when svcitem_id in (200001,200002,200003,200004,200005) then 1 else 0 end as month_p2p_sms_mark
                     from ${db_user}.dw_newbusi_sms_dt
                     where op_time=date('${p_optime}') and
                           svcitem_id in (200001,200002,200003,200004,200005,200007)
                     union all
                     select user_id,
                            0 as month_p2p_sms_mark
                     from ${db_user}.dw_newbusi_ismg_dt
                     where op_time=date('${p_optime}') and
                           sp_code = '931007'          or
                           svcitem_id in (300001,300002,300003,300004,300007,300010,300011,300012,300013)
                     union all
                     select user_id,
                            0 as month_p2p_sms_mark
                     from  ${db_user}.dw_newbusi_call_dt
                     where op_time=date('${p_optime}') and
                           svcitem_id in (100002,100026,100003,100021,100025)
                   ) a
               group by user_id;"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1022
		return -1
	}
	
	aidb_commit $conn
	
	#	M23、建索引
	
	set sqlbuf "create index session.idx_product_7_1 on session.t_dw_product_7(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1023
		return -1
	}
	
	aidb_commit $conn
	

	
	#	M24、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_8
               (
               	user_id              varchar(20),
               	day_sms_mark		     smallint,
                day_p2p_sms_mark	   smallint
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1024
		return -1
	}
	
	aidb_commit $conn
	
	
	#	M25、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_8(user_id,day_sms_mark,day_p2p_sms_mark)
	            select user_id,
	                   1 as day_sms_mark,
				             case when sum(a.day_p2p_sms_mark)>=1 then 1 else 0 end as day_p2p_sms_mark
	            from (select user_id,
	                         case when svcitem_id in (200001,200002,200003,200004,200005) then 1 else 0 end as day_p2p_sms_mark
                     from ${db_user}.dw_newbusi_sms_$p_timestamp
                     where svcitem_id in (200001,200002,200003,200004,200005,200007)
                     union all
                     select user_id,
                            0 as day_p2p_sms_mark
                     from ${db_user}.dw_newbusi_ismg_$p_timestamp
                     where sp_code = '931007'          or
                           svcitem_id in (300001,300002,300003,300004,300007,300010,300011,300012,300013)
                     union all
                     select user_id,
                            0 as day_p2p_sms_mark
                     from  ${db_user}.dw_newbusi_call_$p_timestamp
                     where svcitem_id in (100002,100026,100003,100021,100025)
                   ) a
               group by user_id;"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1025
		return -1
	}
	
	aidb_commit $conn
	
	#	M26、建索引
	
	set sqlbuf "create index session.idx_product_8_1 on session.t_dw_product_8(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1026
		return -1
	}
	
	aidb_commit $conn
	
	
	
	
#	M27、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_9 like ${db_user}.dw_product_yyyymmdd partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1027
		return -1
	}
	
	aidb_commit $conn
	
	
	#	M28、将当天的数据业务记录插入临时表
		
	set sqlbuf "insert into session.t_dw_product_9
               (user_id          ,cust_id          ,acct_id             ,age                ,age_id          ,sex_id             ,
                education_id     ,custtype_id      ,custclass_id        ,credit             ,credit_id       ,product_no         ,
                imsi             ,sim_id           ,sim_type            ,city_id            ,city_id1        ,county_id          ,
                channel_id       ,channeltype_id   ,warn_fee            ,so_nbr             ,accttype_id     ,user_online        ,
                user_online_id   ,brand_id         ,crm_brand_id1       ,crm_brand_id2      ,crm_brand_id3   ,plan_id            ,
                userstatus_id    ,stopstatus_id    ,usertype_id         ,month_new_mark     ,month_off_mark  ,month_recreate_mark,
                month_new_count  ,month_off_count  ,month_recreate_count,day_new_mark       ,day_off_mark    ,day_recreate_mark  ,
                day_call_mark    ,month_call_mark  ,day_newbusi_mark    ,month_newbusi_mark ,active_mark     ,zero_mark          ,
                month_active_mark,month_zero_mark  ,vip_mark            ,single_mark        ,vpmn_mark       ,enterprise_mark    ,
                import_ent_mark  ,free_mark        ,test_mark           ,bill_mark          ,snapshot_mark   ,recreate_mark      ,
                create_date      ,valid_date       ,sts_date            ,expire_date        ,call_counts     ,call_duration      ,
                call_duration_m  ,call_duration_s  ,basecall_fee        ,toll_fee           ,info_fee        ,other_fee          ,
                mtd_call_counts  ,mtd_call_duration,mtd_call_duration_m ,mtd_call_duration_s,mtd_basecall_fee,mtd_toll_fee       ,
                mtd_info_fee     ,mtd_other_fee    ,balance_fee         ,owe_fee            ,month_sms_mark  ,month_p2p_sms_mark ,
                day_sms_mark     ,day_p2p_sms_mark)
                select a.user_id            ,a.cust_id          ,a.acct_id             ,a.age                 ,a.age_id          ,a.sex_id             ,
                       a.education_id       ,a.custtype_id      ,a.custclass_id        ,a.credit              ,a.credit_id       ,a.product_no         ,
                       a.imsi               ,a.sim_id           ,a.sim_type            ,a.city_id             ,
                       case when f.cust_id is not null and a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and f.level_def_mode=1 then '888' else a.city_id end as city_id1,
                       a.county_id          ,a.channel_id       ,a.channeltype_id      ,a.warn_fee            ,a.so_nbr          ,a.accttype_id        ,
                       a.user_online        ,a.user_online_id   ,a.brand_id            ,a.crm_brand_id1       ,a.crm_brand_id2   ,a.crm_brand_id3      ,
                       a.plan_id            ,a.userstatus_id    ,a.stopstatus_id       ,a.usertype_id         ,a.month_new_mark  ,a.month_off_mark     ,
                       a.month_recreate_mark,a.month_new_count  ,a.month_off_count     ,a.month_recreate_count,a.day_new_mark    ,a.day_off_mark       ,
                       a.day_recreate_mark  ,
                       case when c.user_id is not null then 1 else 0 end as day_call_mark,
                       case when d.user_id is not null then 1 else 0 end as month_call_mark,
                       case when m.user_id is not null then 1 else 0 end as day_newbusi_mark,
                       case when n.user_id is not null then 1 else 0 end as month_newbusi_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and (c.user_id is not null or m.user_id is not null) 
                                 then 1 else 0 end as active_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and c.user_id is null and m.user_id is null 
                                 then 1 else 0 end as zero_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and (d.user_id is not null or n.user_id is not null) 
                                 then 1 else 0 end as month_active_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and d.user_id is null and n.user_id is null 
                                 then 1 else 0 end as month_zero_mark,
                       a.vip_mark           ,a.single_mark      ,
                       0 as vpmn_mark,
                       case when f.cust_id is not null and a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) then 1 else 0 end enterprise_mark,
                       case when f.cust_id is not null and f.level_def_mode=1 then 1 else 0 end enterprise_mark,
                       0 as free_mark,
                       0 as test_mark,
                       case when (c.basecall_fee+c.toll_fee+c.info_fee+c.other_fee)>0 then 1 else 0 end bill_mark,
                       0 as snapshot_mark,
                       case when a.day_recreate_mark=1 then 1 else 0 end recreate_mark,
                       a.create_date        ,a.valid_date       ,a.sts_date            ,a.expire_date         ,
                       coalesce(c.call_counts,0) as call_counts,
                       coalesce(c.call_duration,0) as call_duration,
                       coalesce(c.call_duration_m,0) as call_duration_m,
                       coalesce(c.call_duration_s,0) as call_duration_s,
                       coalesce(c.basecall_fee,0) as basecall_fee,
                       coalesce(c.toll_fee,0) as toll_fee,
                       coalesce(c.info_fee,0) as info_fee,
                       coalesce(c.other_fee,0) as other_fee,
                       coalesce(d.call_counts,0) as mtd_call_counts,
                       coalesce(d.call_duration,0) as mtd_call_duration,
                       coalesce(d.call_duration_m,0) as mtd_call_duration_m,
                       coalesce(d.call_duration_s,0) as mtd_call_duration_s,
                       coalesce(d.basecall_fee,0) as mtd_basecall_fee,
                       coalesce(d.toll_fee,0) as mtd_toll_fee,
                       coalesce(d.info_fee,0) as mtd_info_fee,
                       coalesce(d.other_fee,0) as mtd_other_fee,
                       coalesce(q.amount,0) as balance_fee,
                       0 as owe_fee,
                       case when h.month_sms_mark = 1 then 1 else 0 end as month_sms_mark,
                       case when h.month_p2p_sms_mark = 1 then 1 else 0 end as month_p2p_sms_mark,
                       case when i.day_sms_mark = 1 then 1 else 0 end as day_sms_mark,
                       case when i.day_p2p_sms_mark = 1 then 1 else 0 end as day_p2p_sms_mark
                from ${db_user}.dw_product_${p_timestamp}_2 a left join session.t_dw_product_2 c on a.user_id=c.user_id
                                                              left join session.t_dw_product_3 d on a.user_id=d.user_id
                                                              left join session.t_dw_product_4 f on a.cust_id=f.cust_id
                                                              left join (select cust_id,sum(amount) as amount
                                                                         from ${db_user}.dw_acct_book_$p_timestamp
                                                                         group by cust_id) q on a.cust_id=q.cust_id
                                                              left join session.t_dw_product_1 m on a.user_id=m.user_id
                                                              left join session.t_dw_product_5 n on a.user_id=n.user_id
                                                              left join session.t_dw_product_7 h on a.user_id=h.user_id
                                                              left join session.t_dw_product_8 i on a.user_id=i.user_id;"
    puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1028
		return -1
	}
	
	aidb_commit $conn
	
	#	M29、建索引
	
	set sqlbuf "create index session.idx_product_9_1 on session.t_dw_product_9(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1029
		return -1
	}
	
	aidb_commit $conn
	
   #	M30、生成dw_product_yyyymmdd数据表
		
	set sqlbuf "insert into ${db_user}.dw_product_$p_timestamp
               (user_id          ,cust_id          ,acct_id             ,age                ,age_id          ,sex_id             ,
                education_id     ,custtype_id      ,custclass_id        ,credit             ,credit_id       ,product_no         ,
                imsi             ,sim_id           ,sim_type            ,city_id            ,city_id1        ,county_id          ,
                channel_id       ,channeltype_id   ,warn_fee            ,so_nbr             ,accttype_id     ,user_online        ,
                user_online_id   ,brand_id         ,crm_brand_id1       ,crm_brand_id2      ,crm_brand_id3   ,plan_id            ,
                userstatus_id    ,stopstatus_id    ,usertype_id         ,month_new_mark     ,month_off_mark  ,month_recreate_mark,
                month_new_count  ,month_off_count  ,month_recreate_count,day_new_mark       ,day_off_mark    ,day_recreate_mark  ,
                day_call_mark    ,month_call_mark  ,day_newbusi_mark    ,month_newbusi_mark ,active_mark     ,zero_mark          ,
                month_active_mark,month_zero_mark  ,vip_mark            ,single_mark        ,vpmn_mark       ,enterprise_mark    ,
                import_ent_mark  ,free_mark        ,test_mark           ,bill_mark          ,snapshot_mark   ,recreate_mark      ,
                create_date      ,valid_date       ,sts_date            ,expire_date        ,call_counts     ,call_duration      ,
                call_duration_m  ,call_duration_s  ,basecall_fee        ,toll_fee           ,info_fee        ,other_fee          ,
                mtd_call_counts  ,mtd_call_duration,mtd_call_duration_m ,mtd_call_duration_s,mtd_basecall_fee,mtd_toll_fee       ,
                mtd_info_fee     ,mtd_other_fee    ,balance_fee         ,owe_fee            ,month_sms_mark  ,month_p2p_sms_mark ,
                day_sms_mark     ,day_p2p_sms_mark)
                select a.user_id          ,a.cust_id          ,a.acct_id             ,a.age                ,a.age_id             ,a.sex_id             ,
                       a.education_id     ,a.custtype_id      ,a.custclass_id        ,a.credit             ,a.credit_id          ,a.product_no         ,
                       a.imsi             ,a.sim_id           ,a.sim_type            ,a.city_id            ,a.city_id1           ,a.county_id          ,
                       a.channel_id       ,a.channeltype_id   ,a.warn_fee            ,a.so_nbr             ,a.accttype_id        ,a.user_online        ,
                       a.user_online_id   ,a.brand_id         ,a.crm_brand_id1       ,a.crm_brand_id2      ,a.crm_brand_id3      ,a.plan_id            ,
                       a.userstatus_id    ,a.stopstatus_id    ,a.usertype_id         ,a.month_new_mark     ,a.month_off_mark     ,a.month_recreate_mark,
                       a.month_new_count  ,a.month_off_count  ,a.month_recreate_count,a.day_new_mark       ,a.day_off_mark       ,a.day_recreate_mark  ,
                       a.day_call_mark    ,a.month_call_mark  ,a.day_newbusi_mark    ,a.month_newbusi_mark ,a.active_mark        ,a.zero_mark          ,
                       a.month_active_mark,a.month_zero_mark  ,a.vip_mark            ,a.single_mark        ,
                       case when e.user_id is not null then 1 else 0 end vpmn_mark,
                       a.enterprise_mark  ,a.import_ent_mark     ,
                       case when g.user_id is not null then 1 else 0 end free_mark,
                       case when l.user_id is not null then 1 else 0 end test_mark,
                       a.bill_mark        ,
                       case when o.user_id is not null then 1 else 0 end snapshot_mark,
                       a.recreate_mark    ,a.create_date      ,a.valid_date          ,a.sts_date           ,a.expire_date        ,a.call_counts        ,
                       a.call_duration    ,a.call_duration_m  ,a.call_duration_s     ,a.basecall_fee       ,a.toll_fee           ,a.info_fee           ,
                       a.other_fee        ,a.mtd_call_counts  ,a.mtd_call_duration   ,a.mtd_call_duration_m,a.mtd_call_duration_s,a.mtd_basecall_fee   ,
                       a.mtd_toll_fee     ,a.mtd_info_fee     ,a.mtd_other_fee       ,a.balance_fee        ,
                       case when coalesce(p.owe_fee,0)>0 then coalesce(p.owe_fee,0) else 0 end as owe_fee,
                       a.month_sms_mark   ,a.month_p2p_sms_mark,a.day_sms_mark       ,a.day_p2p_sms_mark
                from session.t_dw_product_9 a left join (select user_id from ${db_user}.dw_vpmn_member_ds
                                                         where op_time=date('${p_optime}')
                                                         group by user_id) e on a.user_id=e.user_id
                                              left join (select user_id from ${db_user}.dw_product_test_phone_ds 
                                                         where op_time=date('${p_optime}') and sts=1 and char(date(valid_date),iso)<'${nextday}' and
                                                               char(date(expire_date),iso)>='${nextday}'
                                                         group by user_id) l on a.user_id=l.user_id
                                              left join (select user_id from ${db_user}.dw_product_free_ds 
                                                         where op_time=date('${p_optime}') and char(date(valid_date),iso)<'${nextday}' and
                                                               char(date(expire_date),iso)>='${nextday}'
                                                         group by user_id) g on a.user_id=g.user_id
                                              left join ${db_user}.Dw_product_snapshot o on a.user_id=o.user_id
                                              left join session.t_dw_product_6 p on a.acct_id=p.acct_id;"
    puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1030
		return -1
	}
	
	aidb_commit $conn
	
	#	M31、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_1 on ${db_user}.dw_product_${p_timestamp}(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1031
		return -1
	}
	
	aidb_commit $conn
	
	#	M32、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_2 on ${db_user}.dw_product_${p_timestamp}(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1032
		return -1
	}
	
	aidb_commit $conn
	
	#	M33、做runstats：
	#exec db2 connect to bassdb user bass2 using bass2
	#exec db2 runstats on table ${db_user}.dw_product_${p_timestamp} on all columns and indexes all
	
	
   if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1033
      return -1
   }
   
   aidb_commit $conn 
   
	#	做runstats：
	aidb_runstats ${db_user}.dw_product_${p_timestamp} 1   
   
   
 	#	20100513 zhaolp2增加处理流程,防止用户资料表出现重复  
   	set sqlbuf "select count(*),count(distinct user_id) from ${db_user}.dw_product_${p_timestamp}"
   	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1034
		return -1
	}
   	
   	while {[set p_row [aidb_fetch $handle]] != ""} {
		set allrows 		[lindex $p_row 0]
		set usercounts 		[lindex $p_row 1]
	}
	
	
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1033
		return -1
	}	
	
	puts $allrows
	puts $usercounts
	
	if { $allrows != $usercounts } {
	      set errmsg "用户资料表user_id存在重复,用户资料表运行标志置为失败,请尽快处理"
	      trace_sql $errmsg 1035
	      return -1
	}	
	
	
	if { $usercounts == "0" } {
	      set errmsg "用户资料表为空,请尽快处理"
	      puts $errmsg
	      trace_sql $errmsg 1036
	      return -1
	}

	aidb_close $handle
	
   return 0
}