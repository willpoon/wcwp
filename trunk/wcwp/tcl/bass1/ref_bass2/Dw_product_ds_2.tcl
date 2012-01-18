#/*******************************************************************************
#	源程序名：Dw_product_ds_2.tcl
#	简要说明：生成用户资料日表(程序二)
#	程序流程：dwd_product_yyyymmdd:dw_call_dt:dw_call_yyyymmdd:dw_vip_cust_yyyymmdd:dw_vpmn_member_yyyymmdd:dw_enterprise_member_yyyymmdd:dw_product_no_press_stop_ds --> dw_product_yyyymmdd:dw_product_yyymm
#  作者    ：杨菲
#  运行方法：ds Dw_product_ds_2.tcl 2007-03-31 20070331
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
#修改记录：1.2010-05-20 By fuzl  增长 user_id 的字段长度 
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
	
	#	M1、删除当日已存在的数据表
	
	set sqlbuf "drop table ${db_user}.dw_product_${p_timestamp}_2;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		#trace_sql $errmsg 1001
	}
	
	aidb_commit $conn
	
	#	M2、创建当日数据表
	
   set sqlbuf "create table ${db_user}.dw_product_${p_timestamp}_2 like ${db_user}.dw_product_yyyymmdd_2 in $tbs_3h
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
	
	#	M4、将当天的新增,离网与拆机复装标志插入临时表
	
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
	
		#	M7、将当月的新增,离网与拆机复装数插入临时表
		
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
	
		#	M8、将当月的新增,离网与拆机复装数插入临时表
		
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
	
	#	M9、建索引
	
	set sqlbuf "create index session.idx_product_2_1 on session.t_dw_product_2(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1009
		return -1
	}

	aidb_commit $conn
	
	#	M10、将当天的数据业务记录插入临时表
		
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
	
	#	M11、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_2_1 on ${db_user}.dw_product_${p_timestamp}_2(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	#	M11、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_2_2 on ${db_user}.dw_product_${p_timestamp}_2(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	aidb_close $handle
	
	#	做runstats：
	aidb_runstats ${db_user}.dw_product_${p_timestamp}_2 1	
	
   return 0
}