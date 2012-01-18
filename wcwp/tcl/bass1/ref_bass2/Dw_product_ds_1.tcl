#/*******************************************************************************
#	源程序名：Dw_product_ds_1.tcl
#	简要说明：生成用户资料日表(程序一)
#	程序流程：dwd_product_yyyymmdd:dw_call_dt:dw_call_yyyymmdd:dw_vip_cust_yyyymmdd:dw_vpmn_member_yyyymmdd:dw_enterprise_member_yyyymmdd:dw_product_no_press_stop_ds --> dw_product_yyyymmdd:dw_product_yyymm
#  作者    ：杨菲
#  运行方法：ds Dw_product_ds_1.tcl 2007-03-31 20070331
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
#  修改日志：2009-06-24 在处理custclass_id的地方左关联  dw_cust_vip_card_yyyymmdd表select coalesce(b.vip_level,0) as custclass_id 关联条件 user_id=user_id；
#  修改人  ：lizz
# 		zhuwei	20110412 根据工单"关于修改系统2经表credit、snapshot_mark 等字段问题修改 "修改credit,credit_id从账户表取相应指标 
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
	
	set sqlbuf "drop table ${db_user}.dw_product_${p_timestamp}_1;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		#trace_sql $errmsg 1001
	}
	
	aidb_commit $conn
	
	#	M2、创建当日数据表
	
   set sqlbuf "create table ${db_user}.dw_product_${p_timestamp}_1 like ${db_user}.dw_product_yyyymmdd_1 in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially;"
   
   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1002
		return -1
	}
	
	#	M3、建立临时表1
	
	set sqlbuf "declare global temporary table session.t_dw_product_${p_timestamp}_1 like dw_product_yyyymmdd_1
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
   	 
	#	M4、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_${p_timestamp}_1
	            (user_id       ,cust_id      ,acct_id      ,age          ,age_id     ,sex_id        ,
	             education_id  ,custtype_id  ,custclass_id ,credit       ,credit_id  ,product_no    ,
	             imsi          ,sim_id       ,sim_type     ,city_id      ,county_id  ,channel_id    ,
	             channeltype_id,warn_fee     ,so_nbr       ,accttype_id  ,user_online,user_online_id,
	             brand_id      ,crm_brand_id1,crm_brand_id2,crm_brand_id3,plan_id    ,userstatus_id ,
	             stopstatus_id ,usertype_id  ,vip_mark     ,single_mark  ,create_date,valid_date    ,
	             sts_date      ,expire_date)
	            select a.user_id       ,a.cust_id      ,a.acct_id      ,
	                   coalesce(b.age,0) as age,
	                   case when coalesce(b.age,-1)<0 then -1 when coalesce(b.age,-1)>=0 and coalesce(b.age,-1)<18 then 1
	                        when coalesce(b.age,-1)>=18 and coalesce(b.age,-1)<24 then 2 when coalesce(b.age,-1)>=24 and coalesce(b.age,-1)<30 then 3
	                        when coalesce(b.age,-1)>=30 and coalesce(b.age,-1)<45 then 4 when coalesce(b.age,-1)>=45 and coalesce(b.age,-1)<60 then 5
	                        else 6 end as age_id,
	                   coalesce(b.sex_id,0) as sex_id,
	                   coalesce(b.education_id,0) as education_id,
	                   coalesce(b.custtype_id,0) as custtype_id,
	                   coalesce(m.vip_level,0) as custclass_id,
	                   case when a.userstatus_id in (1,2,3,6,8,9)  then j.credit end as credit    ,
	                   case when a.userstatus_id in (1,2,3,6,8,9)  then j.credit_class end  as credit_id,
	                   a.product_no    ,
	                   a.sphone_id as imsi,
	                   a.sim_id        ,a.sim_type     ,a.city_id      ,a.county_id  ,a.channel_id    ,
	                   coalesce(n.channeltype_id,1) as channeltype_id,
	                   a.warn_fee      ,a.so_nbr       ,
	                   coalesce(j.accttype_id,0) as accttype_id,
	                   case when a.userstatus_id in (1,2,3,6,8) then days(date('${p_optime}'))-days(date(a.create_date))+1
	                        else days(date(a.sts_date))-days(date(a.create_date))+1 end as user_online,
	                   0 as user_online_id,
	                   a.brand_id      ,
	                   coalesce(h.crm_brand_id1,1) as crm_brand_id1,
	                   coalesce(h.crm_brand_id2,10) as crm_brand_id2,
	                   case when coalesce(h.crm_brand_id2,10)=10 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 101
	                        when coalesce(h.crm_brand_id2,10)=10 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 102
	                        when coalesce(h.crm_brand_id2,10)=10 then 103
	                        when coalesce(h.crm_brand_id2,10)=100 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1001
	                        when coalesce(h.crm_brand_id2,10)=100 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1002
	                        when coalesce(h.crm_brand_id2,10)=100 then 1003
	                        when coalesce(h.crm_brand_id2,10)=110 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1101
	                        when coalesce(h.crm_brand_id2,10)=110 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1102
	                        when coalesce(h.crm_brand_id2,10)=110 then 1103
	                        when coalesce(h.crm_brand_id2,10)=120 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1201
	                        when coalesce(h.crm_brand_id2,10)=120 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1202
	                        when coalesce(h.crm_brand_id2,10)=120 then 1203
	                        when coalesce(h.crm_brand_id2,10)=150 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1501
	                        when coalesce(h.crm_brand_id2,10)=150 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1502
	                        when coalesce(h.crm_brand_id2,10)=150 then 1503
	                        when coalesce(h.crm_brand_id2,10)=160 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1601
	                        when coalesce(h.crm_brand_id2,10)=160 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1602
	                        when coalesce(h.crm_brand_id2,10)=160 then 1603
	                        when coalesce(h.crm_brand_id2,10)=170 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 1701
	                        when coalesce(h.crm_brand_id2,10)=170 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 1702
	                        when coalesce(h.crm_brand_id2,10)=170 then 1703
	                        when coalesce(h.crm_brand_id2,10)=220 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 2201
	                        when coalesce(h.crm_brand_id2,10)=220 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 2202
	                        when coalesce(h.crm_brand_id2,10)=220 then 2203
	                        when coalesce(h.crm_brand_id2,10)=230 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (-100,1001,1002,1003,1004,1005,1006,1007) then 2301
	                        when coalesce(h.crm_brand_id2,10)=230 and int(right(rtrim(char(coalesce(i.sprom_id,-100))),4)) in (9022,9025,9026,9027) then 2302
	                        when coalesce(h.crm_brand_id2,10)=230 then 2303
	                        when coalesce(h.crm_brand_id2,10)=20 then 200
	                        when coalesce(h.crm_brand_id2,10)=30 then 300
	                        when coalesce(h.crm_brand_id2,10)=40 and coalesce(i.sprom_id,-100) in (90002006) then 403
	                        when coalesce(h.crm_brand_id2,10)=40 and coalesce(i.sprom_id,-100) in (90002004) then 404
	                        when coalesce(h.crm_brand_id2,10)=40 and coalesce(i.sprom_id,-100) in (90002005) then 401
	                        when coalesce(h.crm_brand_id2,10)=40 then 402
	                        when coalesce(h.crm_brand_id2,10)=50 then 500
	                        when coalesce(h.crm_brand_id2,10)=60 then 600
	                        when coalesce(h.crm_brand_id2,10)=70 then 700
	                        when coalesce(h.crm_brand_id2,10)=80 then 800
	                        when coalesce(h.crm_brand_id2,10)=90 then 900
	                        when coalesce(h.crm_brand_id2,10)=130 then 1300
	                        when coalesce(h.crm_brand_id2,10)=140 then 1400
	                        when coalesce(h.crm_brand_id2,10)=180 then 1800
	                        when coalesce(h.crm_brand_id2,10)=190 then 1900
	                        when coalesce(h.crm_brand_id2,10)=200 then 2000
	                        when coalesce(h.crm_brand_id2,10)=210 then 2100
	                        when coalesce(h.crm_brand_id2,10)=240 then 2400
	                        when coalesce(h.crm_brand_id2,10)=250 then 2500
	                        when coalesce(h.crm_brand_id2,10)=260 then 2600
	                        when coalesce(h.crm_brand_id2,10)=270 then 2700
	                        when coalesce(h.crm_brand_id2,10)=280 then 2800
	                        else -1 end as crm_brand_id3,
	                   a.plan_id       ,a.userstatus_id,
	                   case when Locate('1',a.os_sts)=0     then 0  when Locate('1',a.os_sts)=1     then 1
	                        when Locate('1',a.os_sts)=2     then 2  when Locate('1',a.os_sts)=3     then 3
	                        when Locate('1',a.os_sts,12)=12 then 12 when Locate('1',a.os_sts)=11    then 11
	                        when Locate('1',a.os_sts,14)=14 then 14 when Locate('1',a.os_sts)=13    then 13
	                        when Locate('1',a.os_sts,16)=16 then 16 when Locate('1',a.os_sts)=15    then 15
	                        when Locate('1',a.os_sts)=26    then 26 when Locate('1',a.os_sts)=27    then 27
	                        when Locate('1',a.os_sts)=28    then 28 when Locate('1',a.os_sts)=29    then 29
	                        when Locate('1',a.os_sts)=31    then 31 when Locate('1',a.os_sts)=32    then 32
	                        when Locate('1',a.os_sts)=33    then 33 else 0 end as stopstatus_id,
	                   a.user_type as usertype_id,
	                   case when m.user_id is null then 0 else 1 end vip_mark,
	                   0 as single_mark,
	                   date(a.create_date) as create_date,
	                   date(a.valid_date) as valid_date,
	                   date(a.sts_date) as sts_date,
	                   date(a.expire_date) as expire_date
	            from ${db_user}.dwd_product_$p_timestamp a left join ${db_user}.dw_cust_$p_timestamp b on a.cust_id=b.cust_id
	                                                       left join ${db_user}.map_pub_brand h on a.plan_id=h.plan_id
	                                                       left join (select a.user_id,max(a.sprom_id) as sprom_id
	                                                                  from ${db_user}.dw_product_sprom_ds a,(select a.user_id,max(a.valid_date) as valid_date
	                                                                                                         from ${db_user}.dw_product_sprom_ds a,${db_user}.dim_product_item b
	                                                                                                         where a.op_time=date('${p_optime}') and a.sprom_id=b.prod_id and 
	                                                                                                               left(ltrim(rtrim(char(b.prod_id))),5) in ('90001','90002','90009') and
	                                                                                                               char(date(a.valid_date),iso)<'${nextday}' and char(date(a.expire_date),iso)>='${optmonth_01}' and
	                                                                                                               char(date(b.valid_date),iso)<= '2006-02-19' and b.is_prom=1
	                                                                                                         group by a.user_id) b,dim_product_item c
	                                                                  where a.op_time=date('${p_optime}') and a.user_id=b.user_id and a.sprom_id=c.prod_id and
	                                                                        a.valid_date=b.valid_date and left(ltrim(rtrim(char(c.prod_id))),5) in('90001','90002','90009') and 
	                                                                        char(date(c.valid_date),iso) <= '2006-02-19' and c.is_prom=1
	                                                                  group by a.user_id) i on a.user_id=i.user_id
	                                                       left join ${db_user}.dw_acct_msg_$p_timestamp j on a.acct_id=j.acct_id
	                                                       left join ${db_user}.dim_pub_channel n on a.channel_id=n.channel_id
	                                                       left join (select user_id,min(vip_level) vip_level from ${db_user}.dw_cust_vip_card_$p_timestamp where rec_status=1 group by user_id) m on a.user_id=m.user_id
	            where a.plan_id<>0;"
	puts $sqlbuf
   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1004
		return -1
	}
	
	aidb_commit $conn
	
	#	M5、将当天的数据业务记录插入临时表
	
	set sqlbuf "insert into ${db_user}.dw_product_${p_timestamp}_1
	            (user_id       ,cust_id      ,acct_id      ,age          ,age_id     ,sex_id        ,
	             education_id  ,custtype_id  ,custclass_id ,credit       ,credit_id  ,product_no    ,
	             imsi          ,sim_id       ,sim_type     ,city_id      ,county_id  ,channel_id    ,
	             channeltype_id,warn_fee     ,so_nbr       ,accttype_id  ,user_online,user_online_id,
	             brand_id      ,crm_brand_id1,crm_brand_id2,crm_brand_id3,plan_id    ,userstatus_id ,
	             stopstatus_id ,usertype_id  ,vip_mark     ,single_mark  ,create_date,valid_date    ,
	             sts_date      ,expire_date)
	            select a.user_id       ,a.cust_id      ,a.acct_id      ,a.age          ,a.age_id     ,a.sex_id        ,
	                   a.education_id  ,a.custtype_id  ,a.custclass_id ,a.credit       ,a.credit_id  ,a.product_no    ,
	                   a.imsi          ,a.sim_id       ,a.sim_type     ,a.city_id      ,a.county_id  ,a.channel_id    ,
	                   a.channeltype_id,a.warn_fee     ,a.so_nbr       ,a.accttype_id  ,a.user_online,
	                   case when a.user_online<30                         then 1  when a.user_online>=30  and a.user_online<60  then 2 
	                        when a.user_online>=60  and a.user_online<90  then 3  when a.user_online>=90  and a.user_online<120 then 4
	                        when a.user_online>=120 and a.user_online<150 then 5  when a.user_online>=150 and a.user_online<180 then 6
	                        when a.user_online>=180 and a.user_online<210 then 7  when a.user_online>=210 and a.user_online<240 then 8
	                        when a.user_online>=240 and a.user_online<270 then 9  when a.user_online>=270 and a.user_online<300 then 10
	                        when a.user_online>=300 and a.user_online<330 then 11 when a.user_online>=330 and a.user_online<365 then 12
	                        when a.user_online/365=1                      then 13 when a.user_online/365=2                      then 14
	                        when a.user_online/365=3                      then 15 when a.user_online/365=4                      then 16
	                        when a.user_online/365=5                      then 17 when a.user_online/365=6                      then 18
	                        when a.user_online/365=7                      then 19 when a.user_online/365=8                      then 20
	                        when a.user_online/365=9                      then 21 when a.user_online/365=10                     then 22
	                        when a.user_online/365=11                     then 23 when a.user_online/365=12                     then 24
	                        when a.user_online/365=13                     then 25 when a.user_online/365=14                     then 26
	                        when a.user_online/365=15                     then 27 when a.user_online/365=16                     then 28
	                        when a.user_online/365=17                     then 29 when a.user_online/365=18                     then 30
	                        when a.user_online/365=19                     then 31 when a.user_online/365=20                     then 32
	                        else 33 end as user_online_id,
	                   a.brand_id      ,a.crm_brand_id1,a.crm_brand_id2,
	                   case when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=101 then 1501 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=102  then 1502
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=103  then 1503 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=200  then 1400
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=300  then 1900 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=401  then 1800
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=402  then 1800 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=403  then 1800
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=404  then 1800 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=500  then 1800
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1001 then 1601 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1002 then 1602
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1003 then 1603 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1101 then 1701
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1102 then 1702 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1103 then 1703
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1201 then 1501 
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1202 then 1502
	                        when (rtrim(char(a.plan_id)) like '%10005' or rtrim(char(a.plan_id)) like '%10006' or rtrim(char(a.plan_id)) like '%10007' or 
	                              rtrim(char(a.plan_id)) like '%10008' or rtrim(char(a.plan_id)) like '%10009' or rtrim(char(a.plan_id)) like '%40002' or 
	                              rtrim(char(a.plan_id)) like '%40003' or rtrim(char(a.plan_id)) like '%40004' or rtrim(char(a.plan_id)) like '%50007' or 
	                              rtrim(char(a.plan_id)) like '%50008' or rtrim(char(a.plan_id)) like '%70002' or b.user_id is not null) and a.crm_brand_id3=1203 then 1503 
	                        else a.crm_brand_id3 end as crm_brand_id3,
	                   a.plan_id       ,a.userstatus_id,a.stopstatus_id,a.usertype_id  ,a.vip_mark   ,
	                   case when rtrim(char(plan_id)) like '%10005' or rtrim(char(plan_id)) like '%10006' or rtrim(char(plan_id)) like '%10007' or 
	                             rtrim(char(plan_id)) like '%10008' or rtrim(char(plan_id)) like '%10009' or rtrim(char(plan_id)) like '%40002' or 
	                             rtrim(char(plan_id)) like '%40003' or rtrim(char(plan_id)) like '%40004' or rtrim(char(plan_id)) like '%50007' or 
	                             rtrim(char(plan_id)) like '%50008' or rtrim(char(plan_id)) like '%70002' or b.user_id is not null then 1
	                        else 0 end as single_mark,
	                   a.create_date   ,a.valid_date   ,a.sts_date     ,a.expire_date
	            from session.t_dw_product_${p_timestamp}_1 a left join (select user_id
	                                                                    from dw_product_sprom_ds
	                                                                    where sprom_id in (90009140,90009141,90009155,92009067) and char(date(valid_date),iso)<'${nextday}' and
	                                                                          char(date(expire_date),iso)>='${nextday}'
	                                                                    group by user_id) b on a.user_id=b.user_id;"
	                                                                    
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1005
		return -1
	}
	
	aidb_commit $conn
	
	
	#	M6、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${p_timestamp}_1_1 on ${db_user}.dw_product_${p_timestamp}_1(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1006
		return -1
	}
	
   aidb_commit $conn
   
	aidb_close $handle
	
	#	做runstats：
	aidb_runstats ${db_user}.dw_product_${p_timestamp}_1 1	
	
   return 0
}
