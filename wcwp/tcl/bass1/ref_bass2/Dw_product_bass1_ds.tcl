#****************************************************************************************
# ** 程序名称: Dw_product_bass1_ds.tcl
# ** 程序功能: 为一经尽快出日数据，只关联出用户资料表中一经必需的字段
# ** 运行粒度: 日
# ** 运行示例: ds Dw_product_bass1_ds.tcl 2008-03-01
# ** 创建时间: 2009-9-11
# ** 创 建 人: xufr
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **		20091126 zhaolp2 增加stopstatus_id字段
#				20100926 liuzhilong增加处理流程,防止用户资料表出现重复  
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#****************************************************************************************
proc deal {p_optime p_timestamp} {

	global conn
	global handle
	global env

	if [ catch { set handle [ aidb_open $conn ] } errmsg ] {
		trace_sql $errmsg 1000
		return -1
	}

	if { [ main_tcl $p_optime $p_timestamp ]  != 0 } {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	aidb_close $handle

	return 0
}
proc main_tcl { p_optime p_timestamp } {
	global conn
	global handle
	global env
	source  $env(AITOOLSPATH)/bin/base_bass.tcl
	set op_time $p_optime
	set timestamp $p_timestamp
	set db_user $env(DB_USER)
	set tbs_3h $env(TBS_3H)
	set optmonth [ string range $p_timestamp 0 5 ]
	set optmonth_01 [ string range $p_optime 0 7 ]01
	set lastmonth [ GetLastMonth [string range $p_timestamp 0 5 ] ]
	set nextday [ai_adddays $handle ${p_optime} 1]
	set handle [ aidb_open $conn ]
	set lastday [ GetLastDay $p_timestamp ]
	set last_day_month [ GetLastDay [ GetNextMonth [ string range $p_timestamp 0 5 ] ]01 ]
	#定义目标表
	set objectTable "${db_user}.Dw_product_bass1_${p_timestamp}"
# #===============================================================================================
# # step 1: 删除目标表
# #===============================================================================================
	set sql_buf "drop table ${objectTable}"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		puts "errmsg:$errmsg"
	}
	aidb_commit $conn
# #===============================================================================================
# # step 2: 创建目标表
# #===============================================================================================
	set sql_buf "
	create table ${objectTable}
	like dw_product_bass1_yyyymmdd
	in ${tbs_3h}
	index in tbs_index
	partitioning key (user_id) using hashing
	not logged initially"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		puts "errmsg:$errmsg"
		trace_sql $errmsg 1001
		return -1
	}
	aidb_commit $conn
# #===============================================================================================
# # step 3: 建立表级锁
# #===============================================================================================
	set sql_buf "
	alter table ${objectTable} locksize table"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		puts "errmsg:$errmsg"
		trace_sql $errmsg 1001
		return -1
	}
	aidb_commit $conn
# #===============================================================================================
# # step 4: 生成数据
# #===============================================================================================
	set sql_buf "
	insert into ${objectTable}
	(
		user_id
		,cust_id
		,usertype_id
		,brand_id
		,crm_brand_id2
		,userstatus_id
		,stopstatus_id
		,product_no
		,imsi
		,city_id
		,channel_id
		,create_date
		,accttype_id
		,test_mark
		,free_mark
		,enterprise_mark
		,recreate_mark
		,VALID_DATE
		,EXPIRE_DATE
	)
	select
		 a.user_id
		,a.cust_id
		,a.user_type usertype_id
		,a.brand_id
		,coalesce(b.crm_brand_id2,10) crm_brand_id2
		,a.userstatus_id
		,case when Locate('1',a.os_sts)=0     then 0  when Locate('1',a.os_sts)=1     then 1
	                        when Locate('1',a.os_sts)=2     then 2  when Locate('1',a.os_sts)=3     then 3
	                        when Locate('1',a.os_sts,12)=12 then 12 when Locate('1',a.os_sts)=11    then 11
	                        when Locate('1',a.os_sts,14)=14 then 14 when Locate('1',a.os_sts)=13    then 13
	                        when Locate('1',a.os_sts,16)=16 then 16 when Locate('1',a.os_sts)=15    then 15
	                        when Locate('1',a.os_sts)=26    then 26 when Locate('1',a.os_sts)=27    then 27
	                        when Locate('1',a.os_sts)=28    then 28 when Locate('1',a.os_sts)=29    then 29
	                        when Locate('1',a.os_sts)=31    then 31 when Locate('1',a.os_sts)=32    then 32
	                        when Locate('1',a.os_sts)=33    then 33 else 0 end as stopstatus_id
		,a.product_no
		,value(g.imsi,a.sphone_id) imsi
		,a.city_id
		,a.channel_id
		,date(a.create_date) create_date
		,coalesce(c.accttype_id,0) accttype_id
		,case when d.user_id is not null then 1 else 0 end test_mark
		,case when e.user_id is not null then 1 else 0 end free_mark
		,case when f.cust_id is not null and a.user_type in (1,2,9) and a.userstatus_id in (1,2,3,6,8) then 1 else 0 end enterprise_mark
		,case when a.user_type in (1,2,9) and h.userstatus_id not in (1,2,3,6,8) and a.userstatus_id in (1,2,3,6,8) and d.user_id is null then 1 else 0 end as recreate_mark
		,date(a.valid_date) valid_date
		,date(a.expire_date) expire_date
from
		${db_user}.dwd_product_$p_timestamp a
	left join
		${db_user}.map_pub_brand b on a.plan_id = b.plan_id
	left join
		${db_user}.dw_acct_msg_$p_timestamp c on a.acct_id = c.acct_id
	left join
		(select user_id from ${db_user}.dw_product_test_phone_ds where op_time = '${p_optime}' and sts = 1 and valid_date < '${nextday}' and expire_date >= '${nextday}' group by user_id) d on a.user_id = d.user_id
	left join
		(select user_id from ${db_user}.dw_product_free_ds where op_time = '${p_optime}' and valid_date < '${nextday}' and expire_date >= '${nextday}' group by user_id) e on a.user_id = e.user_id
	left join
		(select cust_id,level_def_mode from ${db_user}.dw_enterprise_member_ds where op_time = '${p_optime}' group by cust_id,level_def_mode) f on a.cust_id = f.cust_id
	left join
		${db_user}.DW_PRODUCT_INS_PROD_RES_IMSI_DS g on a.user_id = g.user_id
	left join
		${db_user}.Dw_product_bass1_${lastday} h on a.user_id = h.user_id
	where
		a.plan_id <> 0"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		puts "errmsg:$errmsg"
		trace_sql $errmsg 1002
		return -1
	}
	aidb_commit $conn
# #===============================================================================================
# # step 5: 为user_id建索引
# #===============================================================================================
	set sqlbuf "create index ${db_user}.i_usrb1${p_timestamp}_1 on ${db_user}.dw_product_bass1_${p_timestamp}(user_id);"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1031
		return -1
	}
	aidb_commit $conn
# #===============================================================================================
# # step 6: 为product_no建索引
# #===============================================================================================
	set sqlbuf "create index ${db_user}.i_usrb1${p_timestamp}_2 on ${db_user}.dw_product_bass1_${p_timestamp}(product_no);"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1032
		return -1
	}
	aidb_commit $conn
# #===============================================================================================
# # step 7: 做runstats
# #===============================================================================================
	#exec db2 connect to bassdb user bass2 using bass2
	#exec db2 runstats on table ${db_user}.dw_product_bass1_${p_timestamp} with distribution on key columns and indexes all tablesample system(30)
	
	#	做runstats：
	aidb_runstats ${db_user}.dw_product_bass1_${p_timestamp} 3	
	
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1033
		return -1
	}
   aidb_commit $conn 
   
   
 	#	20100926 liuzhilong增加处理流程,防止用户资料表出现重复  
   	set sqlbuf "select count(*),count(distinct user_id) from ${db_user}.dw_product_bass1_${p_timestamp}"
   	
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

	return 0
}
