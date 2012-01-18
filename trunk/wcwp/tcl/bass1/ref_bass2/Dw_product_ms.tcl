
#/*******************************************************************************
#	源程序名：Dw_product_ms.tcl
#	简要说明：生成用户资料月表
#	程序流程：dw_call_yyyymmdd:dw_product_score_yyyymm:dw_acct_should_yyyymm:dw_acct_owe_yyyymm --> dw_product_yyyymm
#  作者    ：杨菲
#  运行方法：ds Dw_product_ms.tcl 2007-03-01
#  备注    ：dw_product_score_$optmonth表中同一user_id,city与状态的有多条记录,所以分去重
#  修改    ：增加：本月使用短信用户标识(month_sms_mark)及本月使用点对点短信用户标识(month_p2p_sms_mark) 20090706 add by lizz
# 		zhuwei	20110412 根据工单"关于修改系统2经表credit、snapshot_mark 等字段问题修改 "
#		修改snapshot_mark,中高端用户标志；从上月表取月消费120月及以上，主叫计费时长100分钟及以上，剔除测试公免用户 
#    20110601 zhuwei 根据张阳要求变更snapshot_mark,中高端用户标志的统计口径，取当月消费120月及以上，主叫计费时长100分钟及以上，剔除测试公免用户 为1
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

	set db_user BASS2
	
	set tbs_3h $env(TBS_3H)
	
	set optmonth [string range $p_timestamp 0 5]
	
	set last_day_month [GetLastDay [GetNextMonth [string range $p_timestamp 0 5]]01]
	
	set lastmonth [GetLastMonth [string range $p_timestamp 0 5]]
	
	#获取数据月份上上月当日yyyy-mm-dd
	set lls_p_optime [ clock format [ clock scan "${p_timestamp} - 2 months" ] -format "%Y-%m-%d" ]
	#获取数据月份上上月yyyymm
	set lasttwomonth [ string range $lls_p_optime 0 3 ][ string range $lls_p_optime 5 6 ]
	
	set nextmonth [string range [GetNextMonth [string range $p_timestamp 0 5]] 0 3]-[string range [GetNextMonth [string range $p_timestamp 0 5]] 4 5]
	
	scan $p_optime "%04s-%02s-%02s" year month day
	
	set lastmonths $month
	
	#	M1、删除当月已存在的数据表
	
	set sqlbuf "drop table ${db_user}.dw_product_$optmonth;"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		#trace_sql $errmsg 1001
	}
	
	aidb_commit $conn
	
	#	M2、创建当月数据表
	
   set sqlbuf "create table ${db_user}.dw_product_$optmonth like ${db_user}.dw_product_yyyymm in $tbs_3h
               index in tbs_index partitioning key (user_id) using hashing not logged initially;"
   
   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1002
		return -1
	}
   
   aidb_commit $conn
   
   #	M3、建立临时表
	
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
                     from ${db_user}.dw_newbusi_call_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_sms_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_ismg_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wap_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_gprs_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_mms_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_wlan_$optmonth
                     union all
                     select user_id
                     from ${db_user}.dw_newbusi_kj_$optmonth) a
               group by user_id;"
	puts $sqlbuf
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
	
	#	M6、建立临时表
	
	set sqlbuf "declare global temporary table session.t_dw_product_2
               (
               	user_id              varchar(20)
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1006
		return -1
	}
	
	aidb_commit $conn
	
	#	M7、将当年新增的用户插入临时表
	
	set sqlbuf1 " select user_id
		          from ${db_user}.dw_product_$last_day_month
		          where month_new_mark=1 "
	
	for {set month 1} {$month < $lastmonths} {incr month} {
        	
     	set currmonth [format "%4s%02d" $year $month]
     	
	   set sqlbuf0 " union all
		              select user_id
		              from ${db_user}.dw_product_$currmonth
		              where month_new_mark=1 "
          
	   set sqlbuf1 ${sqlbuf1}${sqlbuf0}

	}
	
	set sqlbuf "insert into session.t_dw_product_2(user_id)
	            select user_id from (${sqlbuf1}) a group by user_id "
	            
	puts $sqlbuf
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1007
		return -1
	}
	
	aidb_commit $conn
	
	#	M8、建索引
	
	set sqlbuf "create index session.idx_product_2_1 on session.t_dw_product_2(user_id);"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1008
		return -1
	}
	
	aidb_commit $conn
	
	#	M9、建立临时表 (生成：本月使用短信用户标识(month_sms_mark)及本月使用点对点短信用户标识(month_p2p_sms_mark))
	
	set sqlbuf "declare global temporary table session.t_dw_product_3
               (
               	user_id              varchar(20),
               	month_sms_mark		 smallint,
               	month_p2p_sms_mark	 smallint
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
	
	#	M10、将短信标识记录插入临时表
	
	set sqlbuf "insert into session.t_dw_product_3(user_id,month_sms_mark,month_p2p_sms_mark)
			select 
				a.user_id,
				case when sum(a.month_sms_mark)>=1 		then 1 else 0 end as month_sms_mark,
				case when sum(a.month_p2p_sms_mark)>=1 	then 1 else 0 end as month_p2p_sms_mark
			from 
			(
			select 
				user_id,
				1 as month_sms_mark,
				case when a.svcitem_id in (200001,200002,200003,200004,200005) then 1 else 0 end as month_p2p_sms_mark
			from  
				bass2.dw_newbusi_sms_$optmonth a
			where a.svcitem_id in (200001,200002,200003,200004,200005,200007)
			union all
			select 
				user_id,
				1 as month_sms_mark,
				0 as month_p2p_sms_mark
			from 
				bass2.dw_newbusi_ismg_$optmonth a
			where a.svcitem_id in (300001,300002,300003,300004,300007,300010,300011,300012,300013)
			union all
			select 
				user_id,
				1 as month_sms_mark,
				0 as month_p2p_sms_mark
			from 
				bass2.dw_newbusi_ismg_$optmonth a
			where a.sp_code = '931007'
			union all
			select 
				user_id,
				1 as month_sms_mark,
				0 as month_p2p_sms_mark
			from 
				bass2.dw_newbusi_call_$optmonth a
			where a.svcitem_id in (100002,100026,100003,100021,100025)
			)a
			group by
				a.user_id"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1004
		return -1
	}
	
	aidb_commit $conn
   
     #	MADD1、声明临时表用于处理中高端用户拍照标志
   set sqlbuf "declare global temporary table session.t_dw_product_4
               (
               	user_id              varchar(30),
               	fact_fee		    decimal(12,2),
               	call_duration_m  integer
               )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
	
	     #	MADD2、中高端用户拍照标志指标插入临时表
   set sqlbuf "insert into session.t_dw_product_4 (user_id,fact_fee,call_duration_m)
					select a.user_id, a.fact_fee , b.call_duration_m 
					from  	(select user_id,sum(fact_fee) fact_fee from bass2.dw_acct_shoulditem_$optmonth    group by 	user_id		) a
   					inner join (select user_id,coalesce(sum(call_duration_m),0) call_duration_m   from  bass2.dw_call_$optmonth
																		WHERE CALLTYPE_ID=0
                                                                        group by user_id
																		) b on a.user_id=b.user_id ; "
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn
	
	     #	MADD3、中高端用户拍照临时表创建索引
   set sqlbuf " create index session.idx_product_4_1 on session.t_dw_product_4(user_id); "
	puts $sqlbuf
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1003
		return -1
	}
	
	aidb_commit $conn	
	
   #	M11、生成dw_product_yyyymm数据表
		
	set sqlbuf "insert into ${db_user}.dw_product_$optmonth
               (user_id        ,cust_id        ,acct_id             ,age           ,age_id         ,sex_id             ,
                education_id   ,custtype_id    ,custclass_id        ,credit        ,credit_id      ,product_no         ,
                imsi           ,sim_id         ,sim_type            ,city_id       ,city_id1       ,county_id          ,
                channel_id     ,warn_fee       ,so_nbr              ,channeltype_id,accttype_id    ,user_online        ,
                user_online_id ,brand_id       ,crm_brand_id1       ,crm_brand_id2 ,crm_brand_id3  ,plan_id            ,
                userstatus_id  ,stopstatus_id  ,usertype_id         ,month_new_mark,month_off_mark ,month_recreate_mark,
                month_new_count,month_off_count,month_recreate_count,year_new_mark ,month_call_mark,month_newbusi_mark ,
                active_mark    ,zero_mark      ,vip_mark            ,single_mark   ,vpmn_mark      ,enterprise_mark    ,
                import_ent_mark,free_mark      ,test_mark           ,bill_mark     ,snapshot_mark  ,recreate_mark      ,
                owe_mark       ,chgbrand_mark  ,stop_1mon_mark      ,create_date   ,valid_date     ,sts_date           ,
                expire_date    ,total_score    ,change_score        ,call_counts   ,call_duration  ,call_duration_m    ,
                call_duration_s,fact_fee       ,local_fee           ,toll_fee      ,roam_fee       ,month_fee          ,
                incr_value_fee ,data_voice_fee ,data_info_fee       ,other_fee     ,stat_fee       ,not_stat_fee       ,
                balance_fee    ,owe_fee		   ,month_sms_mark		,month_p2p_sms_mark)
                select a.user_id        ,a.cust_id          ,a.acct_id             ,a.age            ,a.age_id         ,a.sex_id             ,
                       a.education_id   ,a.custtype_id      ,a.custclass_id        ,a.credit         ,a.credit_id      ,a.product_no         ,
                       a.imsi           ,a.sim_id           ,a.sim_type            ,a.city_id        ,a.city_id1       ,a.county_id          ,
                       a.channel_id     ,a.warn_fee         ,a.so_nbr              ,a.channeltype_id ,a.accttype_id    ,a.user_online        ,
                       a.user_online_id ,a.brand_id         ,a.crm_brand_id1       ,a.crm_brand_id2  ,a.crm_brand_id3  ,a.plan_id            ,
                       a.userstatus_id  ,a.stopstatus_id    ,a.usertype_id         ,a.month_new_mark ,a.month_off_mark ,a.month_recreate_mark,
                       a.month_new_count,a.month_off_count  ,a.month_recreate_count,
                       case when j.user_id is not null then 1 else 0 end year_new_mark,
                       a.month_call_mark,a.month_newbusi_mark  ,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and a.test_mark<>1 and (b.user_id is not null or i.user_id is not null) 
                            then 1 else 0 end as active_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) and a.test_mark<>1 and b.user_id is null and i.user_id is null 
                            then 1 else 0 end as zero_mark,
                       a.vip_mark       ,a.single_mark      ,a.vpmn_mark           ,a.enterprise_mark,a.import_ent_mark,a.free_mark          ,
                       a.test_mark      ,
                       case when (b.basecall_fee+b.toll_fee+b.info_fee+b.other_fee)>0 then 1 else 0 end as bill_mark,
                       case when m.fact_fee>=120 and m.call_duration_m >= 100 and a.free_mark <> 1 and a.test_mark <> 1 and a.usertype_id in (1,2,9) and a.userstatus_id in (1,2,3,6,8) then 1 else 0 end  snapshot_mark  ,
                       case when a.month_recreate_count>0 then 1 else 0 end recreate_mark,
                       case when a.usertype_id in (1,2,9) and coalesce(c.unpay_fee,0)>0 then 1 else 0 end as owe_mark,
                       case when h.user_id is not null then 1 else 0 end as chgbrand_mark,
                       case when a.usertype_id in (1,2,9) and a.userstatus_id=1 and a.test_mark<>1 and a.stopstatus_id<>0 and 
                                 (coalesce(e.stat_fee,0)-coalesce(e.month_fee,0))<=0 then 1 else 0 end as stop_1mon_mark,
                       a.create_date    ,a.valid_date       ,a.sts_date            ,a.expire_date    ,
                       0 as total_score,
                       0 as change_score,
                       a.mtd_call_counts,a.mtd_call_duration,a.mtd_call_duration_m,a.mtd_call_duration_s,
                       coalesce(e.fact_fee,0) as fact_fee,
                       coalesce(e.local_fee,0) as local_fee,
                       coalesce(e.toll_fee,0) as toll_fee,
                       coalesce(e.roam_fee,0) as roam_fee,
                       coalesce(e.month_fee,0) as month_fee,
                       coalesce(e.incr_value_fee,0) as incr_value_fee,
                       coalesce(e.data_voice_fee,0) as data_voice_fee,
                       coalesce(e.data_info_fee,0) as data_info_fee,
                       coalesce(e.other_fee,0) as other_fee,
                       coalesce(e.stat_fee,0) as stat_fee,
                       coalesce(e.not_stat_fee,0) as not_stat_fee,
                       a.balance_fee    ,
                       coalesce(c.unpay_fee,0) as owe_fee,
                       coalesce(k.month_sms_mark,0) as month_sms_mark,
                       coalesce(k.month_p2p_sms_mark,0) as month_p2p_sms_mark
                from ${db_user}.dw_product_$last_day_month a left join (select user_id,
                                                                               sum(call_counts) as call_counts,
                                                                               sum(call_duration) as call_duration,
                                                                               sum(call_duration_m) as call_duration_m,
                                                                               sum(call_duration_s) as call_duration_s,
                                                                               sum(basecall_fee) as basecall_fee,
                                                                               sum(toll_fee) as toll_fee,
                                                                               sum(info_fee) as info_fee,
                                                                               sum(other_fee) as other_fee
                                                                        from ${db_user}.dw_call_$optmonth
                                                                        group by user_id) b on a.user_id=b.user_id
                                                             left join (select user_id,sum(unpay_fee) as unpay_fee 
                                                                        from ${db_user}.dw_acct_owe_$optmonth
                                                                        group by user_id) c on a.user_id=c.user_id
                                                             left join ${db_user}.dw_acct_should_$optmonth e on a.user_id=e.user_id
                                                             left join (select user_id from ${db_user}.dw_product_brandchg_$optmonth group by user_id) h on a.user_id=h.user_id
                                                             left join session.t_dw_product_1 i on a.user_id=i.user_id
                                                             left join session.t_dw_product_2 j on a.user_id=j.user_id
                                                             left join session.t_dw_product_3 k on a.user_id=k.user_id
                                                             left join session.t_dw_product_4 m on a.user_id=m.user_id  ;"
               
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1009
		return -1
	}
	
	aidb_commit $conn
	
	#	M12、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${optmonth}_1 on ${db_user}.dw_product_${optmonth}(user_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1010
		return -1
	}
	
	aidb_commit $conn
	
	#	M13、建索引
	
	set sqlbuf "create index ${db_user}.i_usr_${optmonth}_2 on ${db_user}.dw_product_${optmonth}(cust_id);"
	
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
  		puts $errmsg
		trace_sql $errmsg 1011
		return -1
	}
	
	aidb_commit $conn
	
	#	M14、做runstats：
	exec db2 connect to bassdb user bass2 using bass2
	exec db2 runstats on table ${db_user}.dw_product_${optmonth} on all columns and indexes all
	
   if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1012
      return -1
   }
   
   aidb_commit $conn 
   
	aidb_close $handle
	
   return 0
   
}

