#****************************************************************************************
# ** 程序名称: stat_channel_reward_0002.tcl
# ** 程序功能: 西藏酬金汇总统计
# ** 运行粒度: 日
# ** 运行示例:  crt_basetab.sh stat_channel_reward_0002.tcl 2011-03-01
# ** 创建时间:  2010-6-22
# ** 创 建 人:  fuzl
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **           2010-07-27     Fuzl       增加地理类型字段 geography_type
# **           2010-11-25     Fuzl       营销活动新用户去了and e.month_new_mark=1条件
# **		     	 2011-03-22     Zhuwei	#营销活动新用户放号酬金 剔除活动表的重复数据，新增用户状态USERSTATUS_ID IN (1,2,3,6,8)
# **           2011-04-26     caiqiao    删除西藏酬金汇总统计2010报表营销活动新用户放号酬金项,在西藏酬金汇总统计2010报表、酬金业务量汇总表中增加外置自助终端缴费酬金项
# ** Copyright(c) 2010 AsiaInfo Technologies (China), Inc.
# ** All Rights Reserved.
#****************************************************************************************
proc deal { p_optime p_timestamp } {
	global conn
	global handle
	global env

	if [ catch { set handle [ aidb_open $conn ] } errmsg ] {
		trace_sql $errmsg 1000
		return -1
	}
	if { [ sql_main $p_optime $p_timestamp ]  != 0} {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	return 0
}
proc sql_main { p_optime p_timestamp } {
	   global conn
	   global handle
	   global env

	   #获取数据库用户
	   set DB2_USER "bass2"

 	   set	  date_optime	[ai_to_date $p_optime]
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   puts   $p_optime
	   set    op_month   "$year$month"
	   set    last_day_month [GetLastDay [GetNextMonth $op_month]01]
     set    last_month [GetLastMonth [string range $op_month 0 5]]01
     scan   $last_month  "%04s%02s%02s" last_month_year last_month_month last_month_day
     set    next_month [GetNextMonth [string range $op_month 0 5]]01
     scan   $next_month  "%04s%02s%02s" next_month_year next_month_month next_month_day



     set   dwd_channel_dept_yyyymmdd           "bass2.dwd_channel_dept_${last_day_month}"
   # set   dwd_channel_dept_yyyymmdd           "bass2.dwd_channel_dept_20120108"
     set   stat_channel_reward_detail_yyyymm   "bass2.stat_channel_reward_detail_$year$month"
     set   dw_product_user_promo_yyyymm        "bass2.dw_product_user_promo_$year$month"
     set   dw_product_yyyymm                   "bass2.dw_product_$year$month          "
     set   channel_user_reward_yyyymm          "bass2.channel_user_reward_$year$month "
     set   channel_nbusi_reward_yyyymm         "bass2.channel_nbusi_reward_$year$month"
     set   channel_charge_reward_yyyymm        "bass2.channel_charge_reward_$year$month"
     #维表
     set   stat_channel_reward_0002_t_lkp      "bass2.stat_channel_reward_0002_t_lkp" 

###===================================================================================================
## step. 创建临时索引
###====================================================================================================

 set sql_buf "create index bass2.reward_0002_01 on $dw_product_yyyymm
              (product_no asc, user_id asc) allow reverse scans collect sampled detailed statistics "
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

 set sql_buf "create index bass2.reward_0002_02 on $dw_product_user_promo_yyyymm
              (user_id asc) allow reverse scans collect sampled detailed statistics"
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
	  
###===================================================================================================
## step1. 创建临时结果表
###====================================================================================================

	   set sql_buf " declare global temporary table session.stat_channel_reward_0002_tmp
	               (city_id            varchar(7),
                  county_id       	 varchar(20),
                  geography_type     integer,
                  channel_id         bigint,
                  channel_name       varchar(128),
                  channel_type       INTEGER,
                  channel_type_dtl2  INTEGER,
                  t_index_id         smallint,
                  result             decimal(12,2)
                  )partitioning key (channel_id) using hashing
                  with replace on commit preserve rows not logged in tbs_user_temp"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
   # 签约放号
   # 1	放号酬金	当月酬金    
   # 2	放号酬金	第四月酬金  
   # 3	放号酬金	第七月酬金 
   # TD放号 
   # 4	TD放号酬金	当月酬金  
   # 5	TD放号酬金	第四月酬金
   # 6	TD放号酬金	第七月酬金
   
	  set sql_buf "insert into session.stat_channel_reward_0002_tmp (city_id
                                                              ,county_id
                                                              ,geography_type
                                                              ,channel_id
                                                              ,channel_name
                                                              ,channel_type
                                                              ,channel_type_dtl2
                                                              ,t_index_id
                                                              ,result
                                                             )
                  select  a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when a.rule_type='1'  then
                                 case when c.use_months= 2 then 1
                                      when c.use_months= 4 then 2
                                      when c.use_months= 7 then 3
                                 end
                               when a.rule_type='11' then
                                 case when c.use_months= 2  then 4
                                      when c.use_months= 4  then 5
                                      when c.use_months= 7  then 6
                                 end
                           end
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a
                       left join  $dwd_channel_dept_yyyymmdd b on a.channel_id=b.channel_id,
                       $channel_user_reward_yyyymm c 
                  where a.phone_id=c.phone_id and a.unique_id=c.unique_id and a.rule_type in ('1','11')
                  group by a.city_id
                         ,a.county_id
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when a.rule_type='1'  then
                                 case when c.use_months= 2 then 1
                                      when c.use_months= 4 then 2
                                      when c.use_months= 7 then 3
                                 end
                               when a.rule_type='11' then
                                 case when c.use_months= 2  then 4
                                      when c.use_months= 4  then 5
                                      when c.use_months= 7  then 6
                                 end
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
    # 新业务
    # 7	新业务酬金	当月酬金  
    # 8	新业务酬金	第三月酬金
    # 9	新业务酬金	第五月月酬金
	  set sql_buf "insert into session.stat_channel_reward_0002_tmp (city_id
                                                              ,county_id
                                                              ,geography_type
                                                              ,channel_id
                                                              ,channel_name
                                                              ,channel_type
                                                              ,channel_type_dtl2
                                                              ,t_index_id
                                                              ,result
                                                             )
                  select  a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when c.use_months= 2 then 7
                               when c.use_months= 3 then 8
                               when c.use_months= 5 then 9
                           end     
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a
                       left join  $dwd_channel_dept_yyyymmdd b on a.channel_id=b.channel_id,
                       $channel_nbusi_reward_yyyymm c 
                  where a.phone_id=c.phone_id and a.unique_id=c.unique_id and  a.rule_type = '8'
                  group by a.city_id
                         ,a.county_id
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when c.use_months= 2 then 7
                               when c.use_months= 3 then 8
                               when c.use_months= 5 then 9
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
	  
    # 10	普通业务酬金	            
    # 11	前台缴费酬金	            
    # 12	自助终端缴费酬金	        
    # 13	营销活动老用户预存费酬金	
    # 17	银行代收费酬金	          
    # 18	随E行酬金	                
    # 19	预提卡酬金	              
    # 20	空中充值酬金	            
    # 21	充值卡酬金	              
    # 22	TD公话超市酬金	          
    # 23	公话超市酬金              
	  set sql_buf "insert into session.stat_channel_reward_0002_tmp (city_id
                                                              ,county_id
                                                              ,geography_type
                                                              ,channel_id
                                                              ,channel_name
                                                              ,channel_type
                                                              ,channel_type_dtl2
                                                              ,t_index_id
                                                              ,result
                                                             )
                  select  a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when a.rule_type='7'   then 10
                               when a.rule_type='12'  then 11
                               when a.rule_type='13' and   RULE_INFO_ID not in (1932 )   then 12
                               when a.rule_type='14'  then 13
                               when a.rule_type='4'   then 17
                               when a.rule_type='9'   then 18
                               when a.rule_type='2'   then 19
                               when a.rule_type='5'   then 20
                               when a.rule_type='6'   then 21
                               when a.rule_type='15'  then 22
                               when a.rule_type='10'  then 23
                               when a.rule_type='3'   then 24
                           end
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a,
                       $dwd_channel_dept_yyyymmdd b
                  where a.channel_id=b.channel_id  
                        and a.rule_type in ('2','3','4','5','6','7','9','10','12','13','14','15')
                  group by a.city_id
                         ,a.county_id
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when a.rule_type='7'   then 10
                               when a.rule_type='12'  then 11
                                when a.rule_type='13' and   RULE_INFO_ID not in (1932 )   then 12
                               when a.rule_type='14'  then 13
                               when a.rule_type='4'   then 17
                               when a.rule_type='9'   then 18
                               when a.rule_type='2'   then 19
                               when a.rule_type='5'   then 20
                               when a.rule_type='6'   then 21
                               when a.rule_type='15'  then 22
                               when a.rule_type='10'  then 23
                               when a.rule_type='3'   then 24
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

   ##营销活动新用户放号酬金
   ## 14	营销活动新用户放号酬金	当月酬金  
   ## 15	营销活动新用户放号酬金	第四月酬金
   ## 16	营销活动新用户放号酬金	第七月酬金
	  # set sql_buf "insert into session.stat_channel_reward_0002_tmp ( city_id
    #                                                          ,county_id
    #                                                          ,geography_type
    #                                                          ,channel_id
    #                                                          ,channel_name
    #                                                          ,channel_type
    #                                                          ,channel_type_dtl2
    #                                                          ,t_index_id
    #                                                          ,result
    #                                                         )
    #              select  a.city_id
    #                     ,rtrim(ltrim(char(a.county_id)))
    #                     ,a.geography_type
    #                     ,a.channel_id
    #                     ,a.channel_name
    #                     ,b.channel_type
    #                     ,b.dept_type_dtl
    #                     ,case when c.use_months= 2 then 14
    #                           when c.use_months= 4 then 15
    #                           when c.use_months= 7 then 16
    #                      end
    #                    ,sum(a.fee)
    #              from $stat_channel_reward_detail_yyyymm a
    #                   left join $channel_user_reward_yyyymm c on a.phone_id=c.phone_id,
    #                   $dwd_channel_dept_yyyymmdd b,
    #                   (SELECT DISTINCT USER_ID FROM $dw_product_user_promo_yyyymm) d,
    #                   $dw_product_yyyymm e
    #              where a.phone_id=e.product_no and a.channel_id=b.channel_id
    #                    and a.unique_id=c.unique_id and a.rule_type='1'
    #                    and d.user_id=e.user_id 
    #                    and e.userstatus_id in (1,2,3,6,8)
    #              group by a.city_id
    #                     ,rtrim(ltrim(char(a.county_id)))
    #                     ,a.geography_type
    #                     ,a.channel_id
    #                     ,a.channel_name
    #                     ,b.channel_type
    #                     ,b.dept_type_dtl
    #                     ,case when c.use_months= 2 then 14
    #                           when c.use_months= 4 then 15
    #                           when c.use_months= 7 then 16
    #                      end"
	  # puts ${sql_buf}
	  # if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	  # 	puts "errmsg:$errmsg"
	  # 	return -1
	  # }
	  #aidb_commit $conn

   #外置自助终端缴费酬

 set sql_buf "insert into session.stat_channel_reward_0002_tmp ( city_id
                                                              ,county_id
                                                               ,geography_type
                                                              ,channel_id
                                                              ,channel_name
                                                               ,channel_type
                                                               ,channel_type_dtl2
                                                               ,t_index_id
                                                               ,result
                                                              )                  
                  select  a.city_id
                         ,rtrim(ltrim(a.county_id))
                         ,a.geography_type
                         ,a.channel_id  
                         ,a.channel_name
                         ,b.channel_type  
                         ,b.channel_type_dtl    
                         ,25 
                         
                         ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a
                       left join $channel_charge_reward_yyyymm b  on a.phone_id=b.phone_id
                  where a.unique_id=b.unique_id and a.rule_type in ('13')  and RULE_INFO_ID in (1932 ) 
                  group by 
                         a.city_id
                         ,rtrim(ltrim(a.county_id))
                         ,a.geography_type
                         ,a.channel_id  
                         ,a.channel_name
                         ,b.channel_type  
                         ,b.channel_type_dtl    
                  
                  
                  
                  
                  "
	   puts ${sql_buf}    
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1         
	   }                  
	  aidb_commit $conn  




  # 汇总合计
	  set sql_buf "insert into session.stat_channel_reward_0002_tmp ( city_id
                                                                    ,county_id
                                                                    ,geography_type
                                                                    ,channel_id
                                                                    ,channel_name
                                                                    ,channel_type
                                                                    ,channel_type_dtl2
                                                                    ,t_index_id
                                                                    ,result
                                                                   )
                  select  city_id
                         ,county_id
                         ,geography_type
                         ,channel_id
                         ,channel_name
                         ,channel_type
                         ,channel_type_dtl2
                         ,0
                         ,sum(result)
                  from session.stat_channel_reward_0002_tmp
                  where t_index_id not in (14,15,16)
                  group by city_id
                          ,county_id
                          ,geography_type
                          ,channel_id
                          ,channel_name
                          ,channel_type
                          ,channel_type_dtl2"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

    #删除旧数据
	   set sql_buf "delete from  bass2.stat_channel_reward_0002 where op_time=$year$month"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

	  #将数据插入结果表
	  set sql_buf "insert into bass2.stat_channel_reward_0002 ( op_time
	                                                           ,city_id
                                                             ,county_id
                                                             ,geography_type
                                                             ,channel_id
                                                             ,channel_name
                                                             ,channel_type
                                                             ,channel_type_dtl2
                                                             ,t_index_id
                                                             ,result
                                                            )
                  select  $year$month
                         ,city_id
                         ,county_id
                         ,geography_type
                         ,channel_id
                         ,channel_name
                         ,channel_type
                         ,channel_type_dtl2
                         ,t_index_id
                         ,sum(result)
                  from session.stat_channel_reward_0002_tmp
                  group by city_id
                         ,county_id
                         ,geography_type
                         ,channel_id
                         ,channel_name
                         ,channel_type
                         ,channel_type_dtl2
                         ,t_index_id "
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

###===================================================================================================
## step. 删除临时索引
###====================================================================================================

 set sql_buf "drop index bass2.reward_0002_01 "
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

 set sql_buf "drop index bass2.reward_0002_02"
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
	  
	  return 0
}
 