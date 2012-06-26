#****************************************************************************************
# ** 程序名称: stat_channel_reward_0019.tcl
# ** 程序功能: 西藏酬金业务量汇总表
# ** 运行粒度: 日
# ** 运行示例:  crt_basetab.sh stat_channel_reward_0019.tcl 2010-11-01
# ** 创建时间:  2010-6-22
# ** 创 建 人:  fuzl
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **           2010-12-03     Fuzl       增加邮政代收费
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
     set   stat_channel_reward_detail_yyyymm   "bass2.stat_channel_reward_detail_$year$month"
     set   dw_product_yyyymm                   "bass2.dw_product_$year$month          "
     set   channel_user_reward_yyyymm          "bass2.channel_user_reward_$year$month "
     set   channel_nbusi_reward_yyyymm         "bass2.channel_nbusi_reward_$year$month"
     set   channel_bbusi_reward_yyyymm         "bass2.channel_bbusi_reward_$year$month"
     set   channel_charge_reward_yyyymm         "bass2.channel_charge_reward_$year$month"  
     
     #维表
     set   dim_prod_up_product_item            "BASS2.dim_prod_up_product_item "
     set   dim_product_item                    "BASS2.dim_product_item         " 
     
     #目标表
     set   stat_channel_reward_0019         "bass2.stat_channel_reward_0019"
###===================================================================================================
## step. 创建临时索引
###====================================================================================================
 set sql_buf "DROP index bass2.reward_0019_01 "
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }
	  aidb_commit $conn
	  
 set sql_buf "create index bass2.reward_0019_01 on  $channel_nbusi_reward_yyyymm
             (channel_type asc, channel_type_dtl asc, busi_code
             asc, busi_id asc, phone_id asc, unique_id asc)"
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

 set sql_buf "DROP index bass2.reward_0019_02 "
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }
	  aidb_commit $conn
 set sql_buf "create index bass2.reward_0019_02 on $dim_prod_up_product_item
             (product_item_id asc, name asc)"                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
###==========================================================================================	       
     exec db2 connect to bassdb user bass2 using bass2
	   exec db2 runstats on table $channel_nbusi_reward_yyyymm
	   exec db2 runstats on table $dim_prod_up_product_item
	   exec db2 terminate 

###===================================================================================================
## step1. 创建临时结果表
###====================================================================================================

	   set sql_buf " declare global temporary table session.stat_channel_reward_0019_tmp
	               (city_id            varchar(7),
                  county_id       	 varchar(20),
                  geography_type     integer,
                  channel_id         bigint,
                  channel_name       varchar(128),
                  channel_type       integer,
                  channel_type_dtl2  integer,
                  busi_notes         varchar(128),
                  months             varchar(128), 
                  counts             integer,
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
   # 1	放号酬金	当月  酬金    
   # 2	放号酬金	第四月酬金  
   # 3	放号酬金	第七月酬金 
   # TD放号 
   # 4	TD放号酬金	当月酬金  
   # 5	TD放号酬金	第四月酬金
   # 6	TD放号酬金	第七月酬金
   
	  set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
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
                                 case when c.brand= 1 then '全球通签约放号'
                                      when c.brand= 4 then '动感地带签约放号'
                                      when c.brand= 5 then '神州行签约放号'
                                 end
                               when a.rule_type='11' then
                                 case when c.brand= 1  then '全球通TD放号'  
                                      when c.brand= 4  then '动感地带TD放号'
                                      when c.brand= 5  then '神州行TD放号'  
                                 end
                           end
                          ,case when c.use_months=2 then '当月'
                                when c.use_months=4 then '第四月'
                                when c.use_months=7 then '第七月'
                           end 
                          ,count(*)
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a
                       left join  $dwd_channel_dept_yyyymmdd b on a.channel_id=b.channel_id,
                       $channel_user_reward_yyyymm c 
                  where a.phone_id=c.phone_id and a.unique_id=c.unique_id and a.rule_type in ('1','11')
                  group by a.city_id
                          ,rtrim(ltrim(char(a.county_id)))
                          ,a.geography_type
                          ,a.channel_id
                          ,a.channel_name
                          ,b.channel_type
                          ,b.dept_type_dtl
                          ,case when a.rule_type='1'  then
                                  case when c.brand= 1 then '全球通签约放号'
                                       when c.brand= 4 then '动感地带签约放号'
                                       when c.brand= 5 then '神州行签约放号'
                                  end
                                when a.rule_type='11' then
                                  case when c.brand= 1  then '全球通TD放号'  
                                       when c.brand= 4  then '动感地带TD放号'
                                       when c.brand= 5  then '神州行TD放号'  
                                  end
                            end
                          ,case when c.use_months=2 then '当月'
                                when c.use_months=4 then '第四月'
                                when c.use_months=7 then '第七月'
                           end "
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
	  set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
                                                                  ,result
                                                                 )
              select a.city_id              ,
                     ltrim(rtrim(a.county_id)),
                     a.geography_type       ,
                     a.channel_id           ,
                     a.channel_name         ,
                     b.channel_type         ,
                     b.channel_type_dtl     ,
                     '新业务-'||d.prod_name            ,
                     case when b.use_months=2 then '当月'
                          when b.use_months=3 then '第三月'
                          when b.use_months=5 then '第五月'
                     end           ,
                     count(1)               ,
                     sum(a.fee)                
                from  $stat_channel_reward_detail_yyyymm a,
                      $channel_nbusi_reward_yyyymm b,
                      $dim_product_item  d
                where a.unique_id=b.unique_id and a.phone_id=b.phone_id  
                      and  a.rule_type='8' and b.note=1  
                      and b.prod_id = d.prod_id
                group  by  a.city_id              ,
                           ltrim(rtrim(a.county_id)),
                           a.geography_type       ,
                           a.channel_id           ,
                           a.channel_name         ,
                           b.channel_type         ,
                           b.channel_type_dtl     ,
                           d.prod_name            ,
                           case when b.use_months=2 then '当月'
                                when b.use_months=3 then '第三月'
                                when b.use_months=5 then '第五月'
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn          
    
	  set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
                                                                  ,result
                                                                 )
                select a.city_id              ,
                       ltrim(rtrim(a.county_id)),
                       a.geography_type       ,
                       a.channel_id           ,
                       a.channel_name         ,
                       b.channel_type         ,
                       b.channel_type_dtl     ,
                       case when b.serv_code = 'XHXW'  then '新业务-移动新华新闻'
                            when b.serv_code = 'FXCHY' then '新业务-飞信功能'
                            when b.serv_code = 'NQXW'  then '新业务-那曲新闻'
                            when b.serv_code = 'SJDT'  then '新业务-手机地图'
                            when b.serv_code = 'SJDH'  then '新业务-手机导航'
                            when b.serv_code = 'SJB'   then '新业务-手机报'
                            when b.serv_code = 'SJDS'  then '新业务-手机电视'
                            when b.serv_code = 'SJZF'  then '新业务-手机支付'
                            else 'SP业务' 
                       end, 
                       case when b.use_months=2 then '当月'
                            when b.use_months=3 then '第三月'
                            when b.use_months=5 then '第五月'
                       end,
                       count(1)               ,
                       sum(a.fee)                  
               from  $stat_channel_reward_detail_yyyymm a,
                     $channel_nbusi_reward_yyyymm b 
               where a.unique_id=b.unique_id and a.phone_id=b.phone_id  
                   and  a.rule_type='8' and b.note=2
               group by a.city_id              ,
                       ltrim(rtrim(a.county_id)),
                       a.geography_type       ,
                       a.channel_id           ,
                       a.channel_name         ,
                       b.channel_type         ,
                       b.channel_type_dtl     ,
                       case when b.serv_code = 'XHXW'  then '新业务-移动新华新闻'
                            when b.serv_code = 'FXCHY' then '新业务-飞信功能'
                            when b.serv_code = 'NQXW'  then '新业务-那曲新闻'
                            when b.serv_code = 'SJDT'  then '新业务-手机地图'
                            when b.serv_code = 'SJDH'  then '新业务-手机导航'
                            when b.serv_code = 'SJB'   then '新业务-手机报'
                            when b.serv_code = 'SJDS'  then '新业务-手机电视'
                            when b.serv_code = 'SJZF'  then '新业务-手机支付'
                            else 'SP业务' 
                       end, 
                       case when b.use_months=2 then '当月'
                            when b.use_months=3 then '第三月'
                            when b.use_months=5 then '第五月'
                       end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn 
	  
    # 10	普通业务酬金
    set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
                                                                  ,result
                                                                 )
                select  a.city_id
                       ,rtrim(ltrim(a.county_id))
                       ,a.geography_type
                       ,a.channel_id
                       ,a.channel_name
                       ,b.channel_type
                       ,b.channel_type_dtl
                       ,case when b.busi_code=2  then '普通业务-跨区业务办理'
                        else '普通业务-'||c.name end
                       ,'当月'
                       ,count(*)
                       ,sum(a.fee)
                from $stat_channel_reward_detail_yyyymm a,
                     $channel_bbusi_reward_yyyymm b,  
                     $dim_prod_up_product_item c
                where a.unique_id=b.unique_id and a.phone_id=b.phone_id 
                      and a.rule_type='7' and b.busi_id=c.product_item_id
                group by a.city_id
                       ,rtrim(ltrim(a.county_id))
                       ,a.geography_type
                       ,a.channel_id
                       ,a.channel_name
                       ,b.channel_type
                       ,b.channel_type_dtl
                       ,case when b.busi_code=2  then '普通业务-跨区业务办理'
                        else '普通业务-'||c.name end"
	   puts ${sql_buf}   
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
    
    # 	邮政公司当月代收费酬金	            
    # 11	前台缴费酬金	            
    # 12	自助终端缴费酬金	        
    # 13	营销活动老用户预存费酬金
	  set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
                                                                  ,result
                                                             )
                  select  a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.channel_type_dtl
                         ,case when a.rule_type='3'   then '邮政公司当月代收费酬金' 
                               when a.rule_type='12'  then '前台缴费酬金'  
                               when a.rule_type='13'  then '自助终端缴费酬金'  
                               when a.rule_type='14'  then '营销活动老用户预存费酬金 ' 
                           end
                          ,'当月'
                          ,sum(b.pre_fee)
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a,
                       $channel_charge_reward_yyyymm b
                  where a.unique_id=b.unique_id  
                        and a.rule_type in ('3','12','13','14')
                        and b.pre_fee<>0
                  group by a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.channel_type_dtl
                         ,case when a.rule_type='3'   then '邮政公司当月代收费酬金' 
                               when a.rule_type='12'  then '前台缴费酬金'  
                               when a.rule_type='13'  then '自助终端缴费酬金'  
                               when a.rule_type='14'  then '营销活动老用户预存费酬金 ' 
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn    
    	
    # 17	银行代收费酬金	          
    # 18	随E行酬金	                
    # 19	预提卡酬金	              
    # 20	空中充值酬金	            
    # 21	充值卡酬金	              
    # 22	TD公话超市酬金	          
    # 23	公话超市酬金              
	  set sql_buf "insert into session.stat_channel_reward_0019_tmp (city_id
                                                                  ,county_id
                                                                  ,geography_type
                                                                  ,channel_id
                                                                  ,channel_name
                                                                  ,channel_type
                                                                  ,channel_type_dtl2
                                                                  ,busi_notes
                                                                  ,months
                                                                  ,counts
                                                                  ,result
                                                             )
                  select  a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case 
                               when a.rule_type='4'   then '银行代收费酬金'  
                               when a.rule_type='9'   then '随E行酬金'  
                               when a.rule_type='2'   then '预提卡酬金'  
                               when a.rule_type='5'   then '空中充值酬金'  
                               when a.rule_type='6'   then '充值卡酬金'  
                               when a.rule_type='15'  then 'TD公话超市酬金'  
                               when a.rule_type='10'  then '公话超市酬金'  
                           end
                          ,'当月'
                          ,count(*)
                          ,sum(a.fee)
                  from $stat_channel_reward_detail_yyyymm a,
                       $dwd_channel_dept_yyyymmdd b
                  where a.channel_id=b.channel_id  
                        and a.rule_type in ('2','4','5','6','9','10','15')
                  group by a.city_id
                         ,rtrim(ltrim(char(a.county_id)))
                         ,a.geography_type
                         ,a.channel_id
                         ,a.channel_name
                         ,b.channel_type
                         ,b.dept_type_dtl
                         ,case when a.rule_type='4'   then '银行代收费酬金'  
                               when a.rule_type='9'   then '随E行酬金'  
                               when a.rule_type='2'   then '预提卡酬金'  
                               when a.rule_type='5'   then '空中充值酬金'  
                               when a.rule_type='6'   then '充值卡酬金'  
                               when a.rule_type='15'  then 'TD公话超市酬金'  
                               when a.rule_type='10'  then '公话超市酬金'  
                           end"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn


    #删除旧数据
	   set sql_buf "delete from  bass2.stat_channel_reward_0019 where op_time=$year$month"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

	  #将数据插入结果表
	  set sql_buf "insert into $stat_channel_reward_0019 ( op_time          
                                                              ,city_id          
                                                              ,county_id       	
                                                              ,geography_type   
                                                              ,channel_id       
                                                              ,channel_name     
                                                              ,channel_type     
                                                              ,channel_type_dtl2
                                                              ,busi_notes       
                                                              ,months           
                                                              ,counts           
                                                              ,result )
                  select  $year$month
                         ,city_id
                         ,county_id
                         ,geography_type
                         ,channel_id
                         ,channel_name
                         ,channel_type
                         ,channel_type_dtl2
                         ,busi_notes
                         ,months
                         ,sum(counts)
                         ,sum(result)
                  from session.stat_channel_reward_0019_tmp
                  group by city_id
                         ,county_id
                         ,geography_type
                         ,channel_id
                         ,channel_name
                         ,channel_type
                         ,channel_type_dtl2
                         ,busi_notes
                         ,months"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
###===================================================================================================
## step. 删除临时索引
###====================================================================================================

 set sql_buf "drop index bass2.reward_0019_01 "
                
	  puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn

# set sql_buf "drop index bass2.reward_0019_02"
#                
#	  puts ${sql_buf}
#	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
#	   	puts "errmsg:$errmsg"
#	   	return -1
#	   }
#	  aidb_commit $conn

	  return 0
}
