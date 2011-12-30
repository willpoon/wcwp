#****************************************************************************************
# ** 程序名称: stat_channel_reward_0007.tcl
# ** 程序功能: 西藏空中充值酬金明细查询
# ** 运行粒度: 月
# ** 运行示例:  crt_basetab.sh stat_channel_reward_0007.tcl 2010-05-01
# ** 创建时间:  2010-6-22
# ** 创 建 人:  fuzl
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **           2010-07-27     Fuzl       增加地理类型字段 geography_type
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
  


     set   channel_etop_reward_yyyymm          "bass2.channel_etop_reward_$year$month"     
     set   stat_channel_reward_detail_yyyymm   "bass2.stat_channel_reward_detail_$year$month"
  
     

	   set sql_buf "delete from  bass2.stat_channel_reward_0007 where op_time=$year$month"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }	   
	   aidb_commit $conn
	     
     
	   set sql_buf "insert into bass2.stat_channel_reward_0007 ( op_time          
                                                              ,city_id          
                                                              ,county_id
                                                              ,geography_type       	
                                                              ,channel_id       
                                                              ,channel_name     
                                                              ,channel_type     
                                                              ,channel_type_dtl2        
                                                              ,product_no
                                                              ,pre_fee          
                                                              ,fee             
                                                             )                         
                  select  $year$month                      
                         ,a.city_id
                         ,rtrim(ltrim(a.county_id))
                         ,a.geography_type
                         ,a.channel_id  
                         ,a.channel_name
                         ,b.channel_type  
                         ,b.channel_type_dtl    
                         ,b.phone_id  
                         ,b.pre_fee     
                         ,a.fee 
                  from $stat_channel_reward_detail_yyyymm a
                       left join $channel_etop_reward_yyyymm b  on a.phone_id=b.phone_id
                  where a.unique_id=b.unique_id and a.rule_type='5'"
	   puts ${sql_buf}    
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1         
	   }                  
	  aidb_commit $conn   
	                      
	  return 0            
}                       
                        