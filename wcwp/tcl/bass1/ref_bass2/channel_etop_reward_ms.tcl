#****************************************************************************************
# ** 程序名称: channel_etop_reward_ms.tcl
# ** 程序功能: 空中充值及无线IP超市数据酬金(2010年6月份之后BOSS系统老新表dw_acct_payment_dm_yyyymm)
# ** 运行粒度: 日
# ** 运行示例:  crt_basetab.sh channel_etop_reward_ms.tcl 2010-07-01
# ** 创建时间:  2010-5-17 13:59
# ** 创 建 人:  fuzl
# ** 问    题: 1.
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **           2010-07-29    fuzl        增加字段geography_type来区分地理类型，并且对公话超市和TD公话做了区分
# **           2010-12-06    fuzl        更改公话超市口径
# **           2011-01-19    heys        结算抽检的号码当月消费改为只有通话费
# **           2011-7-4      fuzl        更改结果表的分区键，增加PHONE_ID
# ** Copyright(c) 2009 AsiaInfo Technologies (China), Inc.
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
     
     set  dwd_channel_dept_yyyymmdd        "bass2.dwd_channel_dept_${last_day_month}" 
     set  dw_product_yyyymm                "bass2.dw_product_${op_month}              "   
     set  dwd_acct_bill_dtl_yyyymm         "bass2.dwd_acct_bill_dtl_${op_month}       "   
     set  dwd_acct_bill_yyyymm             "bass2.dwd_acct_bill_${op_month}           "   
     set  dw_agent_acc_info_yyyymmdd       "bass2.dw_agent_acc_info_${last_day_month} "   
#    set  dw_acct_payitem_yyyymm           "bass2.dw_acct_payitem_${op_month}         "  
     set  dw_acct_payment_dm_yyyymm         "bass2.dw_acct_payment_dm_${op_month}"
     set  dw_channel_shop_number_yyyymm     "bass2.dw_channel_shop_number_${op_month}"
 
     
     
	   set sql_buf "drop table bass2.channel_etop_reward_${op_month}"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }	   
	   aidb_commit $conn
	   
	  set sql_buf "create table bass2.channel_etop_reward_${op_month} like bass2.channel_etop_reward_yyyymm
		             distribute by hash(object_id,phone_id)   
		             in tbs_report index in tbs_index not logged initially"
	  puts ${sql_buf}
	  if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	  	puts "errmsg:$errmsg"
	  }	  
	  aidb_commit $conn	   

     set sql_buf "insert into bass2.channel_etop_reward_${op_month} (
	                         op_time           
                          ,object_id         
                          ,entity_type       
                          ,unique_id         
                          ,phone_id          
                          ,region_code       
                          ,county_code
                          ,geography_type       
                          ,channel_type     
                          ,channel_type_dtl  
                          ,channel_type_dtl3 
                          ,pre_fee           
                          ,ext1              
                            
                 )       
                select '$year-$month-$day'
                       ,a.entity_id      
                       ,a.entity_type    
                       ,row_number() over()               
                       ,a.phone_id       
                       ,a.region_code    
                       ,a.county_code 
                       ,a.geography_type
                       ,a.channel_type   
                       ,a.channel_type_dtl 
                       ,a.channel_type_dtl3 
                       ,a.pre_fee
                       ,a.mark                        
                from                    
                    (select b.channel_id  entity_id
                            ,1 entity_type                            
                            ,c.shop_number      phone_id      
                            ,b.channel_city    region_code   
                            ,b.region_type     county_code
                            ,b.geography_type 
                            ,b.channel_type  
                            ,b.dept_type_dtl   channel_type_dtl    
                            ,b.dept_type_dtl3  channel_type_dtl3
                            ,case when c.td_mark=1  then 3 else 2  end mark
                            ,sum(a.local_fee+a.toll_fee+a.roam_fee)        pre_fee                                
                      from $dwd_channel_dept_yyyymmdd b,
                           $dw_product_yyyymm  a,
                           $dw_channel_shop_number_yyyymm  c
                      where b.channel_id = c.channel_id
                      and a.product_no=c.shop_number
                      and a.userstatus_id <>0 
                      and b.rec_status=0
                      group by b.channel_id                         
                              ,c.shop_number     
                              ,b.channel_city   
                              ,b.region_type     
                              ,b.geography_type 
                              ,b.channel_type  
                              ,b.dept_type_dtl   
                              ,b.dept_type_dtl3
                              ,case when c.td_mark=1  then 3 else 2  end 
                      union all
                      select   b.channel_id entity_id
                              ,1 entity_type
                              ,c.key_num      phone_id      
                              ,b.channel_city   region_code   
                              ,b.region_type    county_code
                              ,b.geography_type
                              ,b.channel_type    
                              ,b.dept_type_dtl  channel_type_dtl 
                              ,b.dept_type_dtl3 channel_type_dtl3
                              ,1 mark 
                              ,(c.balance+c.amount)     pre_fee                                
                      from  $dw_agent_acc_info_yyyymmdd a,
                            $dwd_channel_dept_yyyymmdd  b,
                            $dw_acct_payment_dm_yyyymm c
                      where a.channel_id = b.organize_id
                            and a.mobile_id=c.key_num
                            and c.opt_code='GJFK'
                      )a"                 
                      
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   	return -1
	   }
	  aidb_commit $conn
	  
	  return 0
}
