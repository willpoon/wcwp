#########################################################################################
# ** 程序名称: stat_enterprise_0141.tcl                                                     #
# ** 程序功能: 集团客户日数据报表开发.                                 #
# ** 运行粒度: 月                                                                       #
# ** 运行示例: crt_basetab -Sstat_enterprise_0141.tcl -D2011-10-26                          #
# ** 创建时间: 2011-10-26                                                             #
# ** 创 建 人: asiainfo-蔡桥                                                           #
#########################################################################################
proc deal {p_optime p_timestamp} {

 	   global conn
 	   global handle

 	   if [catch {set handle [aidb_open $conn]} errmsg] {
 	   	trace_sql $errmsg 1000
 	   	return -1
 	   }

 	   if {[stat_enterprise_0141 $p_optime $p_timestamp ]!= 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }

 	   aidb_commit $conn
 	   aidb_close $handle

 	   return 0
 }

proc stat_enterprise_0141 { p_optime p_timestamp } {
	global conn
	global handle
	global env

  #日期处理
  scan $p_optime "%04s-%02s-%02s" year month day
  #本月最后一日
  set last [GetThisMonthDays [string range $year$month 0 5]01]
  #下一个月
  set    next_month [GetNextMonth [string range $year$month 0 5]]01
  scan $next_month "%04s%02s%02s" n_year n_month n_day
  puts ${n_year}-${n_month}-${n_day}  
  puts $last


	#源表

  set dw_product_yyyymmdd                   "dw_product_$year$month$day"
  set DW_ENTERPRISE_MEMBER_MID_yyyymmdd     "DW_ENTERPRISE_MEMBER_MID_$year$month$day"
  set DW_NEWBUSI_SMS_yyyymmdd               "DW_NEWBUSI_SMS_$year$month$day"
  set DW_ENTERPRISE_msg_yyyymmdd            "DW_ENTERPRISE_msg_$year$month$day"
  set DW_NEWBUSI_gprs_yyyymmdd              "DW_NEWBUSI_gprs_$year$month$day"
  set DW_ACCT_SHOULD_TODAY_yyyymmdd         "DW_ACCT_SHOULD_TODAY_$year$month$day"
  
 #==========删除当期数据===================================================
         	set sql_buf " delete from bass2.stat_enterprise_0141 where op_time = '${year}-${month}-${day}' "
         	puts ${sql_buf}
         	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
        		trace_sql $errmsg 10150
        		puts "errmsg:$errmsg"
        		return -1
        	}
        	aidb_commit $conn      
        	
        	   

 #==========将数据入表===================================================
         	set sql_buf " insert into bass2.stat_enterprise_0141 (op_time,city_id,ENTERPRISE_ID,ENTERPRISE_name,fact_fee,CALL_DURATION_M,sms_nums,gprs_2g_flows,gprs_3g_flows,tx_nums,dd_nums)
         	select '$year-$month-$day',
                	case when b.LEVEL_DEF_MODE=1 then '888' else b.ENT_CITY_ID end ,
                	a.ENTERPRISE_ID,
                	b.ENTERPRISE_name,
                	
                	sum(value(g.FACT_FEE,0)),
                	sum(c.CALL_DURATION_M),
                	sum(value(result,0)),
                	sum(case when e.MNS_TYPE=0 then e.RATING_RES else 0 end),
                	sum(case when e.MNS_TYPE=1 then e.RATING_RES else 0 end),
                	count(f.PRODUCT_NO) ,
                	count(a.PRODUCT_NO) 
                	from $DW_ENTERPRISE_MEMBER_MID_yyyymmdd a 
                  left join $DW_ENTERPRISE_msg_yyyymmdd b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
                  left join $dw_product_yyyymmdd c on a.PRODUCT_NO=c.PRODUCT_NO and  c.USERSTATUS_ID in (1,2,3,6,8)
                  left join ( select product_no,count(*) result from  $DW_NEWBUSI_SMS_yyyymmdd group by product_no  ) d on a.PRODUCT_NO=d.PRODUCT_NO
                  left join (select product_no,MNS_TYPE,sum(RATING_RES) RATING_RES from  $DW_NEWBUSI_gprs_yyyymmdd group by product_no,MNS_TYPE  ) e  on a.product_no=e.product_no
                  left join $dw_product_yyyymmdd f on a.product_no=f.PRODUCT_NO and f.USERSTATUS_ID in (1,2,3,6,8) and f.ACTIVE_MARK=1
                  left join $DW_ACCT_SHOULD_TODAY_yyyymmdd g on a.product_no=g.PRODUCT_NO

                  group by case when b.LEVEL_DEF_MODE=1 then '888' else b.ENT_CITY_ID end ,a.ENTERPRISE_ID,b.ENTERPRISE_name
                  
        "
         	puts ${sql_buf}
         	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
        		trace_sql $errmsg 10150
        		puts "errmsg:$errmsg"
        		return -1
        	}
        	aidb_commit $conn       

       	  	

	return 0
}