######################################################################################################
#接口名称：竞争对手用户号码汇总
#接口编码：21020
#接口说明：截至统计月末最后一天24:00，满足当日“竞争对手客户到达数”统计条件的竞争对手用户号码集合。
#“竞争对手客户到达数”是指在90天内与中国移动客户发生过互联互通（包括语音、点对点短信和点对点彩信）的竞争对手用户号码集合。
#程序名称: g_i_21020_month.tcl
#功能描述: 生成21020的数据
#运行粒度: 月
#源    表：1.bass2.dw_comp_all_yyyymm
#          2.bass2.dw_comp_cust_yyyymm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-09-19
#问题记录：1.只抓取竞争用户在网的，并且竞争对手运营商类型在规范中描述的清单
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time
  
  #本月 yyyymm
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
  puts $op_month
        

  #本月第一天 yyyy-mm-dd
  set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
  puts $this_month_first_day

  #本月最后一天 yyyy-mm-dd
  set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
  puts $this_month_last_day

	global app_name
	set app_name "g_i_21020_month.tcl"                 

  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_21020_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	

    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass2.dw_comp_cust_$op_month with distribution and detailed indexes all
    
    exec db2 runstats on table bass2.dw_comp_all_$op_month with distribution and detailed indexes all
    
    exec db2 terminate
	
	
	#直接汇总到结果表
	#COMP_PRODUCT_NO
#13228986978*  
#13228986978
#存在如上的重复！
  set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_i_21020_month
				select 
         TIME_ID
        ,COMP_PRODUCT_NO
        ,COMP_BRAND_ID
        ,COMP_BEGIN_DATE
        ,COMP_LAST_DATE
        ,CALL_COUNTS
        ,SMS_COUNTS
        ,MMS_COUNTS
        from 				
				(
				select 
						${op_month} TIME_ID,
						a.comp_product_no COMP_PRODUCT_NO,
						case 
						  when a.comp_brand_id in (3,4) then '021000'
						  when a.comp_brand_id in (7)   then '022000'
						  when a.comp_brand_id in (9,10,11) then '031000'
						  when a.comp_brand_id in (2) then '032000'
						  when a.comp_brand_id in (1,8) then '033000'
						  when a.comp_brand_id in (6) then '034000'
						end COMP_BRAND_ID ,
						replace(substr(char(a.comp_open_date),1,10),'-','') COMP_BEGIN_DATE,
						replace(substr(char(a.comp_last_date),1,10),'-','') COMP_LAST_DATE,
						value(char(sum(b.in_call_counts+b.out_call_counts)),'0') CALL_COUNTS,
						value(char(sum(b.mo_sms_counts+b.mt_sms_counts)),'0') SMS_COUNTS,
					  value(char(sum(b.mo_mms_counts+b.mt_mms_counts)),'0') MMS_COUNTS,
					  row_number()over(partition by substr(trim(a.COMP_PRODUCT_NO),1,11) order by a.COMP_PRODUCT_NO ) rn
				from  bass2.dw_comp_cust_$op_month a
				left join bass2.dw_comp_all_$op_month b on a.comp_product_no = b.comp_product_no
				where a.comp_userstatus_id = 1
				  and a.comp_brand_id in (1,2,3,4,6,7,8,9,10,11)
			group by a.comp_product_no,
						case 
						  when a.comp_brand_id in (3,4) then '021000'
						  when a.comp_brand_id in (7)   then '022000'
						  when a.comp_brand_id in (9,10,11) then '031000'
						  when a.comp_brand_id in (2) then '032000'
						  when a.comp_brand_id in (1,8) then '033000'
						  when a.comp_brand_id in (6) then '034000'
						end ,
						replace(substr(char(a.comp_open_date),1,10),'-',''),
						replace(substr(char(a.comp_last_date),1,10),'-','') 
						) t where t.rn = 1
			"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

set sql_buff "
	update g_i_21020_month a
	set COMP_PRODUCT_NO = substr(COMP_PRODUCT_NO,1,11)
	where time_id = $op_month
	and COMP_BRAND_ID = '021000'
	and length(trim(COMP_PRODUCT_NO)) > 11
"
exec_sql $sql_buff


set sql_buff "
	delete from 
	(
		select * from   G_I_21020_MONTH
			where COMP_BRAND_ID = '021000'
			and time_id = $op_month
			and length(trim(COMP_PRODUCT_NO)) <> 11
	) t
"
exec_sql $sql_buff

	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '021000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 1 "


set sql_buff "
	delete from 
	(
		select * from   G_I_21020_MONTH
			where COMP_BRAND_ID = '031000'
			and time_id = $op_month
			and length(trim(COMP_PRODUCT_NO)) <> 11
	) t
"
exec_sql $sql_buff


	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '031000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 2 "


  #1.检查chkpkunique
        set tabname "G_I_21020_MONTH"
        set pk                  "COMP_PRODUCT_NO"
        chkpkunique ${tabname} ${pk} ${op_month}
        
        
	 
	return 0
}	