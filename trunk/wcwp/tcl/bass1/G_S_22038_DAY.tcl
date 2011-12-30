######################################################################################################
#接口名称：各品牌用户日KPI
#接口编码：22038
#接口说明：记录各品牌用户的日KPI信息
#程序名称: G_S_22038_DAY.tcl
#功能描述: 生成22038的数据
#运行粒度: 日
#源    表：1.bass2.dw_acct_shoulditem_today_yyyymmdd
#          2.bass2.dw_product_yyyymmdd 
#          3.bass1.int_22038_yyyymm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.该接口程序因为用到了Dw_comp_cust_dt，所以不能重跑以前的数据。
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $op_time 8 9]
        #本月第一天 yyyymmdd
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        set this_month_first_day [string range $op_month 0 5]01

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22038_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#01:创建临时表(结果表的临时表)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22038_day_tmp1
                     (
			  brand_id          varchar(1),
			  income            bigint,
			  d_comm_users      bigint,
			  m_comm_users      bigint,
			  d_voice_users     bigint,
			  m_voice_users     bigint,
			  d_incr_users      bigint,
			  m_incr_users      bigint,
			  w_comm_nums       bigint,
			  d_incr_fee        bigint,
			  d_voice_incr_fee  bigint,
			  d_sms_fee         bigint,
			  d_other_fee       bigint		
                      )
                      partitioning key 
                      (brand_id)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
					
	#04：当日出帐的收入之和
        set handle [aidb_open $conn]
        set sql_buff "insert into session.g_s_22038_day_tmp1
                 select
                   coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2'),
                   bigint(sum(a.fact_fee)*100),
                   0,0,0,0,0, 0,0,0,0,0, 0
                 from 
                    bass2.dw_acct_shoulditem_today_$timestamp a,
                    bass2.dw_product_$timestamp  b
                 where
                   a.user_id=b.user_id
                 group by
                   b.brand_id "               
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#05：当日通信用户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,
                       count (distinct user_id),
                       0,0,0,0,0, 0,0,0,0,0
                     from 
                       bass1.int_22038_$op_month 
                     where 
                       op_time=$timestamp
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle       
	
	#06:当月累计通信用户数 
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,
                        count (distinct user_id),
                       0,0,0,0,0, 0,0,0,0
                     from
                        bass1.int_22038_$op_month 
                     where 
                        op_time<=$timestamp
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        #07:当日语音用户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,
                       count(distinct user_id),
                       0,0,0,0,0, 0,0,0
                     from 
                       bass2.dw_product_$timestamp
                     where 
                       day_call_mark=1
                     group by
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#08:当月累计语音客户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0, 0,0
	             from
	               bass2.dw_product_$timestamp
                     where
                       month_call_mark=1 
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#09:当日增值业务使用客户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0,0
	             from
	               bass2.dw_product_$timestamp
                     where
                       day_newbusi_mark =1 
                     group by 
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
		
	#10:当月增值业务使用客户数
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2'),
	               0,0,0,0,0,0,
	               count(distinct user_id),
	               0,0,0,0,0
	             from  bass2.dw_product_$timestamp
              where  month_newbusi_mark=1 
              group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
			
	#11:本周通信客户数(本指标为周指标，每周只在周五（数据日期）报一次，其它天填0)
	set friday [ clock format [ clock scan $optime ] -format "%A" ]
	if { ${friday} == "Friday" } {
	  set Thursday $last_day
	  puts $Thursday
	  set Wednesday [GetLastDay [string range $Thursday 0 7]]
	  puts $Wednesday
	  set Tuesday   [GetLastDay [string range $Wednesday 0 7]]
	  puts $Tuesday
	  set Monday    [GetLastDay [string range $Tuesday 0 7]]
	  puts $Monday
	  set Sunday    [GetLastDay [string range $Monday 0 7]]
	  puts $Sunday
	  set Saturday  [GetLastDay [string range $Sunday 0 7]]
	  puts $Saturday
	  
          set handle [aidb_open $conn]
          set sql_buff "insert into session.g_s_22038_day_tmp1
	             select
	               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(t.brand_id)),'2'),
	               0,0,0,0,0,0,0,
                       count (distinct t.user_id),
                        0,0,0,0
                     from
                       (
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$timestamp
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Thursday
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Wednesday
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Tuesday
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Monday 
                         union all 
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Sunday 
                         union all
                         select brand_id,user_id from bass1.int_22038_$op_month where op_time=$Saturday                                                               
                       )t
                     group by
                       coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(t.brand_id)),'2') "
          puts $sql_buff
          if [catch { aidb_sql $handle $sql_buff } errmsg ] {
        		WriteTrace "$errmsg" 2020
        		puts $errmsg
        		aidb_close $handle
        		return -1
	  }
	  aidb_commit $conn
	  aidb_close $handle 	  
		
        }
        
  #12:当日增值业务收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0,0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100 in (4,5,6,7)
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   
	
	#当日语音增值业务收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=5
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle 
        
        #当日短信类业务收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0
                from
                  bass2.dw_acct_shoulditem_today_$timestamp a,
                  bass2.dw_product_$timestamp  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=6
                  and a.user_id=b.user_id
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle 
	
	#当日非短信类业务收入(当日非短信类业务收入=当日新业务收入-当日语音增值业务收入-当日短信类业务收入)
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22038_day_tmp1
                 select 
                   brand_id,
                   0,0,0,0,0,0,0,0,0,0,0,
                   sum(d_incr_fee-d_voice_incr_fee-d_sms_fee)
                from
                  session.g_s_22038_day_tmp1
                group by 
                  brand_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    
	
        #13:汇总到结果表
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22038_day 
	             select
	               $timestamp
	               ,'$timestamp'
	               ,char(brand_id)
                       ,char(sum(income))           
                       ,char(sum(d_comm_users))     
                       ,char(sum(m_comm_users))     
                       ,char(sum(d_voice_users))    
                       ,char(sum(m_voice_users))    
                       ,char(sum(d_incr_users))     
                       ,char(sum(m_incr_users))     
                       ,char(sum(w_comm_nums))      
                       ,char(sum(d_incr_fee))       
                       ,char(sum(d_voice_incr_fee)) 
                       ,char(sum(d_sms_fee))        
                       ,char(sum(d_other_fee)) 
	             from
	               session.g_s_22038_day_tmp1 
	             group by
	               char(brand_id) "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	      
#####################################
	return 0
}