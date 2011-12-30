######################################################################################################
#接口单元名称：用户选择WLAN资费套餐
#接口单元编码：02016
#接口单元说明：截至当月底最后一天24时，所有订购WLAN资费套餐的有效用户的订购关系记录
#程序名称: G_I_02016_MONTH.tcl
#功能描述: 生成02016的数据
#运行粒度: 月
#源    表：1.bass2.dwd_product_sprom_active_yyyymmdd(用户套餐关系(在用))
#          2.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2010-05-24
#问题记录：1.
#修改历史: 1.1.6.7规范剔除历史离网用户
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        #----求上月最后一天---#,格式 yyyymmdd
        puts $last_month_day
        
        set thisyear [string range $op_time 0 3]
        puts $thisyear
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #程序名称
	    set app_name "G_I_02016_MONTH.tcl"

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02016_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

####90030024	WLAN无线宽带上网套餐100元包月
####90030036	WLAN无线宽带上网套餐30元包月
####90030031	WLAN无线宽带上网套餐50元包月
####90005000	WLAN自由套餐(0.2元/分)
####90005001	WLAN经济套餐20元
####90005002	WLAN商务套餐100元
####90005003	WLAN超值套餐200元
####90005004	WLAN自由套餐(0.12元/分)
####90005005	WLAN凭证受理套餐（60小时的时长）
####90008120	WLAN30元/月包15小时
####90008121	WLAN50元/月包40小时
####90008122	WLAN100元/月包200小时


####90008120 WLAN30元/月包15小时                106：30元/月包15小时    
####90008121 WLAN50元/月包40小时                107：50元/月包40小时    
####90008122 WLAN100元/月包200小时              108：100元/月包200小时  
####90030036 WLAN无线宽带上网套餐30元包月       106：30元/月包15小时
####90030031 WLAN无线宽带上网套餐50元包月       107：50元/月包40小时
####90030024 WLAN无线宽带上网套餐100元包月      108：100元/月包200小时
####90005001 WLAN经济套餐20元                   300：本地WLAN资费套餐


    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02016_month
	     (time_id,prod_id,user_id,valid_date)
		select
		    $op_month,
			case when b.prod_id in(90030036,90008120) then '106'
			     when b.prod_id in(90030031,90008121) then '107'
			     when b.prod_id in(90030024,90008122) then '108'
			     when b.prod_id in(90005000,90005001,90005002,90005003,90005004,90005005) then '300'
			 end prod_id,
			 a.user_id,
			 replace(char(date(a.valid_date)),'-','') valid_date
		from bass2.dwd_product_sprom_active_${last_month_day} a,
			 bass2.dim_product_item b
		where a.sprom_id=b.prod_id
		  and b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)
		  and replace(char(date(a.valid_date)),'-','')<='${last_month_day}'
		  and replace(char(date(a.expire_date)),'-','')>'${last_month_day}'
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


    #剔除历史离网用户数据
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02016_month where time_id = $op_month and user_id in
                      (select user_id from bass2.dw_product_$op_month where not (userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)) and month_off_mark<>1)
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



  
  #进行主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select prod_id,user_id,count(*) cnt from bass1.g_i_02016_month
	              where time_id =$op_month
	             group by prod_id,user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02016接口用户选择WLAN资费套餐主键唯一性校验未通过"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }

#2011.12.03 see INT_CHECK_R181_MONTH.tcl
 #1.检查chkpkunique
       set tabname "g_i_02016_month"
       set pk                  "USER_ID"
       chkpkunique ${tabname} ${pk} ${op_month}
       
        
	return 0
}


#select user_id,count(0) from    g_i_02016_month where time_id = 201107
#group by user_id 
#having count(0) > 1
#89157334068705      	2
#
#89157334068705      	2
#
#select count(0),count(distinct user_id ) from     g_i_02016_month where time_id = 201106
#
#select * from   g_i_02016_month where time_id = 201107
#and user_id = '89157334068705'
#
#TIME_ID	PROD_ID	USER_ID	VALID_DATE
#201106	108	89157334068705      	20110601
#201106	107	89157334068705      	20110601
#
#delete from g_i_02016_month
#where user_id = '89157334068705'
#and PROD_ID = '107'
#and time_id = 201106
#