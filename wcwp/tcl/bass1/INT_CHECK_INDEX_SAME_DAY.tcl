######################################################################################################
#程序名称：INT_CHECK_INDEX_SAME_DAY.tcl
#校验接口：G_S_22012_DAY.tcl,G_S_22201_DAY.tcl
#          G_A_02004_DAY.tcl,G_A_02008_DAY.tcl,G_S_04018_DAY.tcl,G_I_06031_DAY.tcl
#          R159_1:新增客户数；R159_2：客户到达数；R159_3：当月累计使用TD网络的上网本客户数；R159_4：离网客户数
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：LIUQF
#编写时间：2009-10-27
#问题记录：
#修改历史: 20100125 在网客户口径变动 usertype_id NOT IN ('2010','2020','2030','9000')
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        #本月 yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#            
#        #上月  yyyymm
#        set last_month [GetLastMonth [string range $op_month 0 5]]
#        
#        #自然月第一天 yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
#        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        puts $l_timestamp
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_INDEX_SAME_DAY.tcl"


	#删除本日数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp and rule_code in ('R159_1','R159_2','R159_3','R159_4')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#建立临时表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.int_check_user_status
				(
			   user_id        CHARACTER(15),
			   product_no     CHARACTER(15),
			   test_flag      CHARACTER(1),
			   sim_code       CHARACTER(15),
			   usertype_id    CHARACTER(4),
			   create_date    CHARACTER(15),
			   time_id        int
				)                            
				partitioning key           
				 (
				   user_id    
				 ) using hashing           
				with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#存放最新用户资料
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.int_check_user_status (
								 user_id    
								,product_no 
								,test_flag  
								,sim_code   
								,usertype_id  
								,create_date
								,time_id )
					select e.user_id
								,e.product_no  
								,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
								,e.sim_code
								,f.usertype_id  
								,e.create_date  
								,f.time_id       
					from (select user_id,create_date,product_no,sim_code,usertype_id
								 			,row_number() over(partition by user_id order by time_id desc ) row_id   
					      from bass1.g_a_02004_day
					      where time_id<=$timestamp ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$timestamp ) f on f.user_id=e.user_id
					where e.row_id=1 and f.row_id=1
			"
	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
							
  #####################
  #R159_1:新增客户数

	#从22012（日KPI）接口取得“新增客户数”字段值
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_new_users)) from bass1.g_s_22012_day where time_id=$timestamp"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#取统计日的一经计算得到的指标值：第一步：取用户历史表中入网日期ent_dt为统计日，且生效日期eff_dt在当前统计日内，
  #失效日期end_dt大于统计日，并且用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的数据，作为日新增用户数进行统计。
  #第二步：按省公司标识汇总指标作为统计日新增用户数
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.int_check_user_status
		where create_date = '$timestamp'
		  and test_flag='0'
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R159_1'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
		,0
	)
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_1 一致性检查新增客户数超出1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_1 ---------------------------------------"
	
	
	
  #####################
  #R159_2:客户到达数

	#从22012（日KPI）接口取得“客户到达数”字段值
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_dao_users)) from bass1.g_s_22012_day where time_id=$timestamp"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#取统计日的一经计算得到的指标值：第一步：取用户历史表中生效日期eff_dt在当前统计日内且失效日期end_dt大于统计日期，
	#用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的用户标识、品牌。第二步：关联用户状态历史（tb_svc_subs_stat_hist）
	#并判断用户状态类型编码（Subs_Stat_Typ_Cd)
	#不等于2010（主动销号）、2020（被动销号）、2030（冷冻期）、9000（无效修正）时，
	#计算日用户到达数（count(distinct tb_svc_subs_hist.subs_id)）。
	#第三步：按省公司标识汇总指标。
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.int_check_user_status
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R159_2'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
		,0
	)
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_2 一致性检查客户到达数超出1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_2 ---------------------------------------"
	
		
	
	
  #####################
  #R159_3:当月累计使用TD网络的上网本客户数

	#取统计日的省公司上报指标值：从22201（使用TD网络的客户日汇总）接口取得“当月累计使用TD网络的上网本客户数”字段值
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_3gbook_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

 ##取统计日的一经计算得到的指标值：第一步：选取统计日去掉上网本专用号段且有效的非测试用户，关联用户状态取在网用户。
 ##第二步：取上网本使用日汇总表(TB_SUM_NTBK_SUBS_D)中，无线网络类型为1，内置号码小于14744000000或大于 14744005999且从月初
 ##到统计日有上网本使用行为的用户。通过该表绑定号码前7位、8位或前9位内关联表地市公司与MSISDN号段的对应关系
 ##（TB_SUM_PRVD_MSISDN_RELATION），取号段表省公司。
 ##第三步：第二步结果与第一步用户状态结果关联，按省公司汇总各省当月累计使用T网的上网本客户数.
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from 
		(
    select a.product_no from 
		    (
		    select product_no,substr(product_no,1,7) msisdn from bass1.G_S_04018_DAY
		     where time_id between $l_timestamp and $timestamp
		       and MNS_TYPE='1'
		       and bigint(INTRA_PRODUCT_NO) not between 14744000000 and 14744005999
		    ) a,
		    (  
		    select  distinct ltrim(rtrim(msisdn)) msisdn from bass1.g_i_06031_day  
		     where time_id between $l_timestamp and $timestamp
		    ) b
        where a.msisdn=b.msisdn
			) aa,
			(
			select user_id,product_no from session.int_check_user_status
			 where usertype_id NOT IN ('2010','2020','2030','9000')
			   and test_flag='0'
			) bb
  where aa.product_no=bb.product_no
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R159_3'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
		,0
	)
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05} {
		set grade 2
	        set alarmcontent "R159_3 一致性检查当月累计使用TD网络的上网本客户数超出5%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_3 ---------------------------------------"
		


  #####################
  #R159_4:离网客户数

	#取统计日的省公司上报指标值：从22012（日KPI）接口取得“离网客户数”字段值
	set handle [ aidb_open $conn ]
	set sqlbuf "
	    select sum(bigint(m_off_users)) from bass1.g_s_22012_day where time_id=$timestamp"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

 ##取统计日的一经计算得到的指标值：第一步：取用户月历史表中生效日期eff_dt在当前统计日内且失效日期end_dt大于统计日期，
 ##用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的数据。第二步：关联用户状态历史表（tb_svc_subs_stat_hist）
 ##中状态生效日期eff_dt在当前统计日内且失效日期end_dt大于统计日期, 用户状态类型编码为2010（主动销号）、2020（被动销号）和2030（冷冻期）的用户数。
 ##第三步：按省公司标识汇总出各省公司的离网用户数。
	set handle [ aidb_open $conn ]
	set sqlbuf "
			select count(distinct user_id) from session.int_check_user_status
			where usertype_id IN ('2010','2020','2030','9000')
			  and test_flag='0'
			  and time_id=$timestamp
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R159_4'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2
		,0
	)
	"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>=0.01 || $RESULT_VAL3<=-0.01 } {
		set grade 2
	        set alarmcontent "R159_4 一致性检查离网客户数超出1%"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

  puts "R159_4 ---------------------------------------"
		
	

	
	return 0
}

