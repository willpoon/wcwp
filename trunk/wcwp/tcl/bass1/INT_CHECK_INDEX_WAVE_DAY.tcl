######################################################################################################
#程序名称：INT_CHECK_INDEX_WAVE_DAY.tcl
#校验接口：G_S_22012_DAY.tcl
#
#R161_1:新增客户数                         ±15%
#R161_2:客户到达数                         ±2%
#R161_3:净增客户数                         ±100%
#R161_4:通信客户数                         ±5%
#R161_5:当月累计通信客户数                 ±5%
#R161_6:使用TD网络的客户数                 ±5%
#R161_7:当月累计使用TD网络的手机客户数     ±5%
#R161_8:当月累计使用TD网络的信息机客户数   ±5%
#R161_9:当月累计使用TD网络的数据卡客户数   ±5%
#R161_10:当月累计使用TD网络的上网本客户数  ±5%
#R161_11:联通移动客户总数                  ±2%
#R161_12:电信移动客户总数                  ±2%
#R161_13:联通移动新增客户数                ±8%
#R161_14:电信移动新增客户数                ±8%
#R161_15:使用TD网络的客户在T网上计费时长   ±20%
#R161_16:使用TD网络的客户在T网上的数据流量 ±20%
#R161_17:离网客户数                        ±70%
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：LIUQF
#编写时间：2010-07-13
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  
        #本月第一天 yyyymmdd
        set l_timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        puts $l_timestamp
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        puts $last_day
        #前两天
        set last_last_day [GetLastDay [string range $last_day 0 7]]
        puts $last_last_day
        #程序名
        set app_name "INT_CHECK_INDEX_WAVE_DAY.tcl"


	#删除本日数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp 
	                    and rule_code in ('R161_1','R161_2','R161_3','R161_4','R161_5','R161_6','R161_7','R161_8','R161_9','R161_10','R161_11','R161_12','R161_13','R161_14','R161_15','R161_16','R161_17')"
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
	declare global temporary table session.check_user_status_1
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

	#存放前一天的用户资料
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.check_user_status_1 (
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
					      where time_id<=$last_day ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$last_day ) f on f.user_id=e.user_id
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



	#建立临时表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	declare global temporary table session.check_user_status_2
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

	#存放前两天的用户资料
	set handle [ aidb_open $conn ]
	set sqlbuf "
       insert into session.check_user_status_2 (
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
					      where time_id<=$last_last_day ) e
					inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
								       from bass1.g_a_02008_day
								       where time_id<=$last_last_day ) f on f.user_id=e.user_id
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



    #########################################################################
    #R161_1:新增客户数

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

    #取上个统计日的指标值：第一步：取用户历史表中入网日期ent_dt为上个统计日，且生效日期eff_dt在上个统计日内，失效日期end_dt大于
    #上个统计日，并且用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的数据，作为日新增用户数进行统计。第二步：按省公司标
    #识汇总指标作为上个统计日的新增用户数。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
		where create_date = '$last_day'
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
		,'R161_1'
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
	if {$RESULT_VAL3>=0.15 || $RESULT_VAL3<=-0.15 } {
		set grade 2
	    set alarmcontent "R161_1 波动性检查新增客户数超出15%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

    puts "R161_1 ---------------------------------------"



    #R161_2:客户到达数

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

    ##取上个统计日的指标值：第一步：取用户历史表中生效日期eff_dt在上个统计日内且失效日期end_dt大于上个统计日，
    ##用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的用户标识、品牌。
    ##第二步：关联用户状态历史（tb_svc_subs_stat_hist）并判断用户状态类型编码（Subs_Stat_Typ_Cd)不等于2010（主动销号）
    ##、2020（被动销号）、2030（冷冻期）、1040（保留期）、1021（主动预销号）、9000（无效修正）时，
    ##计算日用户到达数（count(distinct tb_svc_subs_hist.subs_id)）。
    ##第三步：按省公司标识汇总指标。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
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
		,'R161_2'
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
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_2 波动性检查客户到达数超出2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_2 ---------------------------------------"



    #R161_3:净增客户数

	#取统计日的指标值：第一步：从省公司上报的22012日KPI中的“客户到达数”取统计日。
	#第二步：从省公司上报的22012日KPI中的“客户到达数”取上个统计日。
	#第三步：按省公司标识用第一步的结果减去第二步的结果。
	
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

	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_dao_users)) from bass1.g_s_22012_day where time_id=$last_day"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL3 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle	

    #取上个统计日的指标值：第一步：取KPI数据表（dm_kpi_value）中处理日期为上个统计日的客户到达数。
    #第二步：取KPI数据表（kpi_value_new）中处理日期为上上个统计日的客户到达数，
    #第三步：按省公司标识用第一步的结果减去第二步的结果，即得到净增客户数。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_1
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
	

	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(distinct user_id) from session.check_user_status_2
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
  "
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL4 [ lindex $p_row 0 ]
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
		,'R161_3'
		,$RESULT_VAL1-$RESULT_VAL3
		,$RESULT_VAL2-$RESULT_VAL4
		,1.000 * (($RESULT_VAL1-$RESULT_VAL3) - ($RESULT_VAL2-$RESULT_VAL4)) / ($RESULT_VAL2-$RESULT_VAL4)
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

    set  RESULT_VAL5 [expr 1.000 * (($RESULT_VAL1-$RESULT_VAL3) - ($RESULT_VAL2-$RESULT_VAL4)) / ($RESULT_VAL2-$RESULT_VAL4) ]
    puts  $RESULT_VAL5
	if {$RESULT_VAL5>=1.00 || $RESULT_VAL5<=-1.00 } {
		set grade 2
	    set alarmcontent "R161_3 波动性检查净增客户数超出100%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_3 ---------------------------------------"



    #R161_4:通信客户数

	#取统计日的指标值：从分品牌日KPI（TB_SUM_DAILY_KPI_BRND）表中取出入库日期等于统计日的各品牌的“当日通信客户数”数据，
	#再将三个品牌相加得到总的通信客户数。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(d_comm_users)) from bass1.g_s_22038_day where time_id=$timestamp"
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

    #取上个统计日的指标值：从分品牌日KPI（TB_SUM_DAILY_KPI_BRND）表中取出入库日期等于上个统计日的各品牌
    #的“当日通信客户数”数据，再将三个品牌相加得到总的通信客户数。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(d_comm_users)) from bass1.g_s_22038_day where time_id=$last_day"
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
		,'R161_4'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_4 波动性检查通信客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_4 ---------------------------------------"




    #R161_5:当月累计通信客户数

	#取统计日的指标值：从分品牌日KPI（TB_SUM_DAILY_KPI_BRND）表中取出入库日期等于统计日的各品牌的“当月累计通信客户数”数据，
	#再将三个品牌相加得到总的当月累计通信客户数
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(m_comm_users)) from bass1.g_s_22038_day where time_id=$timestamp"
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

    #取上个统计日的指标值：从分品牌日KPI（TB_SUM_DAILY_KPI_BRND）表中取出入库日期等于上个统计日的各品牌的“当月累计通信客户数”数据，
    #再将三个品牌相加得到总的当月累计通信客户数
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(m_comm_users)) from bass1.g_s_22038_day where time_id=$last_day"
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
		,'R161_5'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_5 波动性检查当月累计通信客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_5 ---------------------------------------"



    #R161_6:使用TD网络的客户数

	#取统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
	#入库日期为统计日的“使用TD网络的客户数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(td_customer_cnt)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #取上个统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
    #入库日期为上个统计日的“使用TD网络的客户数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(td_customer_cnt)) from bass1.g_s_22201_day where time_id=$last_day"
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
		,'R161_6'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_6 波动性检查使用TD网络的客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_6 ---------------------------------------"


    #R161_7:当月累计使用TD网络的手机客户数

	#取统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
	#入库日期为统计日的“当月累计使用TD网络的手机客户数”
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_usage_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #取上个统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
    #入库日期为上个统计日的“当月累计使用TD网络的手机客户数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(mtl_td_usage_mark)) from bass1.g_s_22201_day where time_id=$last_day"
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
		,'R161_7'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_7 波动性检查当月累计使用TD网络的手机客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_7 ---------------------------------------"


    #R161_9:当月累计使用TD网络的数据卡客户数

	#取统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
	#入库日期为统计日的“当月累计使用TD网络的数据卡客户数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(mtl_td_datacard_mark)) from bass1.g_s_22201_day where time_id=$timestamp"
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

    #取上个统计日的指标值：取使用TD网络的客户日汇总(tb_sum_td_usd_net_cust_d)表中，
    #入库日期为上个统计日的“当月累计使用TD网络的数据卡客户数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(mtl_td_datacard_mark)) from bass1.g_s_22201_day where time_id=$last_day"
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
		,'R161_9'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_9 波动性检查当月累计使用TD网络的数据卡客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_9 ---------------------------------------"



    #R161_10:当月累计使用TD网络的上网本客户数

	#取统计日的指标值：从22201（使用TD网络的客户日汇总）接口取得“当月累计使用TD网络的上网本客户数”字段值
	
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

    #取上个统计日的指标值：第一步：选取上个统计日去掉上网本专用号段且有效的非测试用户，关联用户状态取在网用户。
    #第二步：取上网本使用日汇总表(TB_SUM_NTBK_SUBS_D)中，无线网络类型为1，内置号码小于14744000000或大于 14744005999且
    #从月初到上个统计日有上网本使用行为的用户。通过该表绑定号码前7位、8位或前9位内关联表地市公司与MSISDN号段的对应关
    #系（TB_SUM_PRVD_MSISDN_RELATION），取号段表省公司。
    #第三步：第二步结果与第一步用户状态结果关联，按省公司汇总各省当月累计使用T网的上网本客户数
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(target2)) from bass1.G_RULE_CHECK where rule_code='R159_3' and time_id=$last_day"
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
		,'R161_10'
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
	if {$RESULT_VAL3>=0.05 || $RESULT_VAL3<=-0.05 } {
		set grade 2
	    set alarmcontent "R161_10 波动性检查当月累计使用TD网络的上网本客户数超出5%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_10 ---------------------------------------"



    #R161_11:联通移动客户总数

	#取统计日的指标值：取竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期为统计日的“联通GSM客户总数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(union_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #取上个统计日的指标值：取竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期为上个统计日的“联通GSM客户总数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(union_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		,'R161_11'
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
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_11 波动性检查联通移动客户总数超出2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_11 ---------------------------------------"



    #R161_12:电信移动客户总数

	#取统计日的指标值：取竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期为统计日的“电信CDMA客户总数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tel_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #取上个统计日的指标值：取竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期为上个统计日的“电信CDMA客户总数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tel_mobile_arrive_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		,'R161_12'
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
	if {$RESULT_VAL3>=0.02 || $RESULT_VAL3<=-0.02 } {
		set grade 2
	    set alarmcontent "R161_12 波动性检查电信移动客户总数超出2%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_12 ---------------------------------------"





    #R161_13:联通移动新增客户数

	#取统计日的指标值：汇总竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期在统计日内的“联通GSM新增客户数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(union_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #取上个统计日的指标值：汇总竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期在上个统计日内的“联通GSM新增客户数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(union_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		,'R161_13'
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
	if {$RESULT_VAL3>=0.08 || $RESULT_VAL3<=-0.08 } {
		set grade 2
	    set alarmcontent "R161_13 波动性检查联通移动新增客户数超出8%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_13 ---------------------------------------"



    #R161_14:电信移动新增客户数

	#取统计日的指标值：汇总竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期在统计日内的“电信CDMA新增客户数”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tel_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$timestamp"
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

    #取上个统计日的指标值：汇总竞争对手日KPI(tb_sum_compt_kpi_daily)表中，入库日期在上个统计日内的“电信CDMA新增客户数”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tel_mobile_new_add_cnt)) from bass1.G_S_22073_DAY where time_id=$last_day"
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
		,'R161_14'
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
	if {$RESULT_VAL3>=0.08 || $RESULT_VAL3<=-0.08 } {
		set grade 2
	    set alarmcontent "R161_14 波动性检查电信移动新增客户数超出8%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_14 ---------------------------------------"



    #R161_15:使用TD网络的客户在T网上计费时长

	#取统计日的指标值：使用TD网络的客户通话情况日汇总(tb_sum_td_usd_net_cust_call_d)表中，
	#入库日期为统计日的“使用TD网络的客户在T网上计费时长”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$timestamp"
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

    #取上个统计日的指标值：使用TD网络的客户通话情况日汇总(tb_sum_td_usd_net_cust_call_d)表中，
    #入库日期为上个统计日的“使用TD网络的客户在T网上计费时长”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$last_day"
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
		,'R161_15'
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
	if {$RESULT_VAL3>=0.2 || $RESULT_VAL3<=-0.2 } {
		set grade 2
	    set alarmcontent "R161_15 波动性检查使用TD网络的客户在T网上计费时长超出20%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_15 ---------------------------------------"




    #R161_16:使用TD网络的客户在T网上的数据流量

	#取统计日的指标值：使用TD网络的客户数据流量日汇总(tb_sum_td_usd_net_cust_data_d)表中，
	#入库日期为统计日的“使用TD网络的客户在T网上的数据流量”。
	
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$timestamp"
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

    #取上个统计日的指标值：使用TD网络的客户数据流量日汇总(tb_sum_td_usd_net_cust_data_d)表中，
    #入库日期为上个统计日的“使用TD网络的客户在T网上的数据流量”。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$last_day"
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
		,'R161_16'
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
	if {$RESULT_VAL3>=0.2 || $RESULT_VAL3<=-0.2 } {
		set grade 2
	    set alarmcontent "R161_16 波动性检查使用TD网络的客户在T网上的数据流量超出20%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_16 ---------------------------------------"






    #R161_17:使用TD网络的客户在T网上的数据流量

	#取统计日的指标值：从22012（日KPI）接口取得“离网客户数”字段值
	
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

    #取上个统计日的指标值：第一步：取用户月历史表中生效日期eff_dt在上个统计日内且失效日期end_dt大于上个统计日，
    #用户类型编码不等于3（测试）并且数据SIM卡用户标志不等于1的数据。
    #第二步：关联用户状态历史表（tb_svc_subs_stat_hist）中状态生效日期eff_dt在上个统计日内且失效日期end_dt
    #大于上个统计日, 用户状态类型编码为2010（主动销号）、2020（被动销号）和2030（冷冻期）的用户数。
    #第三步：按省公司标识汇总出各省公司的离网用户数。
    
	set handle [ aidb_open $conn ]
	set sqlbuf "
			select count(distinct user_id) from session.check_user_status_1
			where usertype_id IN ('2010','2020','2030','9000')
			  and test_flag='0'
			  and time_id=$last_day
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
		,'R161_17'
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
	if {$RESULT_VAL3>=0.7 || $RESULT_VAL3<=-0.7 } {
		set grade 2
	    set alarmcontent "R161_17 波动性检查离网客户数超出70%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R161_17 ---------------------------------------"



	

	
	return 0
}

