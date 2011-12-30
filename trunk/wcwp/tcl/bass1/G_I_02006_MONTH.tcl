######################################################################################################
#接口名称：用户积分变更情况
#接口编码：02006
#接口说明：记录用户积分变化后的月末快照
#程序名称: g_i_02006_month.tcl
#功能描述: 生成02006的数据
#运行粒度: 月
#源    表：1.bass2.ods_product_sc_scorelist_yyyymm(积分_明细表)
#          2.bass2.dw_product_$op_month
#          3.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2010-06-08
#问题记录：1.注意，只割接月需转化用户ID
#修改历史: 1.南方基地上线新脚本/新的业务抓取口径，
#          2.修改月离网清零积分口径 liuqf 20110105
#          3.1.7.1规范修改 liuqf 20110127
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        #set op_time 2008-10-01
        #set optime_month 2008-10
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $op_time
        puts $op_month
        puts $last_month


#      set year 2008
#      set month 07
#      set day 01

      scan   $op_time "%04s-%02s-%02s" year month day
      set    tmp_time_string  ""

      set  threeyear [expr $year-3]
      puts "--------------------------------------"
      puts $threeyear
      puts "--------------------------------------"
      
      ##两年前的1月1日,如：2007-01-01
      set  twoyear [expr $year-2]
      puts $twoyear

      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"
      set    BeginDate  ${twoyear}${sa}
      set    lasttwo_year_begin_month  ${twoyear}${one}
      puts   $BeginDate
      puts   $lasttwo_year_begin_month

      ##一年前的1月1日,如：2008-01-01
      set  oneyear [expr $year-1]
      puts $oneyear

      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"
      set    EndDate  ${oneyear}${sa}
      set    last_year_begin_month  ${oneyear}${one}
      puts   $EndDate
      puts   $last_year_begin_month

      ##当年的1月1日,如：2009-01-01
      set  syear [expr $year]
      puts $syear

      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"
      set    NowDate  ${syear}${sa}
      set    this_year_begin_month  ${syear}${one}
      puts   $this_year_begin_month
      puts $NowDate

      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay


      #得到上个月的1号日期
      set sql_buff "select date('$ThisMonthFirstDay')-1 month from bass2.dual"
	    puts $sql_buff
	    set LastMonthFirstDay [get_single $sql_buff]
	    puts $LastMonthFirstDay
      set LastMonthYear [string range $LastMonthFirstDay 0 3]
      set LastMonth [string range $LastMonthFirstDay 5 6]

      #得到下个月的1号日期
      set sql_buff "select date('$ThisMonthFirstDay')+1 month from bass2.dual"
	    puts $sql_buff
	    set NextMonthFirstDay [get_single $sql_buff]
	    puts $NextMonthFirstDay
      set NextMonthYear [string range $NextMonthFirstDay 0 3]
      set NextMonth [string range $NextMonthFirstDay 5 6]

      set NextMonth06 [string range $NextMonthFirstDay 0 3][string range $NextMonthFirstDay 5 6]06
      puts $NextMonth06
      set charge_date1 "$NextMonthFirstDay 00:00:00"
      puts $charge_date1
      set charge_date2 "[string range $NextMonthFirstDay 0 7]04 00:00:00"
      puts $charge_date2
      #charge_date> '2008-02-01 00:00:00' and charge_date < '2008-02-04 00:00:00';"



  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #清空临时表bass1.month_02006_mid2
  set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE bass1.month_02006_mid2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  # 修正神州行品牌用户取数逻辑
  set handle [aidb_open $conn]
	set sql_buff "
				 insert into bass1.month_02006_mid2
				 select t.user_id
				 from ( select user_id,brand_id,row_number() over(partition by user_id order by time_id desc ) row_id
				 				from bass1.g_a_02004_day
				 				where time_id<=$this_month_last_day ) t
				 where t.row_id=1 and t.brand_id='2'
				 with ur "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #建立临时表
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_02006_month_tmp
	                 (
                         user_id             character(20),
                         point_sum           bigint,
                         free_point          bigint,
                         used_point          bigint,
                         t_used_point        bigint,
                         tone_used_point     bigint,
                         ttwo_used_point     bigint,
                         tthree_used_point   bigint,
                         used_pointlj        bigint,
                         coms_pointlj        bigint,
                         cash_pointlj        bigint,
                         canuse_point        bigint,
                         off_point           bigint,
                         change_point        bigint
                      )
                      partitioning key
                      (user_id)
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


  #当月消费积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,point_sum
	  )
		select
			product_instance_id  as user_id
			,sum(orgscr+adjscr)   as point_sum
		from bass2.dwd_product_sc_scorelist_$op_month
		where scrtype=1 and count_cycle_id=$op_month and actflag='1'
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #当月奖励积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,free_point
	  )
		select
			product_instance_id  as user_id
			,sum(orgscr+adjscr) free_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where scrtype=2 and count_cycle_id=$op_month and actflag='1'
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #当前可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,used_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) used_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1'
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #T年可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,t_used_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) t_used_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1' and year=$syear
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #T-1年可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,tone_used_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) tone_used_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1' and year=$oneyear
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #T-2年可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,ttwo_used_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) ttwo_used_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1' and year=$twoyear
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #20110126 new add by 1.7.1
  #T-3年可兑换积分(历史可兑换积分)
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,tthree_used_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) tthree_used_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1' and year<=$threeyear
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #为了完成南方基地user_id变化无缝连接，需处理02006上个月的用户字段数据
  #同时计算 累计总积分值/累计总消费积分/累计已兑现积分值 3个字段数值
  #当前月的加上上个月的累积值
  set handle [aidb_open $conn]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,used_pointlj
		,coms_pointlj
		,cash_pointlj
	  )
	  select 
        user_id
		,sum(aa.used_pointlj)
		,sum(aa.coms_pointlj)
		,sum(aa.cash_pointlj)
	   from
	    (
		select 
	     a.user_id
			,bigint(used_pointlj) used_pointlj
			,bigint(coms_pointlj) coms_pointlj
			,bigint(cash_pointlj) cash_pointlj
		from bass1.g_i_02006_month a
		where a.time_id=${last_month}
		union all
		select
			product_instance_id  as user_id
			,sum(orgscr+adjscr) used_pointlj
			,0 coms_pointlj
			,0 cash_pointlj
		from bass2.dwd_product_sc_scorelist_$op_month
		where count_cycle_id=$op_month and actflag='1'
		group by product_instance_id
	    union all
	    	select
			product_instance_id  as user_id
			,0 used_pointlj
			,sum(orgscr+adjscr) coms_pointlj
			,0 cash_pointlj
		from bass2.dwd_product_sc_scorelist_$op_month
		where scrtype=1 and count_cycle_id=$op_month and actflag='1'
		group by product_instance_id
	    union all
	    	select
			product_instance_id  as user_id
			,0 used_pointlj
			,0 coms_pointlj
			,sum(usrscr) cash_pointlj
		from bass2.dwd_product_sc_scorelist_$op_month
		where count_cycle_id=$op_month and actflag='1'
		group by product_instance_id
		) as aa
	group by aa.user_id
	"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #到期可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,canuse_point
	  )
		select
			product_instance_id  as user_id
			,sum(curscr) canuse_point
		from bass2.dwd_product_sc_scorelist_$op_month
		where actflag='1' and year=$twoyear
		group by product_instance_id
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #离网清零积分
###	set handle [ aidb_open $conn ]
###	set sql_buff "
###	insert into session.g_i_02006_month_tmp 
###	  (
###		user_id
###		,off_point
###	  )
###		select
###			a.product_instance_id  as user_id
###			,sum(a.curscr) off_point
###		from bass2.dwd_product_sc_scorelist_$op_month a,
###		     bass2.dw_product_$op_month b
###		where a.product_instance_id=b.user_id
###		  and a.actflag='1' and b.month_off_mark = 1
###		group by a.product_instance_id
###       "
###
###	puts $sql_buff
###	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2020
###		puts $errmsg
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,off_point
	  )
		select 
		    b.user_id
		    ,sum(case when b.month_off_mark = 1 then c.end_month_score else 0 end) off_point
		from bass2.dw_product_$op_month b,
		     bass2.ods_product_sc_month_backup_$last_month c
		where b.user_id=c.product_instance_id
		  and b.month_off_mark = 1
		group by b.user_id
   "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #转网清零积分，直接抓取二经用户表里头的
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	  (
		user_id
		,change_point
	  )
	select 
		user_id
		,sum(case when chgbrand_mark = 1 then change_score else 0 end) change_point
	from bass2.dw_product_$op_month
	where chgbrand_mark = 1
	group by user_id 
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #汇总到结果表(剔除测试)
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02006_month
                   select
                     $op_month
                     ,a.user_id
                     ,char(sum(value(point_sum,0)))
                     ,char(sum(value(free_point,0)))
                     ,char(sum(value(used_point,0)))
                     ,char(sum(value(t_used_point,0)))
                     ,char(sum(value(tone_used_point,0)))
                     ,char(sum(value(ttwo_used_point,0)))
                     ,char(sum(value(tthree_used_point,0)))
                     ,char(sum(value(used_pointlj,0)))
                     ,char(sum(value(coms_pointlj,0)))
                     ,char(sum(value(cash_pointlj,0)))
                     ,char(sum(value(canuse_point,0)))
                     ,char(sum(value(off_point,0)))
                     ,char(sum(value(change_point,0)))
                   from
                     session.g_i_02006_month_tmp a,
                     bass2.dw_product_$op_month b
                   where a.user_id = b.user_id 
                     and b.usertype_id in (1,2,9)
                     and b.test_mark = 0
                   group by a.user_id
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



  #到期可兑换积分<0则到期可兑换积分=0
  set handle [aidb_open $conn]
	set sql_buff "update bass1.g_i_02006_month set canuse_point = '0' where time_id = $op_month and bigint(canuse_point)<0 "
    
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#如果累计已兑现积分<0则为0
  set handle [aidb_open $conn]
	set sql_buff "update bass1.g_i_02006_month set cash_pointlj = '0' where time_id = $op_month and bigint(cash_pointlj) < 0 "
    
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #R048 积分表中不含神州行用户，或神州行用户积分为零
  set handle [aidb_open $conn]
	set sql_buff "delete from g_i_02006_month where time_id = $op_month and user_id in
                      (select user_id from  bass1.month_02006_mid2) with ur"
    
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



	return 0
}

#内部函数部分
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle


	return $result
}
#--------------------------------------------------------------------------------------------------------------





