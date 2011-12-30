######################################################################################################
#接口名称：用户积分变更情况
#接口编码：02006
#接口说明：记录用户积分变化后的月末快照
#程序名称: g_i_02006_month.tcl
#功能描述: 生成02006的数据
#运行粒度: 月
#源    表：1.bass2.dwd_product_scoredetail_yyyymm(用户积分明细)
#          2.bass2.dwd_product_score_yyyymm(用户积分累计)
#          3.bass2.dwd_product_exchgscore_yyyymm(用户积分兑换情况)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：
#编写时间：2007-03-22
#问题记录：1.本接口逻辑上有问题，针对某个用户的某种积分类型编码统计总积分值、可兑现积分余额
#            和已兑现积分值没有意义。所以本程序只在某个用户的积分类型编码为消费积分的时候才统计
#            总积分值、可兑现积分余额和已兑现积分值三个指标，其它的填0。
#          2.目前BOSS送过来的用户积分明细的数据，一个用户一个月只有一条记录。所以该接口数据
#            要UNION表用户积分兑换情况的部分数据
#修改历史: 1.
#	set sql_buff "insert into bass1.g_i_02006_month
#                           select
#                             $op_month
#                             ,point_type_id
#                             ,user_id
#                             ,char(sum(point_sum))
#                             ,char(sum(free_point))
#                             ,char(sum(used_point))
#                           from
#                             session.g_i_02006_month_tmp
#                           group by
#                             point_type_id
#                             ,user_id  "
#   2. 20090714 liuzhilong 修正神州行品牌用户取数逻辑
#   3. 1.6.1规范新增5个字段修改 liuqf
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        #set op_time 2008-10-01
        #set optime_month 2008-10
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $op_time
        puts $op_month



#      set year 2008
#      set month 07
#      set day 01

      scan   $op_time "%04s-%02s-%02s" year month day
      set    tmp_time_string  ""

   ##两年前的1月1日,如：2007-01-01
      set  syear [expr $year-2]
      puts $syear

      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"
      set    BeginDate  ${syear}${sa}
      set    lasttwo_year_begin_month  ${syear}${one}
      puts   $BeginDate
      puts   $lasttwo_year_begin_month

   ##一年前的1月1日,如：2008-01-01
      set  syear [expr $year-1]
      puts $syear

      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"
      set    EndDate  ${syear}${sa}
      set    last_year_begin_month  ${syear}${one}
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

####2009-08-10
####200907
####2007
####2007-01-01 00:00:00
####200701
####2008
####2008-01-01 00:00:00
####200801
####2009
####200901
####2009-01-01 00:00:00
####2009-07-01
####select date('2009-07-01')-1 month from bass2.dual
####2009-06-01
####select date('2009-07-01')+1 month from bass2.dual
####2009-08-01
####20090806
####2009-08-01 00:00:00
####2009-08-04 00:00:00


  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #清空临时表bass1.month_02006_mid1
  set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE bass1.month_02006_mid1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #插入临时表bass1.month_02006_mid1
  set handle [aidb_open $conn]
	set sql_buff "
insert into bass1.month_02006_mid1
select a.user_id,a.Brand_id from
   (select time_id,user_id,Brand_id from G_A_02004_DAY where time_id <= $this_month_last_day) a,
   (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$this_month_last_day group by user_id)b
  where a.time_id=b.time_id and a.user_id=b.user_id
with ur"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #清空临时表bass1.month_02006_mid2
  set handle [aidb_open $conn]
	set sql_buff "
ALTER TABLE bass1.month_02006_mid2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #插入临时表bass1.month_02006_mid2
  set handle [aidb_open $conn]
##	set sql_buff "
##                 insert into bass1.month_02006_mid2
##                 select a.user_id from
##                                                 (select time_id,user_id,Brand_id from G_A_02004_DAY where time_id <= $this_month_last_day and brand_id = '2') a,
##                                                 (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$this_month_last_day and brand_id = '2' group by user_id)b
##                                           where a.time_id=b.time_id and a.user_id=b.user_id with ur"

# 修正 神州行品牌用户取数逻辑
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
		##WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle





#01	消费积分
#02	奖励积分

#  POINT_SUM  CHARACTER(8)    NOT NULL,/*当月消费积分*/
#  FREE_POINT CHARACTER(8)    NOT NULL,/*当月奖励积分*/
#  USED_POINT CHARACTER(8)    NOT NULL,/*当前可兑换积分*/
#
#  USED_POINTLJ CHARACTER(8)    NOT NULL,/*累计总积分值*/
#  COMS_POINTLJ CHARACTER(8)    NOT NULL,/*累计总消费积分*/
#  CASH_POINTLJ CHARACTER(8)    NOT NULL /*累计已兑现积分值*/

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
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#处理T年可兑换积分、T-1年可兑换积分、T-2年可兑换积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	declare global temporary table session.g_i_02006_month_tmp_1
	(
		user_id             varchar(12),
		t_remain_point      bigint,
		tone_remain_point   bigint,
		ttwo_remain_point   bigint,
		point_sum           bigint
	)
	partitioning key (user_id) using hashing
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp_1
	(
		user_id
		,t_remain_point
		,tone_remain_point
		,ttwo_remain_point
		,point_sum
	)
	select
		user_id
		,sum(t2_total_coin) - sum(t2_cost_coin) t_remain_point
		,sum(t1_total_coin) - sum(t1_cost_coin) tone_remain_point
		,sum(t_total_coin) - sum(t_cost_coin) ttwo_remain_point
		,sum(m_cost_coin) point_sum
	from
		(
			select
				user_id
				,sum(total_coin) t2_total_coin
				,0 t2_cost_coin
				,0 t1_total_coin
				,0 t1_cost_coin
				,0 t_total_coin
				,0 t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scoredetail_$op_month
			where
				consume_coin <> 0 and
				month >= '${lasttwo_year_begin_month}' and
				month < '${last_year_begin_month}'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,sum(old_total_score - total_score) t2_cost_coin
				,0 t1_total_coin
				,0 t1_cost_coin
				,0 t_total_coin
				,0 t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scorechg_$op_month
			where
				total_score - old_total_score < 0 and
				alter_date >= '$BeginDate' and
				alter_date < '$EndDate'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,0 t2_cost_coin
				,sum(total_coin) t1_total_coin
				,0 t1_cost_coin
				,0 t_total_coin
				,0 t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scoredetail_$op_month
			where
				consume_coin <> 0 and
				month >= '${last_year_begin_month}' and
				month < '$this_year_begin_month'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,0 t2_cost_coin
				,0 t1_total_coin
				,sum(old_total_score - total_score) t1_cost_coin
				,0 t_total_coin
				,0 t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scorechg_$op_month
			where
				total_score - old_total_score < 0 and
				alter_date >= '$EndDate' and
				alter_date < '$NowDate'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,0 t2_cost_coin
				,0 t1_total_coin
				,0 t1_cost_coin
				,sum(total_coin) t_total_coin
				,0 t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scoredetail_$op_month
			where
				consume_coin <> 0 and
				month >= '$this_year_begin_month' and
				month <= '$op_month'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,0 t2_cost_coin
				,0 t1_total_coin
				,0 t1_cost_coin
				,0 t_total_coin
				,sum(old_total_score - total_score) t_cost_coin
				,0 m_cost_coin
			from
				bass2.dwd_product_scorechg_$op_month
			where
				total_score - old_total_score < 0 and
				alter_date >= '$NowDate' and
				alter_date < '$charge_date1'
			group by
				user_id
			union all
			select
				user_id
				,0 t2_total_coin
				,0 t2_cost_coin
				,0 t1_total_coin
				,0 t1_cost_coin
				,0 t_total_coin
				,0 t_cost_coin
				,sum(consume_coin) m_cost_coin
			from
				bass2.dwd_product_scoredetail_$op_month
			where
				consume_coin <> 0 and
				month = '$op_month'
			group by
				user_id
		) a
	group by
		a.user_id"
	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	(
		user_id
		,point_sum
		,free_point
		,used_point
		,t_used_point
		,tone_used_point
		,ttwo_used_point
		,used_pointlj
		,coms_pointlj
		,cash_pointlj
		,canuse_point
		,off_point
		,change_point
	)
	select
		user_id
		,0 point_sum
		,0 free_point
		,0 used_point
		,case when t_remain_point >= point_sum then t_remain_point - point_sum else 0 end t_used_point
		,case 
			when t_remain_point >= point_sum then 0
			when t_remain_point + tone_remain_point >= point_sum then t_remain_point + tone_remain_point - point_sum
			else 0
		end	tone_used_point
		,case
			when t_remain_point >= point_sum then 0
			when t_remain_point + tone_remain_point >= point_sum then 0
			when t_remain_point + tone_remain_point + ttwo_remain_point >= point_sum then t_remain_point + tone_remain_point + ttwo_remain_point - point_sum 
			else 0
		end ttwo_used_point
		,0 used_pointlj
		,0 coms_pointlj
		,0 cash_pointlj
		,0 canuse_point
		,0 off_point
		,0 change_point
	from
		session.g_i_02006_month_tmp_1"
	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#处理离网清零积分、转网清零积分
	set handle [ aidb_open $conn ]
	set sql_buff "
	insert into session.g_i_02006_month_tmp 
	(
		user_id
		,point_sum
		,free_point
		,used_point
		,t_used_point
		,tone_used_point
		,ttwo_used_point
		,used_pointlj
		,coms_pointlj
		,cash_pointlj
		,canuse_point
		,off_point
		,change_point
	)
	select 
		user_id
		,0 point_sum
		,0 free_point
		,0 used_point
		,0 t_used_point
		,0 tone_used_point
		,0 ttwo_used_point
		,0 used_pointlj
		,0 coms_pointlj
		,0 cash_pointlj
		,0 canuse_point
		,sum(case when month_off_mark = 1 then change_score else 0 end) off_point
		,sum(case when chgbrand_mark = 1 then change_score else 0 end) change_point
	from 
		bass2.dw_product_$op_month
	where 
		change_score <> 0 and
		(month_off_mark = 1 or
		chgbrand_mark = 1) 
	"
	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#插入当月消费积分
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,t_used_point,tone_used_point,ttwo_used_point,used_pointlj,coms_pointlj,cash_pointlj,canuse_point,off_point,change_point)
	                     select user_id,consume_coin,0,0,0,0,0,0,0,0,0
	                       from bass2.dwd_product_scoredetail_$op_month
	                      where consume_coin <> 0 and  month = '$op_month' "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#插入当月奖励积分
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,t_used_point,tone_used_point,ttwo_used_point,used_pointlj,coms_pointlj,cash_pointlj,canuse_point,off_point,change_point)
	                     select user_id,0,total_score-old_total_score,0,0,0,0,0,0,0,0
	                       from bass2.dwd_product_scorechg_$op_month
                        where total_score-old_total_score > 0 and
                              date(Alter_date) >= date('$ThisMonthFirstDay') and
   	                          date(Alter_date) < date('$NextMonthFirstDay') and
	                            alter_type in (6,7,72,73,711,712,61)"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#插入当前可兑换积分
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,t_used_point,tone_used_point,ttwo_used_point,used_pointlj,coms_pointlj,cash_pointlj,canuse_point,off_point,change_point)
	                     select user_id,0,0,change_score,0,0,0,0,0,0,0
	                       from bass2.dw_product_$op_month "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#累计总积分值
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,consume_coin+online_coin+credit_coin+data_coin,0,0,0
	                       from bass2.DWD_PRODUCT_SCOREDETAIL_$op_month
	                      where consume_coin+online_coin+credit_coin+data_coin <> 0"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#累计总积分值 根据市场部工作联络单要求奖励的1000分  5:积分自愿转赠
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,total_score-old_total_score,0,0,0
	                       from bass2.DWD_PRODUCT_SCORECHG_$op_month
	                      where total_score-old_total_score > 0 and alter_type in (5,6,7,11,28,29,40,61,65,71,72,73) "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle






	#累计总消费积分
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,0,consume_coin,0,0
	                       from bass2.DWD_PRODUCT_SCOREDETAIL_$op_month
	                      where consume_coin <> 0"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#累计总积分值 根据市场部工作联络单要求奖励的1000分  5:积分自愿转赠
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,0,total_score-old_total_score,0,0
	                       from bass2.DWD_PRODUCT_SCORECHG_$op_month
	                      where total_score-old_total_score > 0 and alter_type in (5,6,7,11,28,29,40,61,65,71,72,73) "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle




#	#插入累计已兑现积分值
##insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
##	                     select user_id,0,0,0,0,0,total_score-change_score,0
##	                       from bass2.dw_product_$op_month
##	                      where change_score > 0
#  set handle [aidb_open $conn]
#	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
#	                     select user_id,0,0,0,0,0,sum(value),0
#	                       from BASS2.DWD_CM_BUSI_COIN_20080630
#	                      group by user_id"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		##WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle


	#插入累计已兑现积分值 转增出去的积分
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,0,0,old_total_score - total_score,0
	                       from bass2.DWD_PRODUCT_SCORECHG_$op_month
	                      where total_score-old_total_score < 0 "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle




	#插入到期可兑换积分(积分三年有效期内，第一年度产生的积分截至到第三年度当月月底仍未兑换的积分，
	#                 例如，假设当前统计日期为2007年9月，则2005年产生的积分截至到2007年9月30日仍未兑换的积分数为到期可兑换积分，单位：分(M值))
	#程序处理方式：到期可兑换积分=if(第一年度产生的积分-累计已兑换积分 < 0) then 0 else 第一年度产生的积分-累计已兑换积分
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02006_month_tmp (user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select user_id,0,0,0,0,0,0,consume_coin+online_coin+credit_coin+data_coin
	                       from bass2.dwd_product_scoredetail_$op_month
	                      where oper_date > '$BeginDate' and oper_date < '$EndDate'"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

# select * from bass2.dwd_product_scoredetail_200803 where create_date < '2005-04-01 0:00:00' with ur


  set handle [aidb_open $conn]
	set sql_buff "create index session.I_1 on session.g_i_02006_month_tmp(user_id) "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #汇总到结果表(剔除公免，测试剔除不？)
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02006_month
                           select
                             $op_month
                             ,a.user_id
                             ,char(sum(point_sum))
                             ,char(sum(free_point))
                             ,char(sum(used_point))
                             ,char(sum(t_used_point))
                             ,char(sum(tone_used_point))
                             ,char(sum(ttwo_used_point))
                             ,char(sum(USED_POINTLJ))
                             ,char(sum(COMS_POINTLJ))
                             ,char(sum(CASH_POINTLJ))
                             ,char(sum(CanUse_Point-CASH_POINTLJ))
                             ,char(sum(off_point))
                             ,char(sum(change_point))
                           from
                             session.g_i_02006_month_tmp a,
                             bass2.dw_product_$op_month b
                           where a.user_id = b.user_id and free_mark = 0 and test_mark = 0
                           group by
                             a.user_id  with ur"


        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	puts "11111"

  #到期可兑换积分<0则到期可兑换积分=0
  set handle [aidb_open $conn]
	set sql_buff "update bass1.g_i_02006_month set CanUse_Point = '0' where time_id = $op_month and bigint(CanUse_Point)<0 "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#如果累计已兑现积分<0则为0
  set handle [aidb_open $conn]
	set sql_buff "update bass1.g_i_02006_month set CASH_POINTLJ = '0' where time_id = $op_month and bigint(CASH_POINTLJ) < 0 "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#冲正
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02006_month
	                     	select $op_month,a.user_id,'0','0','0',char(bigint(cash_pointlj)+bigint(used_point)-bigint(used_pointlj)),'0','0','0'
	                     	  from
                            (select user_id,used_pointlj,cash_pointlj,used_point from g_i_02006_month where time_id = $op_month and bigint(used_pointlj) < bigint(cash_pointlj)+bigint(used_point)) a,
                            bass1.month_02006_mid1 b
                        where a.user_id = b.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#统一主键
  set handle [aidb_open $conn]
	set sql_buff "update bass1.g_i_02006_month set time_id = 888888 where time_id = $op_month "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02006_month
                           select
                             $op_month
                             ,a.user_id
                             ,char(sum(bigint(point_sum)))
                             ,char(sum(bigint(free_point)))
                             ,char(sum(bigint(used_point)))
                             ,char(sum(bigint(t_used_point)))
                             ,char(sum(bigint(tone_used_point)))
                             ,char(sum(bigint(ttwo_used_point)))
                             ,char(sum(bigint(USED_POINTLJ)))
                             ,char(sum(bigint(COMS_POINTLJ)))
                             ,char(sum(bigint(CASH_POINTLJ)))
                             ,char(sum(bigint(CanUse_Point)))
                             ,char(sum(bigint(off_point)))
                             ,char(sum(bigint(change_point)))
                           from
                             g_i_02006_month a,
                             bass1.month_02006_mid1 b
                           where time_id = 888888 and a.user_id = b.user_id and b.Brand_id <> '2'
                           group by
                             a.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.g_i_02006_month where  time_id = 888888 "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle




##如果全球通累计总消费积分值<1000，那么当前可兑换积分=0 且 到期可兑换积分=0
#  set handle [aidb_open $conn]
#	set sql_buff "update bass1.g_i_02006_month set used_point = '0' , canuse_point = '0' where time_id= $op_month and user_id in (
#	              select a.user_id from
#                     ( select a.user_id,coms_pointlj,used_point,canuse_point
#                         from (select time_id as time_id,user_id as user_id,coms_pointlj,used_point,canuse_point  from bass1.g_i_02006_month where time_id<=$op_month ) a,
#                              (select max(time_id) as time_id,user_id as user_id from bass1.g_i_02006_month where time_id<=$op_month group by user_id)b
#                        where a.time_id=b.time_id and a.user_id=b.user_id)a,
#                      (select a.user_id,a.Brand_id from
#                          (select time_id,user_id,Brand_id from G_A_02004_DAY where time_id <= $this_month_last_day) a,
#                          (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$this_month_last_day group by user_id)b
#                        where a.time_id=b.time_id and a.user_id=b.user_id)b
#                where a.user_id = b.user_id and b.brand_id = '1' and bigint(a.coms_pointlj) < 1000 and bigint(a.used_point) <> 0 and (used_point <> '0' or canuse_point <> '0') ) "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		##WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#
##如果动感地带累计总消费积分值<500，那么当前可兑换积分=0 且 到期可兑换积分=0
#  set handle [aidb_open $conn]
#	set sql_buff "update bass1.g_i_02006_month set used_point = '0' , canuse_point = '0'  where time_id= $op_month and user_id in (
#	              select a.user_id from
#                     ( select a.user_id,coms_pointlj,used_point,canuse_point
#                         from (select time_id as time_id,user_id as user_id,coms_pointlj,used_point,canuse_point  from bass1.g_i_02006_month where time_id<=$op_month ) a,
#                              (select max(time_id) as time_id,user_id as user_id from bass1.g_i_02006_month where time_id<=$op_month group by user_id)b
#                        where a.time_id=b.time_id and a.user_id=b.user_id)a,
#                      (select a.user_id,a.Brand_id from
#                          (select time_id,user_id,Brand_id from G_A_02004_DAY where time_id <= $this_month_last_day) a,
#                          (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$this_month_last_day group by user_id)b
#                        where a.time_id=b.time_id and a.user_id=b.user_id)b
#                where a.user_id = b.user_id and b.brand_id = '3' and bigint(a.coms_pointlj) < 500 and bigint(a.used_point) <> 0  and (used_point <> '0' or canuse_point <> '0') ) "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		##WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle


# R048 积分表中不含神州行用户，或神州行用户积分为零
  set handle [aidb_open $conn]
	set sql_buff "insert into g_i_02006_month (time_id,user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
                   select $op_month,a.user_id,'0','0','0','0','0','0','0' from
                        ( select a.user_id,'0','0',point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point
                           from (select time_id as time_id,user_id as user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point
	                                 from bass1.g_i_02006_month where time_id<=$op_month ) a,
                                (select max(time_id) as time_id,user_id as user_id from bass1.g_i_02006_month where time_id<=$op_month group by user_id)b
                          where a.time_id=b.time_id and a.user_id=b.user_id)a,
                          bass1.month_02006_mid1 b
                   where a.user_id = b.user_id and b.brand_id = '2' and bigint(a.used_point) <> 0  with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


# R048 积分表中不含神州行用户，或神州行用户积分为零
  set handle [aidb_open $conn]
	set sql_buff "delete from g_i_02006_month where time_id = $op_month and user_id in
                      (select user_id from  bass1.month_02006_mid2) with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



 #from bass2.DWD_CM_BUSI_COIN_$NextMonth06 a
  set sql_buff "insert into g_i_02006_month (time_id,user_id,point_sum,free_point,used_point,USED_POINTLJ,COMS_POINTLJ,CASH_POINTLJ,CanUse_Point)
	                     select $op_month,a.user_id,'0','0',char(value),char(value),'0','0','0'
	                       from bass2.DWD_CM_BUSI_COIN_20081106 a
	                      where charge_date > '$charge_date1' and charge_date < '$charge_date2' with ur;"
	puts $sql_buff
  exec_sql $sql_buff

  set sql_buff "update bass1.g_i_02006_month set time_id = 888888 where time_id = $op_month;"
	puts $sql_buff
  exec_sql $sql_buff

  set sql_buff "insert into bass1.g_i_02006_month
                           select
                             $op_month
                             ,a.user_id
                             ,char(sum(bigint(point_sum)))
                             ,char(sum(bigint(free_point)))
                             ,char(sum(bigint(used_point)))
                             ,char(sum(bigint(t_used_point)))
                             ,char(sum(bigint(tone_used_point)))
                             ,char(sum(bigint(ttwo_used_point)))
                             ,char(sum(bigint(USED_POINTLJ)))
                             ,char(sum(bigint(COMS_POINTLJ)))
                             ,char(sum(bigint(CASH_POINTLJ)))
                             ,char(sum(bigint(CanUse_Point)))
                             ,char(sum(bigint(off_point)))
                             ,char(sum(bigint(change_point)))
                           from
                             g_i_02006_month a,
                             bass1.month_02006_mid1 b
                           where time_id = 888888 and a.user_id = b.user_id and b.Brand_id <> '2'
                           group by
                             a.user_id with ur;"
	puts $sql_buff
  exec_sql $sql_buff

  set sql_buff "delete from  bass1.g_i_02006_month where  time_id = 888888;"
	puts $sql_buff
  exec_sql $sql_buff







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
		##WriteTrace "$errmsg" 2005
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
		##WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle


	return $result
}
#--------------------------------------------------------------------------------------------------------------





