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
#          3.1.7.8规范修改 panzw 20120117
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


      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay


  #删除本期数据
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	exec_sql $sql_buff
	
	set sql_buff "alter table bass1.g_i_02006_month_1 activate not logged initially with empty table"
	exec_sql $sql_buff
	
	set sql_buff "
		insert into G_I_02006_MONTH_1
		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=$op_month and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum(  CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
			,0 LEAVE_CLEAR_POINTS
			,0 OTHER_CLEAR_POINTS
		from bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1
		where   actflag='1' and  scrtype<>5 and count_cycle_id <= $op_month
		group by product_instance_id
		with ur
	"
	exec_sql $sql_buff

##~   sc_scorelist 表中SCRTYPE:为积分类型：

##~   01：消费积分 

##~   21：奖励积分 

##~   23：转赠积分 

##~   24：品牌积分 

##~   25：网龄积分 

##~   05：其他 



##~   积分转换：
##~   1.2008年之后
##~   2.分品牌
##~   3.按BOSS操作，转成5 ，再应用原口径

	set sql_buff "
		insert into G_I_02006_MONTH_1
		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=$op_month and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum(  CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
			,0 LEAVE_CLEAR_POINTS
			,0 OTHER_CLEAR_POINTS
		from (select * from bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1 where scrtype = 5) a
		where   actflag='1' and count_cycle_id <= $op_month
		group by product_instance_id
		with ur
	"
	exec_sql $sql_buff
	

	set sql_buff "
	insert into G_I_02006_MONTH_1
			(
				USER_ID
				,LEAVE_CLEAR_POINTS
			)
select 
		b.user_id
		,sum(case when b.month_off_mark = 1 then c.end_month_score else 0 end) LEAVE_CLEAR_POINTS
		from bass2.dw_product_$op_month b,
		     bass2.ods_product_sc_month_backup_$last_month c
		where b.user_id=c.product_instance_id
		  and b.month_off_mark = 1
		group by b.user_id
with ur
"
	exec_sql $sql_buff
	
	
#OTHER_CLEAR_POINTS

set sql_buff "
	insert into G_I_02006_MONTH_1
	  (
		user_id
		,OTHER_CLEAR_POINTS
	  )
	select 
		user_id
		,sum(case when chgbrand_mark = 1 then change_score else 0 end) OTHER_CLEAR_POINTS
	from bass2.dw_product_$op_month
	where chgbrand_mark = 1
	group by user_id 
	having sum(case when chgbrand_mark = 1 then change_score else 0 end) > 0
	with ur
       "
	exec_sql $sql_buff

# 年底积分清零，每月都记录T-3年的，不报	
#		set sql_buff "
#		insert into G_I_02006_MONTH_1
#				(
#					USER_ID
#					,OTHER_CLEAR_POINTS
#				)
#	select 
#			b.user_id
#			,sum(case when b.month_off_mark = 1 then c.END_SCORE else 0 end) LEAVE_CLEAR_POINTS
#			from bass2.dw_product_$op_month b,
#				bass2.ods_product_sc_month_backup_$op_month c
#			where b.user_id=c.product_instance_id
#			group by b.user_id
#			having sum(case when b.month_off_mark = 1 then c.END_SCORE else 0 end)
#	with ur
#	"
#		exec_sql $sql_buff
#		




set sql_buff "
	insert into G_I_02006_MONTH
	  (
         TIME_ID
        ,USER_ID
        ,MONTH_POINTS
        ,MONTH_QQT_POINTS
        ,MONTH_AGE_POINTS
        ,TRANS_POINTS
        ,CONVERTIBLE_POINTS
        ,ALL_POINTS
        ,ALL_CONSUME_POINTS
        ,ALL_CONVERTED_POINTS
        ,LEAVE_CLEAR_POINTS
        ,OTHER_CLEAR_POINTS
	  )
	select 
        $op_month TIME_ID
        ,a.USER_ID
        ,char(sum(value(a.MONTH_POINTS,0)))
        ,char(sum(value(a.MONTH_QQT_POINTS,0)))
        ,char(sum(value(a.MONTH_AGE_POINTS,0)))
        ,char(sum(value(a.TRANS_POINTS,0)))
        ,char(sum(value(a.CONVERTIBLE_POINTS,0)))
        ,char(sum(value(a.ALL_POINTS,0)))
        ,char(sum(value(a.ALL_CONSUME_POINTS,0)))
        ,char(sum(value(a.ALL_CONVERTED_POINTS,0)))
        ,char(sum(value(a.LEAVE_CLEAR_POINTS,0)))
        ,char(sum(value(a.OTHER_CLEAR_POINTS,0)))
	from G_I_02006_MONTH_1 a
	,bass2.dw_product_$op_month b
	   where a.user_id = b.user_id 
		 and b.usertype_id in (1,2,9)
		 and b.test_mark = 0
		 and (b.userstatus_id in (1,2,3,6,8) or b.month_off_mark = 1)
   group by a.user_id
	with ur
       "
	exec_sql $sql_buff

#	
#	
#	  #到期可兑换积分<0则到期可兑换积分=0
#	  set handle [aidb_open $conn]
#		set sql_buff "update bass1.g_i_02006_month set canuse_point = '0' where time_id = $op_month and bigint(canuse_point)<0 "
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	
#	
#		#如果累计已兑现积分<0则为0
#	  set handle [aidb_open $conn]
#		set sql_buff "update bass1.g_i_02006_month set cash_pointlj = '0' where time_id = $op_month and bigint(cash_pointlj) < 0 "
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	
#	
#	  #R048 积分表中不含神州行用户，或神州行用户积分为零
#	  set handle [aidb_open $conn]
#		set sql_buff "delete from g_i_02006_month where time_id = $op_month and user_id in
#	                      (select user_id from  bass1.month_02006_mid2) with ur"
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	


	return 0
}



