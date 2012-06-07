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
        ##~   set optime_month 2011-12
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $op_time
        puts $op_month
        puts $last_month
		
        global app_name
        set app_name "G_I_02006_MONTH.tcl"
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay


  #删除本期数据
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	exec_sql $sql_buff
	
	set sql_buff "alter table bass1.g_i_02006_month_1 activate not logged initially with empty table"
	exec_sql $sql_buff


##~   1.非全球通品牌的客户-包含品牌奖励积分 
##~   2-3月全球通、动感地带2品牌“专项转移积分”均存在数据；
##~   1-3月神州行品牌“专项转移积分”均存在数据；
	set sql_buff "
		insert into G_I_02006_MONTH_1
		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=$op_month and scrtype=24 and b.brand_id = '1' then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  count_cycle_id<=$op_month and $op_month <= 201112 and scrtype=5  and b.brand_id in ('1','3') then orgscr+adjscr else 0 end )   as trans_points --只有全/动才有转移积分 而且 应该放到一月份中，后续月份没有转移积分。
			,sum(  CURSCR )   as convertible_points --此算法没有踢除负数，如果碰到负的，也计算了。故在所有已转换的积分中加上|-的积分|
			,sum( case when  (orgscr+adjscr) < 0 and scrtype = 99 then 0 else orgscr+adjscr end )   as all_points --剔除积分为负的。因为负的是活动透支积分，非正常消费获得。这里只计算正常获得的积分。负的在这里表示：用户还需要返回这么多积分。表中的标识为 scrtype = 99 and orgscr+adjscr < 0 
			,sum( case when  scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( case when CURSCR < 0 then USRSCR+abs(CURSCR) else USRSCR end )   as all_converted_points --与convertible_points 相应
			,0 LEAVE_CLEAR_POINTS
			,0 OTHER_CLEAR_POINTS
		from bass2.dwd_product_sc_scorelist_$op_month  a ,  bass1.INT_02004_02008_MONTH_$op_month  b
		where   actflag='1' and count_cycle_id <= $op_month
		and a.PRODUCT_INSTANCE_ID = b.user_id
		group by product_instance_id
		with ur
	"
	
##~   针对12月数据做临时处理，把dwd_product_sc_scorelist_201201 scrtype=5    合到  bass2.dwd_product_sc_scorelist_201112 中
	##~   set sql_buff "
		##~   insert into G_I_02006_MONTH_1
		##~   select
			##~   product_instance_id  as user_id
			##~   ,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			##~   ,sum( case when  count_cycle_id=$op_month and scrtype=24 and b.brand_id = '1' then orgscr+adjscr else 0 end )   as month_qqt_points
			##~   ,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			##~   ,sum( case when  b.brand_id in ('1','3') then trans_points else 0 end )   as trans_points --只有全/动才有转移积分 而且 应该放到12月份中，后续月份没有转移积分。
			##~   ,sum(  CURSCR )   as convertible_points --此算法没有踢除负数，如果碰到负的，也计算了。故在所有已转换的积分中加上|-的积分|
			##~   ,sum( case when  (orgscr+adjscr) < 0 and scrtype = 99 then 0 else orgscr+adjscr end )   as all_points --剔除积分为负的。因为负的是活动透支积分，非正常消费获得。这里只计算正常获得的积分。负的在这里表示：用户还需要返回这么多积分。表中的标识为 scrtype = 99 and orgscr+adjscr < 0 
			##~   ,sum( case when  scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			##~   ,sum( case when CURSCR < 0 then USRSCR+abs(CURSCR) else USRSCR end )   as all_converted_points --与convertible_points 相应
			##~   ,0 LEAVE_CLEAR_POINTS
			##~   ,0 OTHER_CLEAR_POINTS
		##~   from bass2.dwd_product_sc_scorelist_$op_month  a ,  bass1.INT_02004_02008_MONTH_$op_month  b
			##~   ,   (  select product_instance_id user_id, sum( case when  count_cycle_id<=201112   then orgscr+adjscr else 0 end )   as trans_points
					##~   from (
							##~   select * from bass2.dwd_product_sc_scorelist_201201   where  scrtype  = 5 		
				##~   ) t group by product_instance_id 
			##~   ) c
		##~   where   actflag='1' and count_cycle_id <= $op_month
		##~   and a.PRODUCT_INSTANCE_ID = b.user_id
		##~   and b.user_id = c.user_id
		##~   group by product_instance_id
		##~   with ur
	##~   "
	
	exec_sql $sql_buff

##~   注意：1.可兑换+已兑换 应= 全部积分
##~   2.如果可兑换为负，那么就把负的转成0，同时把已兑换的置成： 已兑换 = 全部积分

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

##~   20120524       为满足校验，修改口径：  ,char(sum(value(case when b.brand_id = 1 then a.MONTH_QQT_POINTS else 0 end ,0)))
##~   R282	月	10_积分计划	非全球通品牌的客户不应上传品牌奖励积分	02006 用户积分情况	非全球通品牌的客户不应上传品牌奖励积分	0.05	



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
        ,char(sum(value(case when a.CONVERTIBLE_POINTS < 0 then 0 else a.CONVERTIBLE_POINTS end ,0)))
        ,char(sum(value(a.ALL_POINTS,0)))
        ,char(sum(value(a.ALL_CONSUME_POINTS,0)))
        ,char(sum(value(case when a.CONVERTIBLE_POINTS < 0 then a.ALL_POINTS else a.ALL_CONVERTED_POINTS end ,0)))
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

##~   20120524
##~   --~   R283	月	10_积分计划	当前可兑换积分不能为负值	02006 用户积分情况	当前可兑换积分不能为负值	0.05	

##~   注意：1.可兑换+已兑换 应= 全部积分
##~   2.如果可兑换为负，那么就把负的转成0，同时把已兑换的置成： 已兑换 = 全部积分
##~   不用delete ， 以免影响积分客户数

##~   2.当前可兑换积分为负值
##~   set sql_buff "
	##~   delete from ( select * from G_I_02006_MONTH where time_id = $op_month and  bigint(CONVERTIBLE_POINTS) < 0 ) t 
       ##~   "
	##~   exec_sql $sql_buff





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


set sql_buff "
	select count(0)
	from G_I_02006_MONTH
	where bigint(ALL_POINTS) <> bigint(ALL_CONVERTED_POINTS) + bigint(CONVERTIBLE_POINTS) 
	and time_id = $op_month
	with ur
"

chkzero2 $sql_buff "总积分<>已兑换+可兑换"




  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "g_i_02006_month"
        set pk                  "USER_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
                
				
	return 0
}


##~   20120524:
##~   rules:
##~   CONVERTIBLE_POINTS+ALL_CONVERTED_POINTS= ALL_POINTS
