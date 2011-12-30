######################################################################################################
#接口名称：用户选择资费营销案历史
#接口编码：02010
#接口说明：用户选择的资费营销案/业务包。
#程序名称: G_I_02010_MONTH.tcl
#功能描述: 生成02010的数据
#运行粒度: 月
#源    表：(已去掉)1.bass2.dwd_product_sprom_history_yyyymmdd(用户套餐关系(历史))
#          (已去掉)2.bass2.dwd_product_sprom_active_yyyymmdd(用户套餐关系(在用))
#					 1.bass2.dw_product_sprom_ds (用户套餐关系表)
#          2.bass2.dw_product_func_yyyymm (用户功能关系)
#          3.bass2.dw_product_yyyymm (用户资料表)
#          4.bass1.g_i_02001_month 
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.liuzhilong 20090701 根据胡银辉要求修改口径
#          2.liuzhilong 20091002 为满足"R013用户资料表中的在网用户都应该在用户选择资费营销案表中存在" 校验 加入修正代码
#  20100120 修改在网用户口径userstatus_id in (1,2,3,6,8) 不排除数据卡 sim_code<>'1'
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        puts $optime_month
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

				#本月 yyyy-mm
				set opmonth $optime_month
				#获取数据月份1号yyyy-mm-01
				set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
				#获取数据月份下月1号yyyy-mm-01
				set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
				#获取数据月份本月末日yyyy-mm-31
				set op_month_last_iso [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]
				#获取数据月份本月末日yyyymm31
				set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
			 

        #删除本期数据
        set handle [aidb_open $conn]
	      set sql_buff "delete from bass1.g_i_02010_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


##	set sql_buff "insert into bass1.G_I_02010_MONTH
##                   (
##                   time_id
##                   ,plan_id
##                   ,user_id
##                   )
##                  select
##                    distinct
##                    $op_month
##                    ,t.plan_id
##                    ,t.user_id
##                  from
##                  (
##                     select
##                       char(sprom_id) as plan_id
##                       ,char(user_id) as user_id
##                     from
##                       bass2.dwd_product_sprom_history_$this_month_last_day
##                     union all
##                     select
##                       char(sprom_id) as plan_id
##                       ,char(user_id) as user_id
##                     from
##                       bass2.dwd_product_sprom_active_$this_month_last_day
##                  )t "
 set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_I_02010_MONTH
                   (
                   time_id
                   ,plan_id
                   ,user_id
                   )
                  select
                    distinct
                    $op_month
                    ,t.plan_id
                    ,t.user_id
                  from
                  (
                     select  char(a.sprom_id) as plan_id
                            ,char(a.user_id) as user_id
                     from bass2.dw_product_sprom_$op_month   a
                     inner join bass2.dw_product_$op_month c on a.user_id=c.user_id    
                     where   a.valid_date < date('$nt_month_01')
			                      and a.expire_date >= date('$op_month_01')
                     				and c.userstatus_id in (1,2,3,6,8)
                     				and c.usertype_id in (1,2,9)
                     		union all
                     select  char(a.plan_id) as plan_id
                            ,char(a.user_id) as user_id
                     from bass2.dw_product_func_$op_month a
                     inner join bass2.dw_product_$op_month c on a.user_id=c.user_id 
                     where  a.expire_date>=date('$op_month_01')
                     				and c.userstatus_id in (1,2,3,6,8)
                     				and c.usertype_id in (1,2,9)
                  )t

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

######  20091002 为满足"R013用户资料表中的在网用户都应该在用户选择资费营销案表中存在" 校验 加入修正代码
 set handle [aidb_open $conn]
	set sql_buff "INSERT INTO bass1.G_I_02010_MONTH(TIME_ID,PLAN_ID,USER_ID)
				SELECT $op_month,CHAR(PLAN_ID),CHAR(USER_ID) 
				FROM BASS2.DW_PRODUCT_$op_month
				WHERE user_id in (
							select  t.user_id
							from (select user_id 
										,usertype_id
										,row_number() over(partition by user_id order by time_id desc ) row_id
										from bass1.g_a_02008_day
										where time_id <=$this_month_last_day ) t
					    inner join (select user_id 
					                ,sim_code
					                ,usertype_id
					            ,row_number() over(partition by user_id order by time_id desc ) row_id
					    from bass1.g_a_02004_day
					    where time_id <=$this_month_last_day ) f   on t.user_id=f.user_id
							where t.row_id=1 and f.row_id=1 
							and t.usertype_id NOT IN ('2010','2020','2030','9000')
							and f.usertype_id<>'3'
							and t.user_id not in ( select distinct user_id from bass1.G_I_02010_MONTH where time_id=$op_month)
					)
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
###########
  
		#去掉不存在 02001 的	plan_id 行
		set handle [aidb_open $conn]
		set sql_buff " delete from bass1.G_I_02010_MONTH 
				        where plan_id not in 
				             (select plan_id from g_i_02001_month 
				               where time_id=$op_month)
								 and time_id=$op_month
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



	return 0
}