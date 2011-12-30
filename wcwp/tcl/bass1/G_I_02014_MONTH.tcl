######################################################################################################
#接口单元名称：用户选择全球通88套餐
#接口单元编码：02014
#接口单元说明：截至当月底最后一天24时，所有订购全球通88套餐的有效用户的订购关系记录
#程序名称: G_I_02014_MONTH.tcl
#功能描述: 生成02014的数据
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
	    set app_name "G_I_02014_MONTH.tcl"

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02014_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

####90001311	全球通88套餐88元
####90001312	全球通88套餐128元
####90001313	全球通88套餐188元
####90001314	全球通88套餐288元
####90001315	全球通88套餐388元
####90001316	全球通88套餐588元
####90001317	全球通88套餐888元
####90001318	全球通88套餐1688元
####无58元套餐定义的

##90001331	1	全球通上网套餐58元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
##90001334	1	全球通商旅套餐58元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
##90001343	1	全球通本地套餐58元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000

###90001332	1	全球通上网套餐88元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
###90001335	1	全球通商旅套餐88元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
###90001344	1	全球通本地套餐88元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000


##90001333	1	全球通上网套餐128元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
##90001336	1	全球通商旅套餐128元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
##90001345	1	全球通本地套餐128元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000

#90001337	1	全球通商旅套餐158元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
#90001338	1	全球通商旅套餐188元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
#90001339	1	全球通商旅套餐288元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
#90001340	1	全球通商旅套餐388元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
#90001341	1	全球通商旅套餐588元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000
#90001342	1	全球通商旅套餐888元	0	2011-04-22 0:00:00.000000	2030-01-01 0:00:00.000000

#商旅套餐188元，对应“3”
#商旅套餐288元，对应“4”
#商旅套餐388元，对应“5”
#商旅套餐588元，对应“6”
#商旅套餐888元，对应“7”
#商旅套餐158元，对应“9”


    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02014_month
	     (time_id,prod_id,user_id,valid_date)
		select
		    $op_month,
			case 
			     when b.prod_id in (90001331,90001334,90001343) then '0'
			     when b.prod_id in (90001311,90001332,90001335,90001344) then '1'
			     when b.prod_id in (90001312,90001333,90001336,90001345) then '2'
			     when b.prod_id in (90001313,90001338) then '3'
			     when b.prod_id in (90001314,90001339) then '4'
			     when b.prod_id in (90001315,90001340) then '5'
			     when b.prod_id in (90001316,90001341) then '6'
			     when b.prod_id in (90001317,90001342) then '7'
			     when b.prod_id in (90001318) then '8'
			     when b.prod_id in (90001337) then '9'
			 end prod_id,
			 a.user_id,
			 replace(char(date(a.valid_date)),'-','') valid_date
		from bass2.dwd_product_sprom_active_${last_month_day} a,
			 bass2.dim_product_item b
		where a.sprom_id=b.prod_id
		  and b.prod_id in
(90001311,90001312,90001313,90001314,90001315,90001316,90001317,90001318,90001344
,90001331 ,90001332 ,90001333 ,90001334 ,90001335 ,90001336 ,90001337 ,90001338 ,90001339 ,90001340 ,90001341 ,90001342 ,90001343 ,90001344 ,90001345 )
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
	set sql_buff "delete from bass1.g_i_02014_month where time_id = $op_month and user_id in
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
  
  #处理业务口径错误
  #建立临时表
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_02014_month_tmp
	             (
							  time_id     integer         not null,
							  prod_id     character(1)    not null,
							  user_id     character(20)   not null,
							  valid_date  character(8)    not null
                )
                partitioning key
                (user_id)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp
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


  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02014_month_tmp
					select time_id,prod_id,user_id,valid_date 
					from 
					(
					    select time_id,prod_id,user_id,valid_date,row_number() over(partition by user_id order by valid_date desc ) row_id
					     from bass1.g_i_02014_month
					    where user_id in 
					    (
					        select user_id from 
					        (
					            select user_id,count(*) cnt from bass1.g_i_02014_month
					            where time_id =$op_month
					            group by user_id
					            having count(*)>1
					        ) b
					    )
					    and time_id =$op_month
					) c
					where c.row_id=1
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


  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.g_i_02014_month
					where user_id in 
					(
					    select user_id from 
					    (
					    select user_id,count(*) cnt from bass1.g_i_02014_month
					    where time_id =$op_month
					    group by user_id
					    having count(*)>1
					) b
					)
					and time_id =$op_month
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


  set handle [aidb_open $conn]
	set sql_buff "insert into  bass1.g_i_02014_month
					select * from session.g_i_02014_month_tmp
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
	             select user_id,count(*) cnt from bass1.g_i_02014_month
	              where time_id =$op_month
	             group by user_id
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
	        set alarmcontent "02014接口用户办理全球通套餐主键唯一性校验未通过"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


	return 0
}