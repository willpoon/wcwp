######################################################################################################
#程序名称：INT_CHECK_0200402008_MONTH.tcl
#校验接口：
#运行粒度: 月
#校验规则: R080,R081,R082,R002,R007,R016,R018,R019,R076
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-22
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #自然月第一天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #程序名
        set app_name "INT_CHECK_0200402008_MONTH.tcl"

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #本月最后一天,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #上月最后一天 yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day


	#--R002:在网用户的区域标识∈(1,2,3)的比例不小于75％

  set handle [aidb_open $conn]
	set sql_buff "select cast(sum(case when b.user_id is null then 0 else 1 end) as decimal(15,2))/cast(count(a.user_id) as decimal (15,2))
                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where usertype_id<>'3'
                and SIM_CODE<>'1'
                
                ) c
                 on a.user_id=c.user_id
                
                left outer join (select user_id,region_flag
                from
                (
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) k
                where k.row_id=1) b
                 on a.user_id=b.user_id
                  and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','1040','1021','9000')
                with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R002',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：在网用户的区域标识∈(1,2,3)的比例不小于75％


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0.75 } {
		set grade 2
	        set alarmcontent " R002校验不通过:在网用户的区域标识∈(1,2,3)的比例小于75％"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


	#--R007:集团成员用户中的用户标识都应该在用户表中

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct user_id)
                from BASS1.G_I_02049_MONTH 
                where time_id=$op_month
                and user_id not in (select distinct user_id from bass1.G_A_02004_DAY)
                with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R00709',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：集团成员用户中的用户标识都应该在用户表中


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R00709校验不通过:集团成员用户中的用户标识有不在用户表中的用户"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


	#--R016:历史离网用户不应存在当月账单

  set handle [aidb_open $conn]
	set sql_buff "select count(user_id) from
               (
               select user_id,usertype_id from
               (
               select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                 from BASS1.G_A_02008_DAY
                 where time_id/100<=$op_month
                 ) k
                 where k.row_id=1
                ) l
                where l.usertype_id like '2%' 
                and user_id in (select distinct  a.user_id
                 from BASS1.G_S_03005_MONTH a
                 left outer join (select distinct user_id from BASS1.G_A_02008_DAY
                 where usertype_id like '2%' 
                 and time_id/100=$op_month) b
                 on a.user_id=b.user_id
                 where a.time_id=$op_month
                 and b.user_id is null)  with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R016',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：历史离网用户不应存在当月账单


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R016校验不通过:历史离网用户存在当月账单"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }		 		 



	#--R018:大客户/大用户表中的用户标识应存在于用户表中

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct user_id) from BASS1.G_I_02005_MONTH 
	              where time_id =$op_month
                  and user_id not in (select distinct user_id from BASS1.G_A_02004_DAY)
                with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R018',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：大客户/大用户表中的用户标识应存在于用户表中


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R018校验不通过:大客户/大用户表中的用户标识不存在于用户表中"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




	#--R019:大客户/大用户表中成为大用户日期，应晚于用户表中该用户的入网时间

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct a.user_id) from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_I_02005_MONTH
                where time_id=$op_month
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_A_02004_DAY
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R019',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：大客户/大用户表中成为大用户日期，应晚于用户表中该用户的入网时间


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R019校验不通过:大客户/大用户表中成为大用户日期，早于用户表中该用户的入网时间"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 
		 
	#--R076:积分表中不含神州行用户，或神州行用户积分为零

  set handle [aidb_open $conn]
	set sql_buff "select count(a.user_id) from 
               ( select a.user_id,used_point
                 from (select time_id as time_id,user_id as user_id,used_point  from bass1.g_i_02006_month where time_id=$op_month ) a
                 )a,
                (
                  select user_id,brand_id from
                  (
                  select user_id,brand_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from G_A_02004_DAY
                  where time_id<=$last_month_day
                  ) k
                  where k.row_id=1
                )b
                where a.user_id = b.user_id and b.brand_id = '2' and bigint(a.used_point) <> 0
                with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R076',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：积分表中不含神州行用户，或神州行用户积分为零


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R076校验不通过:积分表中含神州行用户，或神州行用户积分为零"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




   #--R080 月初在网用户当月消费积分为0的比例≤30%
   
   set sqlbuf "select count(a.user_id)
               from 
                 (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,point_sum  from bass1.g_i_02006_month where time_id=$op_month and bigint(point_sum) = 0) a        
                 )a,
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R080',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.3} {
		set grade 2
	        set alarmcontent "R080 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R080	月初在网用户当月消费积分为0的比例≤30%"

		 		 



   #--R081 月初在网用户当前可兑换积分为0的比例≤30%
   
   set sqlbuf "select count(a.user_id)
               from 
               (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,used_point  from bass1.g_i_02006_month where time_id=$op_month and bigint(used_point) = 0) a
                 )a,
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<$l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id   with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R081',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.3} {
		set grade 2
	        set alarmcontent "R081 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R081 月初在网用户当前可兑换积分为0的比例≤30%"


   #--R082 月初在网用户累计总消费积分为0的比例≤20%
   
   set sqlbuf "select count(a.user_id)
               from 
                 (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,used_pointlj  from bass1.g_i_02006_month where time_id=$op_month and bigint(used_pointlj) = 0 ) a
                 )a,
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R082',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.2} {
		set grade 2
	        set alarmcontent "R082 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R082 月初在网用户累计总消费积分为0的比例≤20%"


	#--R077:累计总消费积分≥当月消费积分

  set handle [aidb_open $conn]
	set sql_buff "select count(*) from G_I_02006_MONTH 
	               where time_id = $op_month 
	                  and bigint(coms_pointlj) < bigint(point_sum)  
	              with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R077',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：累计总消费积分≥当月消费积分


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R077校验不通过:累计总消费积分<当月消费积分"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


	#--R078:累计已兑换积分值≥本月兑换积分值

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct a.user_id) from 
                  (
                  select a.user_id,a.cash_pointlj
                    from (select time_id as time_id,user_id as user_id,cash_pointlj as cash_pointlj from bass1.g_i_02006_month where time_id=$op_month ) a
                   )a,
                  (select user_id,sum(bigint(used_point)) as used_point from G_S_02007_MONTH where time_id = $op_month  group by user_id) b
                where a.user_id = b.user_id and bigint(a.cash_pointlj) < bigint(b.used_point) with ur"

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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R078',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：累计已兑换积分值≥本月兑换积分值


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R078校验不通过:累计已兑换积分值<本月兑换积分值"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


   #--R079 参与兑换客户数≤上一个月月底的有兑换资格客户数
   
   set sqlbuf "select count(distinct user_id ) from G_S_02007_MONTH where time_id = $op_month with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id) from 
              ( select a.user_id,coms_pointlj
                from (select time_id as time_id,user_id as user_id,coms_pointlj as coms_pointlj from bass1.g_i_02006_month where time_id =$op_month ) a
              )a,
               (select user_id,Brand_id from G_A_02004_DAY where time_id < $last_month_day )b
               where a.user_id = b.user_id and ((b.brand_id = '1' and bigint(a.coms_pointlj) >= 1000) or (b.brand_id = '3' and bigint(a.coms_pointlj) >= 500)) with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R079',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
#  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
#  puts  $RESULT_VAL3
	if {$RESULT_VAL1>$RESULT_VAL2} {
		set grade 2
	        set alarmcontent "R079 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R079 参与兑换客户数≤上一个月月底的有兑换资格客户数"
		
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