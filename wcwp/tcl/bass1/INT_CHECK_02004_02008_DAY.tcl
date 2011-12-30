######################################################################################################
#程序名称：INT_CHECK_02004_02008_DAY.tcl
#校验接口：G_A_02004_DAY.tcl G_A_02008_DAY.tcl
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-30
#问题记录：
#修改历史: liuqf 090731 规则id原来的R01109 修改为R001
#          20100125 在网用户口径变动 usertype_id not in ('2010','2020','2030','9000') 不排除数据卡 sim_code<>'1'

#          20110526 R001 -> R003 根据：附件 一级经营分析系统数据准确性考核规则（2011）.xls

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
#        #本月第一天 yyyymmdd
#        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]

 set curr_month [string range $op_time 0 3][string range $op_time 5 6]
 
        #程序名
        set app_name "INT_CHECK_02004_02008_DAY.tcl"

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp 
	                    and rule_code in ('R003','R010')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_1
select user_id,product_no,usertype_id,sim_code from
 (
 select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
 where time_id<=$timestamp
 ) k
 where k.row_id=1 with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_2
   select user_id,usertype_id from
   (
   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
   where time_id<=$timestamp
   ) k
   where k.row_id=1 with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
	
	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_3
select a.*,b.user_id user_no,b.usertype_id usertype_no from 
                CHECK_0200402008_DAY_1 a,
                CHECK_0200402008_DAY_2 b
               where a.user_id = b.user_id and a.usertype_id <> '3' 
                 and b.usertype_id not in ('2010','2020','2030','9000')
with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	


  set handle [aidb_open $conn]
	set sql_buff "select count(*) from
(
select product_no,count(*) cnt from CHECK_0200402008_DAY_3
group by product_no
			   ) k where k.cnt>=2
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R003',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " 02004和02008接口用户一致校验R003校验不通过,需9点之前解决"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
### 由于 201107 校验不通过，发现原校验不准确 。在这修正之。
# 20110819
set sql_buff "
SELECT CNT1 - CNT2 
FROM 
	(
	select COUNT(0) CNT1
	from
	( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
		   from bass1.g_a_02008_day
		   where  time_id/100 <= $curr_month
		   ) f 
	where    f.row_id=1 
	    ) A        
	, (
	select COUNT(0) CNT2
	from
	(select user_id,create_date,product_no,brand_id,sim_code,usertype_id
			,row_number() over(partition by user_id order by time_id desc ) row_id   
	from bass1.g_a_02004_day
	where time_id/100 <= $curr_month ) e
	where e.row_id=1
	) B 
"

set RESULT_VAL [get_single $sql_buff]

	if {[format %.3f [expr ${RESULT_VAL} ]]!=0 } {
		set grade 2
	        set alarmcontent " 02004和02008接口用户一致校验R003校验不通过,需9点之前解决"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


		 
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE test1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
# 
# 
#	set handle [aidb_open $conn]
#	set sql_buff "insert into test1
#select user_id,product_no,usertype_id,sim_code from
# (
# select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
# where time_id<=$timestamp
# ) k
# where k.row_id=1 with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle 
# 
#
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE test2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle 
#
#	set handle [aidb_open $conn]
#	set sql_buff "insert into test2
#   select user_id,usertype_id from
#   (
#   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
#   where time_id<=$timestamp
#   ) k
#   where k.row_id=1 with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
##	#--R011:一个电话号码不能同时对应多个非离网的用户ID
##
##  set handle [aidb_open $conn]
##	set sql_buff "
##	 select value(cnt,0) from
##	 (
##	 select count(a.product_no) cnt from 
##                test1 a,
##                test2 b
##               where a.user_id = b.user_id and a.usertype_id <> '3' and a.sim_code <> '1' and 
##                     b.usertype_id not in ('2010','2020','2030','9000')
##               group by a.product_no
##               having count(a.user_id) > 1 
##   ) k            
##               with ur"
##               
#	#--R011:一个电话号码不能同时对应多个非离网的用户ID
#
#  set handle [aidb_open $conn]
#	set sql_buff "select count(a.product_no) from 
#                (select a.user_id,a.product_no,usertype_id,sim_code from
#                 (select time_id,user_id,product_no,usertype_id,sim_code from G_A_02004_DAY where time_id <= $timestamp ) a,
#                 (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$timestamp 
#                   group by user_id
#                  ) b
#                  where a.time_id=b.time_id and a.user_id=b.user_id 
#                 )a,
#                 
#                (select a.user_id,usertype_id from
#                 (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <= $timestamp ) a,
#                 (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$timestamp 
#                     group by user_id)b
#                  where a.time_id=b.time_id and a.user_id=b.user_id
#                 ) b
#               where a.user_id = b.user_id and a.usertype_id <> '3' and a.sim_code <> '1' and 
#                     b.usertype_id not in ('2010','2020','2030','9000')
#               group by a.product_no
#               having count(a.user_id) > 1 
#               with ur"
#
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
#
#	puts $DEC_RESULT_VAL1
#
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R01109',$DEC_RESULT_VAL1,0,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#
#	#--判断
#	#--异常
#	#--1：一个电话号码不能同时对应多个非离网的用户ID
#
#
#	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
#		set grade 2
#	        set alarmcontent " R01109校验不通过"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#		 }
		 		 

	#--R010:未经集团公司有效许可用户状态不能随意传9000

  set handle [aidb_open $conn]
	set sql_buff "select count(*) from bass1.G_A_02008_DAY where usertype_id='9000' and time_id=$timestamp  with ur"

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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R010',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：未经集团公司有效许可用户状态不能随意传9000


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R010校验不通过,用户状态不能随意上传9000的"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



  #进行02008主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,count(*) cnt from bass1.g_a_02008_day
	              where time_id =$timestamp
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
	aidb_close $handle
	
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02008接口主键唯一性校验未通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }




  #进行02004主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,count(*) cnt from bass1.g_a_02004_day
	              where time_id =$timestamp
	             group by user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02004接口主键唯一性校验未通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }








		
	return 0
}
