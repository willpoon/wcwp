######################################################################################################
#程序名称：INT_CHECK_0205202008_MONTH.tcl
#校验接口：
#运行粒度: 月
#校验规则: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-25
#问题记录：
#修改历史: 20100125 在网用户口径变动 usertype_id not in ('2010','2020','2030','9000') 不排除数据卡 sim_code<>'1'

#修改历史:   20110526 R002 -> R004 根据：附件 一级经营分析系统数据准确性考核规则（2011）.xls

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
        set app_name "INT_CHECK_0205202008_MONTH.tcl"

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #本月最后一天,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #上月最后一天 yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day

 puts " 删除旧数据"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R004') "        
	  exec_sql $sqlbuf   

	#--R004:在网用户的区域标识∈(1,2,3)的比例不小于75％

  set handle [aidb_open $conn]
	set sql_buff "select cast(sum(case when b.user_id is null then 0 else 1 end) as decimal(15,2))/cast(count(a.user_id) as decimal (15,2))
                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                  where time_id<=$last_month_day
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where usertype_id<>'3'
                and time_id<=$last_month_day               
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
                where  a.usertype_id not in ('2010','2020','2030','9000')
                with ur"
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

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month ,'R004',$DEC_RESULT_VAL1,0,0,0) "
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
	        set alarmcontent " R004校验不通过:在网用户的区域标识∈(1,2,3)的比例小于75％"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
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