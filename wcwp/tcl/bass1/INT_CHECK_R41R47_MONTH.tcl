######################################################################################################
#程序名称：	INT_CHECK_R41R47_MONTH.tcl
#校验接口：	03004
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
#        set optime_month 2011-08
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        global app_name
        set app_name "INT_CHECK_R41R47_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in 
					(
					 'R041'
					,'R042'
					,'R043'
					,'R044'
					,'R045'
					,'R046'
					,'R047'
					)
 	  "        

	  exec_sql $sqlbuf





#校园基本信息表中的校园标识都应该在校园本网学生用户表中存在	not pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
)
) t 
"

 
  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

 # 校验值超标时告警	
chkzero2 $sql_buff "R041 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R041',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff


#校园信息化覆盖情况表中的校园标识都应该在校园基本信息表中存在	pass	

set sql_buff " 
select count(0) from table(
select distinct SCHOOL_ID from G_I_06003_MONTH where TIME_ID = $op_month
and SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
) 
) t 
"
  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

 # 校验值超标时告警	
chkzero2 $sql_buff "R042 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R042',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff

#校园区域本网用户表中的校园标识都应该在校园基本信息表中存在	pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_02031_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
)
) t 
"
  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "R043 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R043',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff

#校园本网学生用户表中的校园标识都应该在校园基本信息表中存在	pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
)
) t 
"
  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "R044 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R044',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff

#校园竞争对手学生用户表中的校园标识都应该在校园基本信息表中存在	pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_02033_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
)
) t 
"

  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "R045 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R045',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff

#重点校园本网学生用户表中的校园标识都应该在校园基本信息表中存在	pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_02034_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
)
) t 
"

  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "R046 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R046',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff

#重点校园竞争对手学生用户表中的校园标识都应该在校园基本信息表中存在	pass	
set sql_buff "
select count(0) from table(
select distinct SCHOOL_ID from G_I_02035_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
)
) t 
"

  set RESULT_VAL 0
 	set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "R047 not pass!"

set sql_buff "
	INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R047',$RESULT_VAL,0,0,0) 
"
exec_sql $sql_buff


##(通报规则) 学生用户数大于校园基本信息中的学生人数
set sql_buff "
select count(0) from (
select a.stud_cnt
,b.*
from (select * from  g_i_06001_month where time_id = $op_month ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = $op_month
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) t
"

  set RESULT_VAL 0
  set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "学生用户数大于校园基本信息中的学生人数"

#	06001	校园基本信息	每月全量	每月8日前	1

#	update       (select * from  g_i_06001_month where time_id = 201110 )
#	set      stud_cnt = '7000'
#	where      SCHOOL_ID = '89189400000001'
#	

##(通报规则) 校园基本信息表未找到校园区域本网用户



set sql_buff "
select count(0) from (

select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = $op_month
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_02031_MONTH where TIME_ID = $op_month
)
) t
"

  set RESULT_VAL 0
  set RESULT_VAL [get_single $sql_buff]

chkzero2 $sql_buff "校园基本信息表未找到校园区域本网用户"




	return 0
}
