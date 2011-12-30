#################################################################
#程序名称: INT_CHECK_DATARULE_MONTH05.tcl
#功能描述: 
#规则编号：R007,R033,R034,R035,R036,R037
#规则属性：数据规则以及取值范围校验
#规则类型：月

#指标摘要：
#          R007	集团用户成员接口的集团客户标识应存在于集团客户接口中                          
#          R033	月	SP企业名称	06012 SP企业代码 	本地或本地全网业务的SP企业名称为空或‘未知’的比例小于5％
#          R034	月	SP企业代码	06012 SP企业代码 	本地或本地全网业务的SP企业代码长度为6位
#          R035	月	梦网短信业务SP企业代码	06012 SP企业代码 	梦网短信本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘4’或‘7’开头
#          R036	月	彩信业务SP企业代码	06012 SP企业代码 	彩信本地或本地全网业务的SP企业代码以‘8’开头，部分省的财信通和商信通彩信业务以‘4’开头
#          R037	月	梦网WAP业务SP企业代码	06012 SP企业代码 	梦网WAP本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘8’开头

#规则描述：
#          R007	集团用户成员接口的集团客户标识应存在于集团客户接口中                          
#          R033	SP企业名称	06012 SP企业代码 	本地或本地全网业务的SP企业名称为空或‘未知’的比例小于5％
#          R034	SP企业代码	06012 SP企业代码 	本地或本地全网业务的SP企业代码长度为6位
#          R035	梦网短信业务SP企业代码	06012 SP企业代码 	梦网短信本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘4’或‘7’开头
#          R036	彩信业务SP企业代码	06012 SP企业代码 	彩信本地或本地全网业务的SP企业代码以‘8’开头，部分省的财信通和商信通彩信业务以‘4’开头
#          R037	梦网WAP业务SP企业代码	06012 SP企业代码 	梦网WAP本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘8’开头

#校验对象：
#        01004	集团客户
#        02049	集团用户成员
#        06012	SP企业代码

#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2008-05-16
#问题记录：1.
#修改历史: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #当天 yyyymmdd
        #set op_time 2008-06-30
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time

        #程序名称
        set app_name "INT_CHECK_DATARULE_DAY.tcl"

        #--删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
		and rule_code in('R007','R033','R034','R035','R036','R037') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


#####################################R007	集团用户成员接口的集团客户标识应存在于集团客户接口中#############################
##        #--DR01	用户业务类型取值
##  set sqlbuf " select count(*) from G_I_02049_MONTH where enterprise_id not in (select enterprise_id from G_A_01004_DAY where time_id/100 <= $op_month );"
##   set RESULT_VAL1 [get_single $sqlbuf]
##  
##   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R007',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
##   exec_sql $sqlbuf
##
##	if {$RESULT_VAL1 > 0} {
##		set grade 2
##	        set alarmcontent "扣分项指标R007	集团用户成员接口的集团客户标识应存在于集团客户接口中"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##	} 
##	puts "R007	集团用户成员接口的集团客户标识应存在于集团客户接口中超出集团考核范围"



###################################R033	本地或本地全网业务的SP企业名称为空或‘未知’的比例小于5％#############################
  set sqlbuf " select count(*) from G_I_06012_MONTH where time_id = $op_month and (sp_name like '%未知%' or sp_name is null or rtrim(sp_name) = '') ;"
   set RESULT_VAL1 [get_single $sqlbuf]
  set sqlbuf " select count(*) from G_I_06012_MONTH where time_id = $op_month ;"
   set RESULT_VAL2 [get_single $sqlbuf]
  
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R007',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2}*100)]]

	if {$DEC_TARGET_VAL1 > 5} {
		set grade 2
	        set alarmcontent "扣分项指标R033	本地或本地全网业务的SP企业名称为空或‘未知’的比例小于5％"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R033	本地或本地全网业务的SP企业名称为空或‘未知’的比例小于5％超出集团考核范围"


###################################R034	SP企业代码	06012 SP企业代码 	本地或本地全网业务的SP企业代码长度为6位#############################
        #--DR01	用户业务类型取值
  set sqlbuf "select  count(*) from G_I_06012_MONTH where time_id = $op_month and length(rtrim(sp_code)) <> 6;"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "扣分项指标R034	SP企业代码	06012 SP企业代码 本地或本地全网业务的SP企业代码长度为6位"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R034	SP企业代码	06012 SP企业代码 	本地或本地全网业务的SP企业代码长度为6位超出集团考核范围"
         



###################################R035	梦网短信业务SP企业代码	06012 SP企业代码 梦网短信本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘4’或‘7’开头
        #--DR01	用户业务类型取值
  set sqlbuf "select  count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '01'  and substr(sp_code,1,1) not in ('9','4','7');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "扣分项指标R035	梦网短信业务SP企业代码	06012 SP企业代码 梦网短信本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘4’或‘7’开头超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R035	梦网短信业务SP企业代码	06012 SP企业代码 梦网短信本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘4’或‘7’开头"



###################################R036	彩信业务SP企业代码	06012 SP企业代码 彩信本地或本地全网业务的SP企业代码以‘8’开头，部分省的财信通和商信通彩信业务以‘4’开头
        #--DR01	用户业务类型取值
  set sqlbuf "select count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '02'  and substr(sp_code,1,1) not in ('8','4');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "扣分项指标R036	彩信业务SP企业代码	06012 SP企业代码 彩信本地或本地全网业务的SP企业代码以‘8’开头，部分省的财信通和商信通彩信业务以‘4’开头超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R036	彩信业务SP企业代码	06012 SP企业代码 	彩信本地或本地全网业务的SP企业代码以‘8’开头，部分省的财信通和商信通彩信业务以‘4’开头"



###################################R037	梦网WAP业务SP企业代码	06012 SP企业代码 梦网WAP本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘8’开头
        #--DR01	用户业务类型取值
  set sqlbuf "select count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '03'  and substr(sp_code,1,1) not in ('9','8');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "扣分项指标R037	梦网WAP业务SP企业代码	06012 SP企业代码 梦网WAP本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘8’开头超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R037	梦网WAP业务SP企业代码	06012 SP企业代码 梦网WAP本地或本地全网业务的SP企业代码以‘9’开头，部分省以‘8’开头"


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
#   	set sql_buff "INSERT INTO bass1.G_REPORT_CHECK(TIME_ID,RULE_ID,FLAG,RET_VAL) VALUES
#		(                                             
#			$op_month,
#			'B10',
#			1,
#			'$RESULT_VAL')"
#exec_sql $sql_buff
  
 

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
#set RESULT_VAL [get_single $sql_buff]
#puts "10:梦网彩信计费量  $RESULT_VAL"
