#################################################################
#程序名称: INT_CHECK_DATARULE_DAY.tcl
#功能描述: 
#规则编号：DR01,DR02,DR03,DR04,DR05,DR11,DR12,DR13,DR14,DR15,DR16,DR17,DR18,DR19,DR21,DR22,DR31,DR32
#规则属性：数据规则以及取值范围校验
#规则类型：日

#指标摘要：
#          DR01	用户业务类型取值                          
#          DR02	客户品牌取值                              
#          DR03	用户付费类型取值                          
#          DR04	数据SIM卡标志取值                         
#          DR05	未经集团公司有效许可用户状态不能随意传9000
#          DR11	梦网彩信话单中SP企业代码                  
#          DR12	梦网短信话单中SP企业代码                  
#          DR13	梦网短信话单的计费类型                    
#          DR14	梦网彩信话单中按条计费话单的信息费        
#          DR15	梦网彩信话单中包月计费话单的信息费        
#          DR16	梦网短信话单中包月话单的费用              
#          DR17	梦网短信话单中按条计费话单的费用          
#          DR18	梦网WAP话单中按条计费话单的信息费         
#          DR19	梦网WAP话单中包月话单的包月费             
#          DR21	通用下载话单中的SP企业代码                
#          DR22	通用下载话单中的计费话单信息费            
#          DR31	语音杂志话单中SP服务代码                  
#          DR32	语音杂志话单中按条计费话单的费用   
#                
#add by zhanght 2009.05.18        

#规则描述：
#          DR01	用户业务类型∈(1,2)
#          DR02	在网用户的客户品牌∈(1,2,3)
#          DR03	在网用户的用户付费类型∈(1,2)
#          DR04	在网用户的数据SIM卡标志∈（0,1）
#          DR05	未经集团公司有效许可用户状态不能随意传9000
#          DR11	梦网彩信话单中SP企业代码为空的比例小于3％
#          DR12	梦网短信话单中SP企业代码为空的比例小于3％
#          DR13	梦网短信话单的计费类型不为空
#          DR14	梦网彩信话单中按条计费话单的信息费≥0
#          DR15	梦网彩信话单中包月计费话单的信息费≥0
#          DR16	梦网短信话单中包月计费话单的信息费=0，且包月费≥0
#          DR17	梦网短信话单中按条计费话单的信息费≥0，且包月费=0
#          DR18	梦网WAP话单中，非百分百折扣的按条计费话单的信息费≥0
#          DR19	梦网WAP话单中，非百分百折扣的包月话单的包月费≥0
#          DR21	通用下载话单中的SP企业代码都以'7'开头
#          DR22	通用下载话单中各种计费话单信息费≥0
#          DR31	语音杂志SP服务代码要以‘12590','12559'或'12596'开头
#          DR32	语音杂志话单中按条计费话单的信息费≥0，且包月费=0

#校验对象：
#        02004 用户
#        02008 用户状态
#        02008 用户状态
#        04004 彩信话单
#        04005 梦网短信话单
#        04006 梦网WAP话单
#        04007 通用下载话单
#        04014 语音杂志话单
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2008-05-16
#问题记录：1.
#修改历史: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        #set op_time 2008-06-30
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名称
        set app_name "INT_CHECK_DATARULE_DAY.tcl"

        #--删除本期数据
        set handle [aidb_open $conn]
#####	set sql_buff "\
#####		delete from bass1.g_rule_check where time_id=$timestamp
#####		and rule_code in('DR01','DR02','DR03','DR04','DR05','DR11','DR12','DR13','DR14','DR15','DR16','DR17','DR18','DR19','DR21','DR22','DR31','DR32') "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####
#####
########################################DR01	用户业务类型取值#############################
#####        #--DR01	用户业务类型取值
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and USER_BUS_TYP_ID not in ('01','02')"
#####
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "用户业务类型非∈(1,2)的数量 " 
#####	
#####	   #--将校验值插入校验结果表
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR01',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--判断
#####        #--异常
#####        #--1：用户业务类型∈(1,2)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "扣分项指标DR01	用户业务类型取值超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR01 用户业务类型取值校验结束"
#####         
#####         
#####
#####
########################################DR02	客户品牌取值 #############################
#####        #--DR02	客户品牌取值 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and brand_id not in ('1','2','3') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "客户品牌取值非∈(1,2,3)的数量 " 
#####	
#####	   #--将校验值插入校验结果表
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR02',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--判断
#####        #--异常
#####        #--1：客户品牌取值∈(1,2,3)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "扣分项指标DR02	客户品牌取值超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR02 客户品牌取值校验结束"
#####
#####
########################################DR03	用户付费类型取值 #############################
#####        #--DR03	用户付费类型取值 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and prompt_type not in ('1','2') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "用户付费类型取值非∈(1,2)的数量 " 
#####	
#####	   #--将校验值插入校验结果表
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR03',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--判断
#####        #--异常
#####        #--1：客户品牌取值∈(1,2,3)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "扣分项指标DR03	用户付费类型取值超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR03	用户付费类型取值校验结束"
#####
#####
#####
########################################DR04	数据SIM卡标志取值 #############################
#####        #--DR04	数据SIM卡标志取值 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and prompt_type not in ('1','2') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "数据SIM卡标志取值非∈(1,2)的数量 " 
#####	
#####	   #--将校验值插入校验结果表
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR04',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--判断
#####        #--异常
#####        #--1：数据SIM卡标志取值∈(1,2)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "扣分项指标DR04	数据SIM卡标志取值超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR04	数据SIM卡标志取值校验结束"
#####
#####
#####
#####
########################################DR05	未经集团公司有效许可用户状态不能随意传9000 #############################
#####        #--DR05	未经集团公司有效许可用户状态不能随意传9000
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02008_DAY where time_id = $timestamp and usertype_id = '9000' "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "未经集团公司有效许可用户状态不能随意传9000非∈(9000)的数量 " 
#####	
#####	   #--将校验值插入校验结果表
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR05',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--判断
#####        #--异常
#####        #--1：客户品牌取值∈(9000)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "扣分项指标DR05	未经集团公司有效许可用户状态不能随意传9000超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR05	未经集团公司有效许可用户状态不能随意传9000"
#####
#####   aidb_close $handle
#####
#####
#####
#####
########################################DR11	梦网彩信话单中SP企业代码 #############################
#####        #--DR11	梦网彩信话单中SP企业代码  梦网彩信话单中SP企业代码为空的比例小于3％
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id = $timestamp and sp_ent_code is null with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id = $timestamp  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####        
#####   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
#####        
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR11',$RESULT_VAL1,$RESULT_VAL2,$DEC_TARGET_VAL1,0.03) "        
#####   exec_sql $sqlbuf
#####
#####	if {$DEC_TARGET_VAL1>=0.03 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR11	梦网彩信话单中SP企业代码超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} elseif {${DEC_TARGET_VAL1}>=0.025 } {
#####		set grade 3
#####	        set alarmcontent "扣分项指标DR11	梦网彩信话单中SP企业代码接近集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####		}
#####	puts "DR11	梦网彩信话单中SP企业代码校验结束"
#####
#####
#####
########################################DR12	梦网短信话单中SP企业代码 #############################
#####        #--DR12	梦网短信话单中SP企业代码  梦网彩信话单中SP企业代码为空的比例小于3％
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp and sp_code is null with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####        
#####   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
#####        
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR12',$RESULT_VAL1,$RESULT_VAL2,$DEC_TARGET_VAL1,0.03) "        
#####   exec_sql $sqlbuf
#####
#####	if {$DEC_TARGET_VAL1>=0.03 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR12	梦网短信话单中SP企业代码超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} elseif {${DEC_TARGET_VAL1}>=0.025 } {
#####		set grade 3
#####	        set alarmcontent "扣分项指标DR12	梦网短信话单中SP企业代码接近集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####		}
#####	puts "DR12	梦网短信话单中SP企业代码校验结束"
#####
#####
########################################DR13	梦网短信话单的计费类型不为空 #############################
#####   #--DR13	梦网短信话单的计费类型不为空
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp and (PayType_id is null or rtrim(PayType_id) = '') with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR13',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR13	梦网短信话单的计费类型不为空超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR13	梦网短信话单的计费类型不为空校验结束"
#####
#####
########################################DR14	梦网彩信话单中按条计费话单的信息费≥0 #############################
#####   #--DR14	梦网彩信话单中按条计费话单的信息费≥0
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id =  $timestamp and billing_type = '2' and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR14',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR14	梦网彩信话单中按条计费话单的信息费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR14	梦网彩信话单中按条计费话单的信息费校验结束"
#####
#####
#####
########################################DR15	梦网彩信话单中包月计费话单的信息费≥0 #############################
#####   #--DR15	梦网彩信话单中包月计费话单的信息费≥0
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id =  $timestamp and billing_type = '3'  and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR15',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR15	梦网彩信话单中包月计费话单的信息费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR15	梦网彩信话单中包月计费话单的信息费≥0校验结束"
#####
#####
#####
########################################DR16	梦网短信话单中包月计费话单的信息费=0，且包月费≥0#############################
#####   #--DR16	梦网短信话单中包月计费话单的信息费=0，且包月费≥0
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '03' and bigint(info_fee) <> 0  with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '03' and bigint(month_fee) < 0  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR16',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####  puts $RESULT_VAL1
#####  puts $RESULT_VAL2
#####	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR16	梦网短信话单中包月计费话单的信息费=0，且包月费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR16	梦网短信话单中包月计费话单的信息费=0，且包月费≥0校验结束"
#####	
#####	
########################################DR17	梦网短信话单中按条计费话单的信息费≥0，且包月费=0#############################
#####   #--DR17	梦网短信话单中按条计费话单的信息费≥0，且包月费=0
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '02'  and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '02' and bigint(month_fee) <> 0 with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR17',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR17	梦网短信话单中按条计费话单的信息费≥0，且包月费=0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR17	梦网短信话单中按条计费话单的信息费≥0，且包月费=0校验结束"
#####	
#####	
########################################DR18	梦网WAP话单中，非百分百折扣的按条计费话单的信息费≥0#############################
#####   #--DR18	梦网WAP话单中，非百分百折扣的按条计费话单的信息费≥0
#####   set sqlbuf " select count(*)  from G_S_04006_day where time_id =  $timestamp and disc_rate <> '100' and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR18',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0} {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR18	梦网WAP话单中，非百分百折扣的按条计费话单的信息费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR18	梦网WAP话单中，非百分百折扣的按条计费话单的信息费≥0校验结束"
#####	
#####	
#####	
########################################DR19	梦网WAP话单中，非百分百折扣的包月话单的包月费≥0#############################
#####   #--DR19	梦网WAP话单中，非百分百折扣的包月话单的包月费≥0
#####   set sqlbuf " select count(*)  from G_S_04006_day where time_id =  $timestamp and disc_rate <> '100' and bigint(month_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR19',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR19	梦网WAP话单中，非百分百折扣的包月话单的包月费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR19	梦网WAP话单中，非百分百折扣的包月话单的包月费≥0校验结束"
#####	
#####
#####
#####
#####
#####
########################################DR21	通用下载话单中的SP企业代码都以'7'开头#############################
#####   #--DR21	通用下载话单中的SP企业代码都以'7'开头
#####   set sqlbuf " select count(*)  from G_S_04007_day where time_id =  $timestamp and substr(sp_code,1,1) <> '7' with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR21',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR21	通用下载话单中的SP企业代码都以'7'开头超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR21	通用下载话单中的SP企业代码都以'7'开头校验结束"
#####
#####
#####
########################################DR22	通用下载话单中各种计费话单信息费≥0#############################
#####   #--DR22	通用下载话单中各种计费话单信息费≥0
#####   set sqlbuf " select count(*)  from G_S_04007_day where time_id =  $timestamp and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR22',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR22	通用下载话单中各种计费话单信息费≥0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR22	通用下载话单中各种计费话单信息费≥0校验结束"
#####
#####
#####
#####
########################################DR31	语音杂志SP服务代码要以‘12590','12559'或'12596'开头#############################
#####   #--DR31	语音杂志SP服务代码要以‘12590','12559'或'12596'开头
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and substr(serv_code,1,5) not in ('12590','12559','12596') "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR31',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR31	语音杂志SP服务代码要以12590,12559或12596开头超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR31	语音杂志SP服务代码要以‘12590','12559'或'12596'开头校验结束"
#####
#####
#####
#####
#####
########################################DR32	语音杂志话单中按条计费话单的信息费≥0，且包月费=0#############################
#####   #--DR32	语音杂志话单中按条计费话单的信息费≥0，且包月费=0
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and sms_fee_type = '02' and bigint(info_fee) < 0   with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and sms_fee_type = '02' and bigint(month_fee) <> 0  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR32',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "扣分项指标DR32	语音杂志话单中按条计费话单的信息费≥0，且包月费=0超出集团考核范围"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR32	语音杂志话单中按条计费话单的信息费≥0，且包月费=0校验结束"
#####

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
