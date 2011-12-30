#################################################################
#程序名称: INT_CHECK_CUSTSRV_MONTH.tcl
#功能描述: 
#规则编号：X4,X5,X6,X7,X8,X9,Y0,Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Z0,Z1,Z2
#规则属性：平衡关系
#规则类型：月
#指标摘要：X4: 话费类投诉次数
#          X5: 网络类投诉次数
#          X6: 彩铃业务投诉次数
#          X7: 梦网短信投诉次数
#          X8: 音信互动投诉次数
#          X9: 梦网彩信投诉次数
#          Y0: WAP投诉次数
#          Y1: 百宝箱投诉次数
#          Y2: SP客服问题
#          Y3: 彩铃业务话费类投诉次数
#          Y4: 梦网短信话费类投诉次数
#          Y5: 音信互动话费类投诉次数
#          Y6: 梦网彩信话费类投诉次数
#          Y7: WAP业务话费类投诉次数
#          Y8: 百宝箱话费类投诉次数
#          Y9: 彩铃业务网络类投诉次数
#          Z0: 梦网短信网络类投诉次数
#          Z1: 音信互动网络类投诉次数
#          Z2: 梦网彩信网络类投诉次数
#规则描述：X4: 22029接口中"话费类投诉"合计 ≥ 22034接口中"话费类投诉"各分项之和
#          X5: 22029接口中"网络类投诉"合计 ≥ 22034接口中"网络类投诉"各分项之和
#          X6: 22029接口中"彩铃业务"的投诉次数合计＝22028接口中彩铃业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          X7: 22029接口中"梦网短信"的投诉次数合计＝22028接口中梦网短信业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          X8: 22029接口中"音信互动"的投诉次数合计＝22028接口中音信互动业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          X9: 22029接口中"梦网彩信"的投诉次数合计＝22028接口中梦网彩信业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          Y0: 22029接口中"WAP"的投诉次数合计＝22028接口中WAP的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          Y1: 22029接口中"百宝箱"的投诉次数合计＝22028接口中百宝箱的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
#          Y2: 22029接口中"SP客服问题"的投诉次数合计＝22028接口中"SP客服问题"的投诉次数合计
#          Y3: 22034接口中"彩铃业务话费类"投诉次数＝22028接口中"彩铃业务话费类"的投诉次数合计
#          Y4: 22034接口中"梦网短信话费类"投诉次数＝22028接口中"梦网短信话费类"的投诉次数合计
#          Y5: 22034接口中"音信互动话费类"投诉次数＝22028接口中"音信互动话费类"的投诉次数合计
#          Y6: 22034接口中"梦网彩信话费类"投诉次数＝22028接口中"梦网彩信话费类"的投诉次数合计
#          Y7: 22034接口中"WAP话费类"投诉次数＝22028接口中"WAP话费类"的投诉次数合计
#          Y8: 22034接口中"百宝箱话费类"投诉次数＝22028接口中"百宝箱话费类"的投诉次数合计
#          Y9: 22034接口中"彩铃业务网络类"投诉次数＝22028接口中"彩铃业务网络类"的投诉次数合计
#          Z0: 22034接口中"梦网短信网络类"投诉次数＝22028接口中"梦网短信网络类"的投诉次数合计
#          Z1: 22034接口中"音信互动网络类"投诉次数＝22028接口中"音信互动网络类"的投诉次数合计
#          Z2: 22034接口中"音信互动网络类"投诉次数＝22028接口中"音信互动网络类"的投诉次数合计
#校验对象：1.BASS1.G_S_22028_MONTH
#          2.BASS1.G_S_22029_MONTH
#          3.BASS1.G_S_22034_MONTH
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#***************************************************/
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #程序名      
        set app_name "INT_CHECK_CUSTSRV_MONTH.tcl"

####        set handle [aidb_open $conn]
####	set sql_buff "\
####		delete from bass1.g_rule_check where time_id=$op_month
####        and rule_code in ('X4','X5','X6','X7','X8','X9','Y0','Y1','Y2','Y3','Y4','Y5','Y6','Y7','Y8','Y9','Z0','Z1','Z2')"
####
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
###############################################
####       #--X4: 话费类投诉次数
####       #--22029接口中"话费类投诉"合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "\
####               SELECT 
####                 COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####               FROM  
####                 BASS1.G_S_22029_MONTH
####               WHERE 
####                  TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='020000'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22034接口中"话费类投诉"各分项之和
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####	     AND CMPLT_TYPE_ID IN ('020101','020102','020105','020106','020111','020103','020112','020199',
####				'020402','020401','020499','020500','020601','020602','020603','020606','020604',
####				'020605','020607','020608','020609','020610','020611','020612','020613','020614')"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--1：22029接口中"话费类投诉"合计 ≥ 22034接口中"话费类投诉"各分项之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}<${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X4超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####       puts "x4 end"  	
####
####        #--X5: 网络类投诉次数
####        #--22029接口中"网络类投诉"合计 
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='040000';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#------22034接口中"网络类投诉"各分项之和
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####	     AND CMPLT_TYPE_ID IN ('040101','040102','040103','040701','040702','040703','040704','040705',
####				'040901','040902','040903','040904','040999','040601','040602','040603','040699',
####				'041101','041102','041104','041103','041199','042011','042012','042015','042013',
####				'042014','042020','042060','042070','042030','042040','042050','042999','043001',
####				'043002','043003','043101','043102','043103','043199','049900')"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--判断
####       #--异常
####       #--1：22029接口中"网络类投诉"合计 ≥ 22034接口中"网络类投诉"各分项之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}<${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X5超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x5 end"         
####
####        #--X6: 彩铃业务投诉次数
####      #--22029接口中"彩铃业务"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070202';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中彩铃业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='370'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####        #--判断
####        #--异常
####        #--22029接口中"彩铃业务"的投诉次数合计＝22028接口中彩铃业务的"业务使用类.终端使用问题+
####        #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X6超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####        puts "x6 end"     
####
####      #--X7: 梦网短信投诉次数
####      #--22029接口中"梦网短信"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070702';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中梦网短信业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='252'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--判断
####       #--异常
####       #--22029接口中"梦网短信"的投诉次数合计＝22028接口中梦网短信业务的"业务使用类.终端使用问题+
####       #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X7超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x7 end"  
####        
####        #--X8: 音信互动投诉次数
####      #--22029接口中"音信互动"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070706';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中音信互动业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='340'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--1：22029接口中"音信互动"的投诉次数合计＝22028接口中音信互动业务的"业务使用类.终端使用问题+
####    #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X8超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x8 end"  
####         
####        #--X9: 梦网彩信投诉次数
####      #--22029接口中"梦网彩信"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070703';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中梦网彩信业务的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='283'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--1：22029接口中"梦网彩信"的投诉次数合计＝22028接口中梦网彩信业务的"业务使用类.终端使用问题+
####    #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标X9超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####      puts "x9 end"                
####
####       #--Y0: WAP投诉次数
####      #--22029接口中"WAP"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070704';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中WAP的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='310'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--判断
####       #--异常
####       #--1：1：22029接口中"WAP"的投诉次数合计＝22028接口中WAP的"业务使用类.终端使用问题+
####       #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y0超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y0 end" 
####         
####         #----Y1: 百宝箱投诉次数
####         #--22029接口中"百宝箱"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070705';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中百宝箱的"业务使用类.终端使用问题+业务使用类.业务使用类"投诉次数之和
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='410'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--122029接口中"百宝箱"的投诉次数合计＝22028接口中百宝箱的"业务使用类.终端使用问题+
####    #--业务使用类.业务使用类"投诉次数之和超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y1超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y1 end" 
####         
####          #----Y2: SP客服问题
####      #--22029接口中"SP客服问题"的投诉次数合计
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='010800';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"SP客服问题"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='010800'
####				AND BUS_FUNC_ID IN ('310','410','283','370','252','340');"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--判断
####       #--异常
####       #--22029接口中"SP客服问题"的投诉次数合计＝22028接口中"SP客服问题"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y2超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y2 end" 
####         
####         
####         #------Y3: 彩铃业务话费类投诉次数
####      #--22034接口中"彩铃业务话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020402';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"彩铃业务话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='370';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"彩铃业务话费类"投诉次数＝22028接口中"彩铃业务话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y3超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y3 end" 
####         
####         #--Y4: 梦网短信话费类投诉次数
####      #--22034接口中"梦网短信话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020613';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"梦网短信话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='252';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"梦网短信话费类"投诉次数＝22028接口中"梦网短信话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y4超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y4 end" 
####         
####         #--Y5: 音信互动话费类投诉次数
####        # --22034接口中"音信互动话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020611';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"音信互动话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='340';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"音信互动话费类"投诉次数＝22028接口中"音信互动话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y5超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y5 end" 
####         
####         #--Y6: 梦网彩信话费类投诉次数
####      #--22034接口中"梦网彩信话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020611';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"梦网彩信话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='283';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"梦网彩信话费类"投诉次数＝22028接口中"梦网彩信话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y6超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y6 end" 
####         
####          #--Y7: WAP业务话费类投诉次数
####      #--22034接口中"WAP话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020605';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"WAP话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='310';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"WAP话费类"投诉次数＝22028接口中"WAP话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y7超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y7 end" 
####         
####       #--Y8: 百宝箱话费类投诉次数
####       #--22034接口中"百宝箱话费类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020612';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028接口中"百宝箱话费类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='410';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"百宝箱话费类"投诉次数＝22028接口中"百宝箱话费类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y8超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y8 end" 
####         
####           #--Y9: 彩铃业务网络类投诉次数
####      #--22034接口中"彩铃业务网络类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='041104';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中"彩铃业务网络类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='370';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"彩铃业务网络类"投诉次数＝22028接口中"彩铃业务网络类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Y9超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y9 end" 
####
####       #----Z0: 梦网短信网络类投诉次数
####       #--22034接口中"梦网短信网络类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042015';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中"梦网短信网络类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='252';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"梦网短信网络类"投诉次数＝22028接口中"梦网短信网络类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Z0超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####	}
####         puts "Z0 end"          
####
####         #----Z1: 音信互动网络类投诉次数
####      #--22034接口中"音信互动网络类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042070';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中"音信互动网络类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='340';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"音信互动网络类"投诉次数＝22028接口中"音信互动网络类"的投诉次数合计超标
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Z1超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Z1 end" 
####         
####         #----Z2: 梦网彩信网络类投诉次数
####      #--22034接口中"梦网彩信网络类"投诉次数
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042060';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028接口中"梦网彩信网络类"的投诉次数合计
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='283';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--判断
####    #--异常
####    #--22034接口中"梦网彩信网络类"投诉次数＝22028接口中"梦网彩信网络类"的投诉次数合计超标
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "准确性指标Z2超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
         puts "Z2 end"       
####################################
        return 0
}