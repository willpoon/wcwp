#################################################################
#��������: INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl
#��������:  
#�����ţ�D0-D6,I2-I5,R099-K7
#�������ԣ�ָ���춯
#�������ͣ���
#ָ��ժҪ��R099: ȫ��ͨ����ͨ����ռ��
#          R100: ����������ͨ����ռ��
#          R101: ���еش�����ͨ����ռ��
#����������R099: ���±����±䶯�ʣ�ȫ��ͨ����ͨ����/�ϼ�����ͨ���ѣ��� 10%����15%��ռ�ȡ�40%   --15%��ռ�ȡ�50% 
#          R100: ���±����±䶯�ʣ�����������ͨ����/�ϼ�����ͨ���ѣ��� 8%����35%��ռ�ȡ�65%
#          R101: ���±����±䶯�ʣ����еش�����ͨ����/�ϼ�����ͨ���ѣ��� 10%����3<ռ��<18%
#          
#У�����
#          1.BASS1.G_S_21003_TO_DAY
#          2.BASS1.G_S_21006_TO_DAY
#          3.BASS1.G_S_21009_DAY
#          4.BASS1.G_A_02004_DAY
#          5.BASS1.G_A_02008_DAY
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.add by zhanght on 20090623 �޸�����Ʒ�Ƶ�ռ�Ȳ�����
#�޸���ʷ: 1.
#  ���еش��Ʒ�ʱ��ռ��
#         } elseif {${DEC_RESULT_VAL1} >=4.0 || ${DEC_TARGET_VAL1}>=0.12} {
#         	set grade 3
#	        set alarmcontent "׼ȷ��ָ��D2�ӽ����ſ��˷�Χ"
#    ������ж�Ӧ���ǽӽ����˷�Χ�������ǳ������˷�Χ 20070901 �Ļ�ѧ
#***************************************************/

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

  #set op_time  2008-05-31
	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
	puts $op_month
	set day [format "%.0f" [string range $Timestamp 6 7]]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)
        #���ϸ��� ��ʽ yyyymm
        set last_month [GetLastMonth [string range $Timestamp 0 5]]
        puts $last_month
        #puts $last_month
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $Timestamp 0 5]01]
        #----��������-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        #puts $thismonthdays
        #----��������-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        #puts $thisyeardays
        #puts $last_day_month
        set app_name "INT_CHECK_R099_7I2_5D0_6_TO_DAY.tcl"

##        set handle [aidb_open $conn]
##
##        #�ж�����
##        if { $day <=28 } {
##        	    	puts "���� $day �ţ�δ��28�ţ��ݲ�����"
##        	    	return 0
##        	        }
##
##
##  #--ɾ����������
##  puts $Timestamp
##	set sql_buff "\
##		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$Timestamp
##         AND RULE_CODE IN ('R099','R100','R101'); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##
##
##
##
##	 #--DEC_CHECK_VALUE_11����������Ʒ�ƺϼ�����ͨ����,DEC_CHECK_VALUE_12����������Ʒ�ƺϼ�����ͨ����
##         #--������Ʒ�ƺϼ�����ͨ����
##         set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(T.FY)
##   	         FROM 
##   	         (
##   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	            FROM BASS1.G_S_21003_TO_DAY
##   	            WHERE TIME_ID/100=${op_month}
##   	            UNION
##   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	            FROM BASS1.G_S_21006_TO_DAY
##   	            WHERE TIME_ID/100=${op_month}
##   	         )T"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_11 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##
##	#--R099: ȫ��ͨ����ͨ����ռ��
##        #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='1'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--��������
##       set DEC_CHECK_VALUE_1 "0"
##       #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R099'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	
##	puts "11111111111111111111111111"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##  
##  puts "22222222222222222222222222"
##	puts ${DEC_TARGET_VAL1}
##
##	#--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R099',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##  puts $sqlbuf
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--�ж�
##        #--�쳣��
###R099: ���±����±䶯�ʣ�ȫ��ͨ����ͨ����/�ϼ�����ͨ���ѣ��� 10%����15%��ռ�ȡ�40%
##        
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<15.0  || ${DEC_RESULT_VAL1}>40.0 || ${DEC_TARGET_VAL1}>0.10} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��R099�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	                } elseif {${DEC_RESULT_VAL1}<=19.0  || ${DEC_RESULT_VAL1}>=39.5  || ${DEC_TARGET_VAL1}>0.09} {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��R099�ӽ����ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}        	
###		
###	}
##	
##	
###add by zhanght on 20090623 ȫ��ͨ����ͨ����ռ�ȱ䶯�ʡ�10%	
##		if {${DEC_TARGET_VAL1}>0.10} {
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��R099�������ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	                } elseif {${DEC_RESULT_VAL1}<=19.0  || ${DEC_RESULT_VAL1}>=39.5  || ${DEC_TARGET_VAL1}>0.09} {
##	        set grade 3
##	        set alarmcontent "׼ȷ��ָ��R099�ӽ����ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}        	
##		
##	}
##	
##	puts "R099 finish"
##	#----------------------------------------------------------
##	#--R100: ����������ͨ����ռ��
##        #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='2'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--��������
##       set DEC_CHECK_VALUE_1 "0"
##       #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R100'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##
##	puts ${DEC_TARGET_VAL1}
##
##	#--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R100',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--�ж�
##        #--�쳣��
###R100: ���±����±䶯�ʣ�����������ͨ����/�ϼ�����ͨ���ѣ��� 8%����35%��ռ�ȡ�65%
##        
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<35.0  || ${DEC_RESULT_VAL1}>65.0 || ${DEC_TARGET_VAL1}>0.08 } {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��R100�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        } elseif {${DEC_RESULT_VAL1}<=38.0  || ${DEC_RESULT_VAL1}>=63.0  || ${DEC_TARGET_VAL1}>0.07} {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��R100�ӽ����ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	 	}
##	 	
###add by zhanght on 20090623	 	����������ͨ����ռ�ȱ䶯�ʡ�10%
##	 	if { ${DEC_TARGET_VAL1}>0.10 } {
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��R100�������ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	        } elseif {${DEC_RESULT_VAL1}<=38.0  || ${DEC_RESULT_VAL1}>=63.0  || ${DEC_TARGET_VAL1}>0.07} {
##	        set grade 3
##	        set alarmcontent "׼ȷ��ָ��R100�ӽ����ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	 	}
##	 	
##	 	
##	 	
##	 	
##	 	
##	 	
##     puts "R100 finish"
##     #------------------------------------------------------------------
##     #--R101: ���еش�����ͨ����ռ��
##   #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='3'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--��������
##       set DEC_CHECK_VALUE_1 "0"
##       #--����ռ�ȳ�100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R101'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##
##	puts ${DEC_TARGET_VAL1}
##
##	#--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R101',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--�ж�
##        #--�쳣��
## #R101: ���±����±䶯�ʣ����еش�����ͨ����/�ϼ�����ͨ���ѣ��� 10%����3<ռ��<18%
##       
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<3.0   || ${DEC_RESULT_VAL1}>18.0 || ${DEC_TARGET_VAL1}>0.10 } {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��R101�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        } elseif {${DEC_RESULT_VAL1}<=2.6|| ${DEC_TARGET_VAL1}>=17.0} {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��R101�ӽ����ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	 	}
##	 	
###add by zhanght on 20090623 ���еش�����ͨ����ռ�ȱ䶯�ʡ�12%	 	
##	if { ${DEC_TARGET_VAL1}>0.12 } {
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��R101�������ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	        } elseif {${DEC_RESULT_VAL1}<=2.6|| ${DEC_TARGET_VAL1}>=17.0} {
##	        set grade 3
##	        set alarmcontent "׼ȷ��ָ��R101�ӽ����ſ��˷�Χ"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	 	}	 	
##	 	
##	 	
##	puts "R101 finish"
##	#--------------------------------------------------------------
##
##
##        aidb_close $handle
	return 0
}
