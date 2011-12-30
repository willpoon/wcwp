#################################################################
#��������: INT_CHECK_59_62H789I01_TO_DAY.tcl
#��������: 
#�����ţ�59,60,61,62,H7,H8,H9,I0,I1
#�������ԣ�������֤
#�������ͣ���
#ָ��ժҪ��59���Ʒ�ʱ����/�ջ��ܹ�ϵ(GSMƽ̨)
#          60��ͨ��ʱ����/�ջ��ܹ�ϵ(GSMƽ̨)
#          61��ͨ��������/�ջ��ܹ�ϵ(GSMƽ̨)
#          62: ��ͨ����ͨ������/�ջ��ܹ�ϵ
#          H7: �Ʒ�ʱ����/�ջ��ܹ�ϵ(������ƽ̨)
#          H8��ͨ��ʱ����/�ջ��ܹ�ϵ(������ƽ̨)
#          H9��ͨ��������/�ջ��ܹ�ϵ(������ƽ̨)
#          I0: �ܷ�����/�ջ��ܹ�ϵ(������ƽ̨)
#          I1: �ܷ�����/�ջ��ܹ�ϵ(GSMƽ̨)
#����������59��|��(����ÿ��GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ� - 1|��1��
#          60��|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1��
#          61��|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ� - 1|��1��
#          62: |��(����ÿ����ͨ����ҵ����ʹ�õ�ͨ�����ϼ�)/������ͨ����ҵ����ʹ�õ�ͨ�����ϼ� - 1|��1��
#          H7: |��(����ÿ������������ҵ����ʹ�õļƷ�ʱ���ϼ�)/��������������ҵ����ʹ�õļƷ�ʱ���ϼ� - 1|��1��
#          H8��|��(����ÿ������������ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/��������������ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1��
#          H9��|��(����ÿ������������ҵ����ʹ�õ�ͨ�������ϼ�)/��������������ҵ����ʹ�õ�ͨ�������ϼ� - 1|��1��
#          I0: |��(����ÿ������������ҵ����ʹ�õ��ܷ��úϼ�)/��������������ҵ����ʹ�õ��ܷ��úϼ� - 1|��1��
#          I1: |��(����ÿ��GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ�)/����GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ� - 1|��1��
#У�����1.BASS1.G_S_21001_DAY       GSM��ͨ����ҵ����ʹ��
#          2.BASS1.G_S_21003_TO_DAY    GSM��ͨ����ҵ����ʹ��(��ʹ��)
#          3.BASS1.G_S_21007_DAY       ��ͨ����ҵ����ʹ��              
#          4.BASS1.G_S_21008_TO_DAY    ��ͨ����ҵ����ʹ��(��ʹ��)   
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��Ϊ21004��21006�ӿ��Ѿ��Ϳգ�����H7��H8��H9��I0ָ��У�������ˡ�
#�޸���ʷ: 1.20100324 liuqf R092/R093/R094 ����Ϊ��У�飬��ȡ����У��
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
###        set db_user $env(DB_USER)
###        #���ϸ��� ��ʽ yyyymm
###        set last_month [GetLastMonth [string range $Timestamp 0 5]]
###        #puts $last_month
###        #----���������һ��---#,��ʽ yyyymmdd
###        set last_day_month [GetLastDay [string range $Timestamp 0 5]01]
###        #puts $last_day_month
###        ##--�����죬��ʽyyyymmdd--##
###        set yesterday [GetLastDay ${Timestamp}]
###           
###        set app_name "INT_CHECK_59_62H789I01_TO_DAY.tcl"
###        
###        set handle [aidb_open $conn]
###	set sql_buff "\
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=${Timestamp} 
###		 AND RULE_CODE IN ('59','60','61','62','H7','H8','H9','I0','I1') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--59���Ʒ�ʱ����/�ջ��ܹ�ϵ(GSMƽ̨)
###        #--|��(����ÿ��GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�)
###    
###         
###        
###        set handle [aidb_open $conn]     
###	set sql_buff "\
###	        select 
###            	  sum(bigint(base_bill_duration))
###                from 
###                  bass1.g_s_21001_day
###                where 
###                  time_id/100=${op_month}"
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--����GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�
###	set handle [aidb_open $conn]     
###	set sql_buff "\
###	            select 
###                      sum(bigint(base_bill_duration))
###                    from 
###                      bass1.g_s_21003_to_day
###                    where 
###                      time_id/100=${op_month}"
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--����У����ֵ
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#�����澯ֵ
###	set DEC_TARGET_VAL2 0.005
###			
###	#--��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'59',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--�ж�
###	#--�쳣
###	#--1��|��(����ÿ��GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�)/
###        #--����GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ� - 1|��1������
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��59�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��59�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        	
###	}
###	puts "59����"
###	
###      #--------------------------------------#
###      #--60��ͨ��ʱ����/�ջ��ܹ�ϵ��GSMƽ̨��
###      #--��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�
###      
###        set handle [aidb_open $conn]    
###	set sql_buff "\
###	        select 
###            	  sum(bigint(call_duration))
###                from 
###                  bass1.g_s_21001_day
###                where 
###                  time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#----����GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�
###	set handle [aidb_open $conn]      
###	set sql_buff "\
###	           select 
###                     sum(bigint(call_duration))
###                   from 
###                     bass1.g_s_21003_to_day
###                   where 
###                     time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--����У����ֵ
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#�����澯ֵ
###	set DEC_TARGET_VAL2 0.005
###			
###	#--��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'60',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--�ж�
###	#--�쳣
###	#--|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1������
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��60�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��60�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        }
###	puts "60����"
###	
###	#--------------------------#
###	#--61��ͨ��������/�ջ��ܹ�ϵ��GSMƽ̨��
###   #--��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ�)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(CALL_COUNTS))
###            from bass1.g_s_21001_day
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#------����GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ�
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(CALL_COUNTS))
###                        from bass1.g_s_21003_to_day
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--����У����ֵ
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#�����澯ֵ
###	set DEC_TARGET_VAL2 0.005
###			
###	#--��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'61',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--�ж�
###	#--�쳣
###	#--|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ� - 1|��1������
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��61�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###		} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "׼ȷ��ָ��61�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        }
###	puts "61����"
###	
###	#------------------------#
###	#--62����ͨ����ͨ������/�ջ��ܹ�ϵ
###        #--��(����ÿ����ͨ����ҵ����ʹ�õ�ͨ�����ϼ�)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(sms_counts))
###            from bass1.G_S_21007_DAY
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###	
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###   #------������ͨ����ҵ����ʹ�õ�ͨ�����ϼ�
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(sms_counts))
###                        from bass1.G_S_21008_TO_DAY
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--����У����ֵ
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#�����澯ֵ
###	set DEC_TARGET_VAL2 0.005
###			
###	#--��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'62',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	
###	#--�ж�
###	#--�쳣
###	#--|��(����ÿ����ͨ����ҵ����ʹ�õ�ͨ�����ϼ�)/������ͨ����ҵ����ʹ�õ�ͨ�����ϼ� - 1|��1������
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��62�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###		 
###		} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "׼ȷ��ָ��62�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	        	
###	}
###	puts "62����"
###	
####	 #--H7���Ʒ�ʱ����/�ջ��ܹ�ϵ��������ƽ̨��
####         #--��(����ÿ������������ҵ����ʹ�õļƷ�ʱ���ϼ�)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(call_counts)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------��������������ҵ����ʹ�õļƷ�ʱ���ϼ�
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(base_bill_duration)),9)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--����У����ֵ
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#�����澯ֵ
####	set DEC_TARGET_VAL1 0.005
####			
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--�ж�
####	#--�쳣
####	#--|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ�������ϼ� - 1|��1������
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "׼ȷ��ָ��H7�������ſ��˷�Χ"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "׼ȷ��ָ��H7�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        }
####	puts "H7����"
####	
####	#----------------------------#
####	#--H8��ͨ��ʱ����/�ջ��ܹ�ϵ��������ƽ̨��
####   #--��(����ÿ������������ҵ����ʹ�õ�ͨ��ʱ���ϼ�)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(call_duration)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------��������������ҵ����ʹ�õ�ͨ��ʱ���ϼ�
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(call_duration)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--����У����ֵ
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#�����澯ֵ
####	set DEC_TARGET_VAL1 0.005
####			
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--�ж�
####	#--�쳣
####	#--|��(����ÿ������������ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/��������������ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1������
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "׼ȷ��ָ��H8�������ſ��˷�Χ"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "׼ȷ��ָ��H8�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        		
####	}
####	puts "H8����"
####	
####	#-----------------------##--H9��ͨ��������/�ջ��ܹ�ϵ��������ƽ̨��
####        #--��(����ÿ������������ҵ����ʹ�õ�ͨ�������ϼ�)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(CALL_COUNTS)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------��������������ҵ����ʹ�õ�ͨ�������ϼ�
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(CALL_COUNTS)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--����У����ֵ
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#�����澯ֵ
####	set DEC_TARGET_VAL1 0.005
####			
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--�ж�
####	#--�쳣
####	#--|��(����ÿ������������ҵ����ʹ�õ�ͨ�������ϼ�)/��������������ҵ����ʹ�õ�ͨ�������ϼ� - 1|��1������
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "׼ȷ��ָ��H9�������ſ��˷�Χ"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "׼ȷ��ָ��H9�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        
####	}
####	puts "H9����"
####	
####	#--------------#
####	 #--I0���ܷ�����/�ջ��ܹ�ϵ��������ƽ̨��
####   #--��(����ÿ������������ҵ����ʹ�õ��ܷ��úϼ�)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(favoured_call_fee)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------��������������ҵ����ʹ�õ��ܷ��úϼ�
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(favoured_call_fee)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--����У����ֵ
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#�����澯ֵ
####	set DEC_TARGET_VAL1 0.005
####			
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'I0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--�ж�
####	#--�쳣
####	#--|��(����ÿ������������ҵ����ʹ�õ��ܷ��úϼ�)/��������������ҵ����ʹ�õ��ܷ��úϼ� - 1|��1������
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "׼ȷ��ָ��I0�������ſ��˷�Χ"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "׼ȷ��ָ��I0�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        	        	}
####	puts "I0����"
###	
###	#--I1���ܷ�����/�ջ��ܹ�ϵ��GSMƽ̨��
###        #--��(����ÿ��GSM����ҵ����ʹ�õ��ܷ��úϼ�)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(favoured_call_fee))
###            from bass1.G_S_21001_DAY
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	#------ --����GSM����ҵ����ʹ�õ��ܷ��úϼ�
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(favoured_call_fee))
###                        from bass1.G_S_21003_TO_DAY
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	#--����У����ֵ
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#�����澯ֵ
###	set DEC_TARGET_VAL2 0.005
###			
###	#--��У��ֵ����У������
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'I1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--�ж�
###	#--�쳣
###	#--|��(����ÿ��GSM����ҵ����ʹ�õ��ܷ��úϼ�)/����GSM����ҵ����ʹ�õ��ܷ��úϼ� - 1|��1������
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "׼ȷ��ָ��I1�������ſ��˷�Χ"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "׼ȷ��ָ��I1�ӽ����ſ��˷�Χ1%,�ﵽ${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	       		
###	}
###	puts "I1����"
###	
###        
	return 0
}
