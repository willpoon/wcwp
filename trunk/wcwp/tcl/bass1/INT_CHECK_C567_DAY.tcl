##############################################################
#��������: INT_CHECK_C567_DAY.tcl
#��������: 
#�����ţ�C5,C6,C7
#�������ԣ�ҵ���߼�(C7������֤)
#�������ͣ���
#ָ��ժҪ��C5���������������û���
#          C6���ҹ�˾�û��г�ռ����
#          C7����������������ͨ�������������
#����������C5���������������û�����0
#          C6���ҹ�˾�û��г�ռ���ʣ�50%
#          C7��|������/������ͨ�������� - 1|  �� 50%
#У�����1.bass1.G_S_22012_DAY    
#          2.bass1.G_S_21001_DAY
#          3.bass1.G_S_21004_DAY
#          4.bass1.G_S_21009_DAY
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�tym
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.22012�ӿڵ�У���Ѿ��ϲ���22073�ӿڣ���Ըýӿڵ�У��ȡ��  �Ļ�ѧ 20090502
#####################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#	global env
#
#	set Optime $op_time
#	set p_optime $op_time
#	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#	set op_month [string range $op_time 0 3][string range $op_time 5 6]
#        append op_time_month ${optime_month}-01
#        set db_user $env(DB_USER)
#        #���ϸ��� ��ʽ yyyymm
#        set last_month [GetLastMonth [string range $Timestamp 0 5]]
#        #puts $last_month
#        #----���������һ��---#,��ʽ yyyymmdd
#        set last_day_month [GetLastDay [string range $Timestamp 0 5]01]
#        #puts $last_day_month
#        ##--�����죬��ʽyyyymmdd--##
#        set yesterday [GetLastDay ${Timestamp}]
#        
#        set app_name "INT_CHECK_C567_DAY.tcl"
#        
#
#        set handle [aidb_open $conn]
#	set sql_buff "\
#		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=${Timestamp} AND RULE_CODE IN ('C5','C6','C7')"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#----C5���������������û���
#	
#	 set handle [aidb_open $conn]      
#	set sql_buff "SELECT 
#                       SUM(BIGINT(D_UNION_NUSERS)+BIGINT(D_TELE_FIX_NUSERS)+BIGINT(D_TELE_NET_NUSERS)+BIGINT(D_OTH_NUSERS))
#                       FROM bass1.G_S_22012_DAY
#                       WHERE TIME_ID=${Timestamp}
#		       "
#        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set DEC_RESULT_VAL2 "0"
#	
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--�ж�
#        #--�쳣��
#        #--C5���������������û�����0����
#        
#        if {${DEC_RESULT_VAL1}<=0} {
#        	set grade 2
#	        set alarmcontent "�۷���ָ��C5�������ſ��˷�Χ"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        
#		}
#		
#	#--C6���ҹ�˾�û��г�ռ����
#        #--�ƶ�����ͨ���û���
#        set handle [aidb_open $conn]        
#	set sql_buff "SELECT 
#                       SUM(BIGINT(D_COMM_USERS))
#                       FROM bass1.G_S_22038_DAY
#                       WHERE TIME_ID=${Timestamp}"
#		        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--�ƶ�����ͨ���û�����������Ӫ���û����ϼ�ֵ
#	set handle [aidb_open $conn]
#	set sql_buff "\
#            SELECT
#	      SUM(T.USERS)
#	    FROM
#	      (
#	          SELECT 
#                    SUM(BIGINT(D_COMM_USERS)) AS  USERS
#                  FROM 
#                    bass1.G_S_22038_DAY
#                  WHERE 
#                    TIME_ID=${Timestamp}
#                  UNION  ALL 
#                  SELECT
#                    BIGINT(D_UNION_USERS)+ BIGINT(D_TELE_FIX_USERS)+
#                    BIGINT(D_TELE_NET_USERS)+BIGINT(D_TELE_OTH_USERS) AS USERS
#                  FROM bass1.G_S_22012_DAY
#                  WHERE TIME_ID=${Timestamp}
#	      )T "        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}       
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--�ж�
#        #--�쳣��
#        #--C6���ҹ�˾�û��г�ռ���ʣ�50%����	
#	set RESULT_VAL2 [format "%.2f" [expr ${DEC_RESULT_VAL1} /1.00/${DEC_RESULT_VAL2}]]
#	if {${RESULT_VAL2}<=0.05} {
#		set grade 2
#	        set alarmcontent "�۷���ָ��C6���ҹ�˾�û��г�ռ���ʳ������ſ��˷�Χ"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        }
#		
#	#--C7����������������ͨ�������������
#        #--��22012��KPI�ӿ���ȡ����������
#        set handle [aidb_open $conn]     
#	set sql_buff "SELECT 
#                        SUM(BIGINT(INCOME))
#                        FROM bass1.G_S_22038_DAY
#                        WHERE TIME_ID=${Timestamp}
#		       "
#        if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle		       	
	



















#��ǰ�޸ĵ�
#	#--������ͨ��������
#	set handle [aidb_open $conn]      
#	set sql_buff "SELECT SUM(T.FEE)
#                      FROM
#                      (
#    	                SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21001_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                        union all
#    	                SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21004_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                        union all
#                        SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21009_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                      ) T "
#		        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--�ж�
#        #--�쳣��
#        #--C7��|������/������ͨ�������� - 1|  �� 30%����	
#	
#	
#	set RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00/${DEC_RESULT_VAL2} - 1)]]
#	if {${RESULT_VAL2}>0.50 ||${RESULT_VAL2}<-0.50} {
#		set grade 2
#	        set alarmcontent "�۷���ָ��C7����������������ͨ������������ʳ������ſ��˷�Χ"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        }

	return 0
}
