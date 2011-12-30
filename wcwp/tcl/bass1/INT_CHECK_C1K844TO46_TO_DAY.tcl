#################################################################
#��������: INT_CHECK_C1K844TO46_TO_DAY.tcl
#��������: 
#�����ţ�C1,K8,44,45,46
#�������ԣ�������֤
#�������ͣ���/��
#��������: ��
#ָ��ժҪ��C1����Ե���żƷ����䶯����
#          K8����Ե������Ϣ��
#          44��ȫ��ͨ��Ե���ŵ���
#          45�������е�Ե���ŵ���
#          46: ���еش���Ե���ŵ���
#����������C1���������½�ܵ�Ե���żƷ����ձ䶯�ʡ��������Ͻ磬
#              ����������߽��ɵ��ո�ʡ�Ʒ����䶯�ʵ�ͳ�ƾ�ֵ�ͱ�׼�ͬȷ����
#              �����Ͻ�=��ֵ+3*��׼������½�=��ֵ-3*��׼��
#          K8����Ե������Ϣ�ѣ�0Ԫ
#          44��ȫ��ͨ��Ե���ţ�0.05Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.15Ԫ ��RMB��
#          45�������е�Ե���ţ�0.05Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.18Ԫ ��RMB��
#          46: ���еش���Ե���ţ�0.02Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.12Ԫ ��RMB��
#У�����1.BASS1.G_S_21007_DAY     ��ͨ����ҵ����ʹ��
#          2.BASS1.G_S_21008_TO_DAY  ��ͨ����ҵ����ʹ����ʱ��(��ʹ��)
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�tym
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
	#���ϸ��� ��ʽ yyyymm
        set last_month [GetLastMonth [string range $p_timestamp 0 5]]
        #puts $last_month
        #----���������һ��---#,��ʽ yyyymmdd
        set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
        #puts $last_day_month
         ##--�����죬��ʽyyyymmdd--##
        set yesterday [GetLastDay ${p_timestamp}]
        #puts $yesterday
        
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $p_timestamp 6 7]
        #puts $today_dd

        #��������
        set app_name "INT_CHECK_C1K844TO46_TO_DAY.tcl"


#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$p_timestamp
    	              and rule_code in ('C1','K8','44','45','46') "
    	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
#################################C1����Ե���żƷ����䶯����###################
#���յ�Ե���żƷ���
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  value(SUM(BIGINT(SMS_COUNTS)),0) AS JFL
                FROM BASS1.G_S_21007_DAY
                WHERE TIME_ID=$p_timestamp
                    and SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21') "
  puts $sqlbuf 
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

  puts $CHECK_VAL1 
       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
     
#���յ�Ե���żƷ���
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  value(SUM(BIGINT(SMS_COUNTS)),0) AS JFL
                FROM BASS1.G_S_21007_DAY
                WHERE TIME_ID=$yesterday
                   and SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21') "

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
        
        set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
 
#��������
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"
     
     #set RESULT_VAL2 1551234       
     set RESULT [format "%.5f" [expr (${RESULT_VAL1}-${RESULT_VAL2}) / 1.0000/${RESULT_VAL2}]]
     
    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($p_timestamp ,
			'C1',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			cast ($RESULT as  DECIMAL(18, 5) ),
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#�ж�C1�����ݾ��飬�����ݶ�Ϊ�䶯�ʲ��ܹ�6.9
	if { $RESULT_VAL1==0 || ($RESULT>=0.069 || $RESULT<=-0.069) } {	
		set grade 2
	        set alarmcontent "�۷���ָ��C1�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	                }
        puts "C1����"
#################################K8����Ե������Ϣ��###########################
	set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(*)
               FROM BASS1.G_S_21008_TO_DAY
               WHERE TIME_ID/100=$this_month
                   AND SVC_TYPE_ID IN ('11','12','13')
                   AND SMS_INFO_FEE<>'0'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

#
     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]

#��������
     set CHECK_VAL1 "0.00"

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($p_timestamp ,
			'K8',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			0,
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#�ж�K8������ҵ��������(11,12,13)��Ե���ŵ���Ϣ�Ѳ�������ļ�¼������
	if { $RESULT_VAL1>0 } {	
		set grade 2
	        set alarmcontent "�۷���ָ��K8�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		  }
        puts "K8����"
#################################44:ȫ��ͨ��Ե���ŵ���####################
###44:ȫ��ͨ��Ե���ŵ���=ͨ�ŷ�/�Ʒ���
       if { $today_dd>=20 } {
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='1'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##��������
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #��У��ֵ����У������
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'44',
	   		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
	   		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
	   		cast ($RESULT as  DECIMAL(18, 5) ),
	   		0)"
	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	   	WriteTrace $errmsg 003
	   	return -1
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   	   
#	   #�ж�44��ȫ��ͨ��Ե���ţ�0.05Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.15Ԫ ��RMB������
	if { $RESULT<0.050 || $RESULT>0.150 } {	
		set grade 2
	        set alarmcontent "׼ȷ��ָ��44�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	} elseif {$RESULT<=0.060 || $RESULT>=0.140 } {
		set grade 3
	        set alarmcontent "׼ȷ��ָ��44�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		 }
         puts "44����"
#################################45�������е�Ե���ŵ���####################
###45:�����е�Ե���ŵ���=ͨ�ŷ�/�Ʒ��� 
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='2'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##��������
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #��У��ֵ����У������
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'45',
	   		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
	   		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
	   		cast ($RESULT as  DECIMAL(18, 5) ),
	   		0)"
	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	   	WriteTrace $errmsg 003
	   	return -1
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   	   
#	   #�ж�44�������е�Ե���ţ�0.05Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.18Ԫ ��RMB������
	if { $RESULT<0.050 || $RESULT>0.180 } {	
		set grade 2
	        set alarmcontent "׼ȷ��ָ��45�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		} elseif {$RESULT<=0.060 || $RESULT>=0.170 } {
		set grade 3
	        set alarmcontent "׼ȷ��ָ��45�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	        } 
         puts "45����" 
#################################46�����еش���Ե���ŵ���####################
###46:���еش���Ե���ŵ���=ͨ�ŷ�/�Ʒ��� 
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='3'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##��������
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #��У��ֵ����У������
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'46',
	   		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
	   		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
	   		cast ($RESULT as  DECIMAL(18, 5) ),
	   		0)"
	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	   	WriteTrace $errmsg 003
	   	return -1
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   	   
#	   #�ж�44�������е�Ե���ţ�0.02Ԫ �� (ͨ�ŷ� / �Ʒ���) �� 0.12Ԫ ��RMB������
	if { $RESULT<0.020 || $RESULT>0.120 } {
		set grade 2
	        set alarmcontent "׼ȷ��ָ��46�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	} elseif {$RESULT<=0.015 || $RESULT>=0.110 } {
		set grade 3
	        set alarmcontent "׼ȷ��ָ��46�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	        } 
	puts "46����"        
############end of if ##########	 
        }      
##################################END#######################
	return 0
}