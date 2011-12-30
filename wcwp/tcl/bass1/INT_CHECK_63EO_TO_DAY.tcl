#################################################################
# ��������: INT_CHECK_63EO_TO_DAY.tcl
# ��������:
# �������:
#	    op_time		����ʱ�䣨���Σ�,��yyyy-mm-dd��
#	    province_id	        ʡ�ݱ���
#	    redo_number	        �ش���š�ֻ�����յ�һ�����붨�������ļ���Ҫ�ش����
#	    trace_fd	        trace�ļ����
#	    temp_data_dir	��ʱ�����ļ��Ĵ��λ�ã�$BASS1FILEDIR/tempdata/�����ڱ���e_transform���ɵ������ļ�
#	    semi_data_dir	�м������ļ��Ĵ��λ�ã�$BASS1FILEDIR/semidata/�����ڱ���.bass1��.bass2��.sample���ļ�
#	    final_data_dir	���������ļ��Ĵ��λ�ã�$BASS1FILEDIR/finaldata/�����ڱ���.dat����һ�����붨�������ļ�
#	    conn		���ݿ�����
#	    src_data	        ����Դ
#	    obj_data	        ����Ŀ��
#
#�����ţ�63,E0
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��63: �˾���ͨ����ͨ����
#          E0����Ե�����ռ���
#����������63: 45 �� ������ͨ����ͨ����/������ͨ�����û��� �� 200 ����/����
#          E0��40�� < (���µ�Ե����ʹ���û��� / ���µ����û���) < 90%��
#              ���ռ��ʽ����±䶯���� �� 10��
#У�����
#          BASS1.G_A_02004_DAY       �û�
#          BASS1.G_A_02008_DAY       �û�״̬
#          BASS1.G_S_21008_TO_DAY    ��ͨ����ҵ����ʹ��(��ʹ���м��)
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#################################################################
proc Deal { op_time p_optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
###	#���ϸ��� ��ʽ yyyymm
###        set last_month [GetLastMonth [string range $p_timestamp 0 5]]
###        #puts $last_month
###        #----���������һ��---#,��ʽ yyyymmdd
###        set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
###        #puts $last_day_month
###         ##--�����죬��ʽyyyymmdd--##
###        set yesterday [GetLastDay ${p_timestamp}]
###        #puts $yesterday
###        
###        ##��������ڣ���ʽdd(��������20070411 ����11)
###        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
###        #puts $today_dd
###
###        #��������
###        set APP_NAME "INT_CHECK_63EO_TO_DAY.tcl"
###
###
####ɾ����������
###        set handle [aidb_open $conn]
###	set sql_buff "
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
###    	              AND RULE_CODE IN ('63','E0') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
####################################63: �˾���ͨ����ͨ����############
###	#������ͨ����ͨ����
###	if { $today_dd>=25 } {
###	     set handle [aidb_open $conn]
###	     set sqlbuf "
###                    SELECT 
###               	      SUM(BIGINT(SMS_COUNTS)) AS TXL,
###               	      COUNT(DISTINCT PRODUCT_NO)
###                    FROM BASS1.G_S_21008_TO_DAY
###                    WHERE TIME_ID/100=$this_month
###                       AND END_STATUS='0'
###                       AND CDR_TYPE_ID IN('00','01','10','11','21','28')"
###
###	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	      }
###	      while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###		set CHECK_VAL2 [lindex $p_row 1]
###		puts $CHECK_VAL1
###		puts $CHECK_VAL2
###	      }
###	      aidb_commit $conn
###	      aidb_close $handle
###	      
###	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.000/${CHECK_VAL2})]]
###	     puts $RESULT_VAL1
###             
###             
###             #��У��ֵ����У������
###             set handle [aidb_open $conn]
###             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###             		($p_timestamp ,
###             		'63',
###             		cast ($CHECK_VAL1 as  DECIMAL(18, 5) ),
###             		cast ($CHECK_VAL2 as  DECIMAL(18, 5) ),
###             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###             		0)"
###             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###             	WriteTrace $errmsg 003
###             	return -1
###             }
###             aidb_commit $conn
###             aidb_close $handle
###             
###             #�ж�63: 45 �� ������ͨ����ͨ����/������ͨ�����û��� �� 200 ����/��������
###	     if { $RESULT_VAL1<45 || $RESULT_VAL1>200 } {
###	        set grade 2
###	        set alarmcontent "׼ȷ��ָ��63�������ſ��˷�Χ"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}
###	     } elseif { $RESULT_VAL1<=50 || $RESULT_VAL1>=190 } {
###	        set grade 3
###	        set alarmcontent "׼ȷ��ָ��63�ӽ����ſ��˷�Χ"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}
###	           
###	    }
###	          
####################################E0����Ե�����ռ���######################
#####���µ�Ե�ʹ���û���(����ռ�ȳ�100) 
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###                    SELECT 
###             	      COUNT(DISTINCT PRODUCT_NO)
###                    FROM BASS1.G_S_21008_TO_DAY
###                    WHERE TIME_ID/100=$this_month
###                      AND END_STATUS='0'
###                      AND SVC_TYPE_ID IN ('11','12','13')
###                      AND CDR_TYPE_ID IN ('00','01','10','11')"
###            puts $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL1 [lindex $p_row 0]
###	    }
###	    aidb_commit $conn
###	    aidb_close $handle	
###	    puts "���µ�Ե�ʹ���û���:$CHECK_VAL1"
####���µ����û��� 
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###     	          SELECT
###    		COUNT(*)
###    	FROM
###    		(SELECT
###    		 	A.TIME_ID,
###    		 	A.USER_ID,
###    		 	A.USERTYPE_ID,
###    		 	A.SIM_CODE
###    		FROM BASS1.G_A_02004_DAY  A,
###    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
###    	 	 WHERE TIME_ID <= ${p_timestamp}
###    	  	 GROUP BY USER_ID
###    	     ) B
###    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
###    	    )T,
###
###    		(SELECT
###    	 		A.TIME_ID,
###    	 		A.USER_ID,
###    	 		A.USERTYPE_ID
###    		 FROM BASS1.G_A_02008_DAY A,
###    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
###    		 WHERE TIME_ID <= ${p_timestamp}
###    		 GROUP BY USER_ID
###    	     ) B
###    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
###    		) M
###   		WHERE T.USER_ID = M.USER_ID
###    		  AND T.TIME_ID <=  ${p_timestamp}
###    		  AND M.TIME_ID <=  ${p_timestamp}
###    		  AND T.USERTYPE_ID <> '3'
###    		  AND T.SIM_CODE <> '1'
###    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')"
###            puts  $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL2 [lindex $p_row 0]
###	    }
###	    aidb_commit $conn
###	    aidb_close $handle	
####���µ�Ե�����ռ���
####ע�⣺��ʽ����Ҫ���� 
###           puts "���µ����û���:$CHECK_VAL2"
###    	   set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}*100.000/${CHECK_VAL2})]]
###       	   puts $RESULT_VAL1
####��������
###           set CHECK_VAL1 0.00
###           set CHECK_VAL2 0.00
###        
#####���µ�Ե�ʹ���û���(����ռ�ȳ�100)
####	    set handle [aidb_open $conn]
####	    set sqlbuf "
####                    SELECT 
####             	      COUNT(DISTINCT PRODUCT_NO)
####                    FROM BASS1.G_S_21008_TO_DAY
####                    WHERE TIME_ID/100=$last_month
####                      AND END_STATUS='0'
####                      AND SVC_TYPE_ID IN ('11','12','13')
####                      AND CDR_TYPE_ID IN ('00','01','10','11')"
####            
####	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####	    	WriteTrace $errmsg 001
####	    	return -1
####	    }
####	    while { [set p_row [aidb_fetch $handle]] != "" } {
####	    	set CHECK_VAL1 [lindex $p_row 0]
####	    }
####	    aidb_commit $conn
####	    aidb_close $handle	
#####���µ����û���
#####ע�⣺��γ���Ҫ��GETKPI�����
####           #set CHECK_VAL1 102323
####           set CHECK_VAL2 [exec get_kpi.sh ${last_day_month} 2 2]
###
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###                    SELECT 
###                      TARGET1 
###   	            FROM 
###   	              BASS1.G_RULE_CHECK 
###   	            WHERE 
###   	              TIME_ID=$last_day_month
###                      AND RULE_CODE='E0'"
###            puts $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL2 [lindex $p_row 0]
###	    }
###	    puts "���µ�Ե�����ռ��ʣ� $CHECK_VAL2"
###	    aidb_commit $conn
###	    aidb_close $handle	
###             
####���µ�Ե�����ռ���
###           set RESULT_VAL2 $CHECK_VAL2
###           puts "���µ�Ե�����ռ��ʣ�$RESULT_VAL2"
###
###           set RESULT [format "%.2f" [expr ($RESULT_VAL1-$RESULT_VAL2)]]
###           puts $RESULT
###           
###           #��У��ֵ����У������
###           set handle [aidb_open $conn]
###           set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###             		($p_timestamp ,
###             		'E0',
###             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###             		cast ($RESULT as  DECIMAL(18, 5) ),
###             		0)"
###           if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###             	WriteTrace $errmsg 003
###             	return -1
###           }
###           aidb_commit $conn
###           aidb_close $handle
###           
###
###           
###           #�жϣ� E0��40�� < (���µ�Ե����ʹ���û��� / ���µ����û���) < 90%,
###           #            ���ռ��ʽ����±䶯���� �� 10������
###	     if { $RESULT_VAL1<=40 || $RESULT_VAL1>=90 || ($RESULT>10 || $RESULT<-10) } {
###	     	set grade 2
###	        set alarmcontent "׼ȷ��ָ��E0�������ſ��˷�Χ"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}	
###	     	   		
###	     } elseif { $RESULT_VAL1<=45 || $RESULT_VAL1>=85 || ($RESULT>8.5 || $RESULT<-8.5) } {
###	     	set grade 3
###	        set alarmcontent "׼ȷ��ָ��E0�ӽ����ſ��˷�Χ"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}	
###	           
###	    }
###	    
###        }
##################################END#######################
	return 0
}