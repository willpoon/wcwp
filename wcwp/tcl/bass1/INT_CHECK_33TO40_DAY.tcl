#################################################################
#��������: INT_CHECK_33TO40_DAY.tcl
#��������: 
#�����ţ�33,34,35,38,39,40
#�������ԣ�ָ���춯
#�������ͣ���
#�������ȣ���
#ָ��ժҪ��33��ȫ��ͨ��������
#          34����������������
#          35�����еش���������
#          38: ȫ��ͨ�û���������
#          39: �������û���������
#          40: ���еش��û���������
#����������33�����壺ȫ��ͨ�������� = ����ȫ��ͨ�û������� /������������12/����������/ ����ȫ��ͨ�û������� x 100%
#              ����ȫ��ͨ�������� �� 5%
#          34�����壺�������������� = �����������û�������/������������12/����������/ �����������û������� x 100%
#			    ������������������ �� 10%
#          35�����壺���еش��������� = ���¶��еش������û���/������������12/����������/ ���¶��еش��û������� x 100%
#              ���򣺶��еش��������� �� 15%
#          38: ���壺ȫ��ͨ�û��������� = (����ȫ��ͨ�û������� / ����ȫ��ͨ�û�������) x 100%
#              ����ȫ��ͨ�û��������� �� 5%
#          39: ���壺�������û��������� = (�����������û������� / �����������û�������) x 100%
#              �����������û��������� �� 10%
#          40: ���壺���еش��û��������� = (���¶��еش��û������� / ���¶��еش��û�������) x 100%
#              ���򣺶��еش��û��������� �� 20%
#У�����1.BASS1.G_A_02004_DAY  �û�
#          2.BASS1.G_A_02008_DAY  �û�״̬
#�������:							
#�������: ����ֵ:0 �ɹ�;-1 ʧ��							
#�� д �ˣ�����							
#��дʱ�䣺2007-03-22							
#�����¼��1.							
#�޸���ʷ: 1.	20091125 ȡ�����й���У��
#################################################################							
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)
        #���ϸ��� ��ʽ yyyymm
        set last_month [GetLastMonth [string range $Timestamp 0 5]]
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $Timestamp 0 5]01]
        #puts $last_month
        
        #puts $last_day_month
         #----��������-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        puts $thismonthdays
        #----��������-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        puts $thisyeardays
        set app_name "INT_CHECK_33TO40_DAY.tcl"
        set day [string range $op_time 8 9]
     
        #--ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$Timestamp
        AND RULE_CODE IN ('33','34','35','38','39','40')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	
#####	#�ж�����
#####        if { $day < 26 } {
#####        	    	puts "���� $day �ţ�δ��23�ţ��ݲ�����"
#####        	    	return 0
#####        	        }
#####	
#####	#--DEC_CHECK_VALUE_4:����ȫ��ͨ�û�������
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_4 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts "$DEC_CHECK_VALUE_4"
#####	
#####	#--DEC_CHECK_VALUE_5:�����������û�������;
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_5 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts $DEC_CHECK_VALUE_5
#####	
#####	#--DEC_CHECK_VALUE_6:���¶��еش��û�������;
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_6 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts $DEC_CHECK_VALUE_6
#####	#----------------------------------
#####	#--33��ȫ��ͨ��������
#####         #--����ȫ��ͨ������
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='1' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--��������
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--����ȫ��ͨ�û�������
#####        set DEC_RESULT_VAL2 "$DEC_CHECK_VALUE_4"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'33',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--�ж�
#####        #--�쳣��
#####        #1��ȫ��ͨ�������� �� 5%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.05 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��33��ȫ��ͨ�������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	         } 
#####	puts "33 finish"
#####	
#####	#---------------------------
#####	 #--34����������������
#####          #--����������������
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='2' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--��������
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--�����������û�������
#####        set DEC_RESULT_VAL2 "$DEC_CHECK_VALUE_5"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'34',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--�ж�
#####        #--�쳣��
#####        #1���������������� �� 10%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.1 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��34���������������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####		
#####         } 
#####	puts "34 finish"
#####	#-----------------------------------------------
#####	#--35�����еش���������
#####        #--���¶��еش�������
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='3' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--��������
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--���¶��еش��û�������
#####        set DEC_RESULT_VAL2 "${DEC_CHECK_VALUE_6}"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'35',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--�ж�
#####        #--�쳣��
#####        #1�����еش��������� �� 10%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.15 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��35�����еش��������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	          } 
#####	puts "35 finish"
#####	#------------------------------
#####	#--38: ȫ��ͨ�û���������
#####   #--����ȫ��ͨ�����û���
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----����ȫ��ͨ�û�������
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_4}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'38',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.05 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��38��ȫ��ͨ�û��������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	         } 
#####	puts "38 finish"
#####	#------------------------
#####	#--39: �������û���������
#####        #--���������������û���
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----�����������û�������
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_5}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'39',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.1 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��39���������û��������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	                 } 
#####	puts "39 finish"
#####	#---------------------------
#####	 #--40: ���еش��û���������
#####   #--���¶��еش������û���
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----���¶��еش��û�������
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_6}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--����ͳ��У��ֵ
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--��У��ֵ����У������
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'40',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.2 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "׼ȷ��ָ��40�����еش��û��������ʳ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####		    } 
#####	puts "40 finish"
#####	
#####	
#####       
#####
#####	aidb_close $handle
#####
	return 0
}
