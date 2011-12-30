#################################################################
#��������: INT_CHECK_98Z6_MONTH.tcl
#��������: 
#�����ţ�98��Z6
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��98������IMEIռ��
#          Z6��IMEI������ռ�����û�������
#����������98������IMEIռȫ��IMEI�������� �� 0.01��
#          Z6��80�� �� IMEI��ռ�����û������� �� 105��
#У�����1.BASS1.G_A_02004_DAY  �û�
#          2.BASS1.G_A_02008_DAY  �û�״̬
#          3.BASS1.G_I_02047_MONTH �û���ʶ��IMEI�Ķ�Ӧ��ϵ
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#���� yyyy-mm
	set opmonth $optime_month
	#�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	
        #��������
##        set app_name "INT_CHECK_98Z6_MONTH.tcl"
##        
##        
##        
##        set handle [aidb_open $conn]
##	set sql_buff "\
##		delete from bass1.g_rule_check where time_id=$op_month 
##		  and rule_code in ('98','Z6')"
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#--98������IMEIռ��
##        #--ͳ�ƿھ����ֱ�ͳ�ƴ�����02047�еĶ���������02004�еĵ�����Ч�û�����
##        #��02047�е�����Ч�û����� �����������IMEIռ��
##        
##        #--������02047�еĶ���������02004�еĵ�����Ч�û���
##       
##           
##       set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##                  COUNT(DISTINCT P.USER_ID) 
##		FROM 
##		(
##                  SELECT USER_ID 
##                  FROM BASS1.G_I_02047_MONTH 
##                  WHERE TIME_ID=$op_month
##		  EXCEPT
##		  SELECT T.USER_ID
##    		  FROM 
##    		    (    SELECT
##    		 	   A.TIME_ID,
##    		 	   A.USER_ID,
##    		 	   A.USERTYPE_ID,
##    		 	   A.SIM_CODE
##    			 FROM BASS1.G_A_02004_DAY A,
##    			(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##    	 	 	 WHERE TIME_ID <=$this_month_last_day
##    	  	         GROUP BY USER_ID
##    	     	         ) B
##    	                WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    	            )T, 	
##    	    
##                    (     SELECT
##    	 		    A.TIME_ID,
##    	 		    A.USER_ID,
##    	 		    A.USERTYPE_ID
##    		    	  FROM BASS1.G_A_02008_DAY A,
##    		         (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
##    		          WHERE TIME_ID <=$this_month_last_day
##    		 	  GROUP BY USER_ID
##    	                 ) B
##    			 WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    		     ) M		
##   		  WHERE 
##   		     T.USER_ID = M.USER_ID
##    		     AND T.TIME_ID <=$this_month_last_day
##    		     AND M.TIME_ID <=$this_month_last_day
##    		     AND T.USERTYPE_ID <> '3'
##    		     AND T.SIM_CODE <> '1'	
##	     )P"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##       puts $DEC_RESULT_VAL1
##       
##       #--02047�е�����Ч�û���
##        set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##            	COUNT(DISTINCT USER_ID) 
##            FROM BASS1.G_I_02047_MONTH 
##            WHERE TIME_ID=$op_month"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL2 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	puts $DEC_RESULT_VAL2
##	
##	#--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'98',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	 #--�ж�
##         #--�쳣
##         #--1������IMEIռȫ��IMEI�������� �� 0.01������
##	if {[format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]>0.0001} {
##	        set grade 2
##	        set alarmcontent "׼ȷ��ָ��98�������ſ��˷�Χ"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##          }
##        
##       # ---------------------------------
##       # --Z6��IMEI������ռ�����û�������
##       # --ͳ�ƿھ���Step1.������02047����ͬʱ��02004��״̬�����������û�״̬������
##       # --               (2010,2020,2030,1040,1021,9000),ͬʱ��02008�ӿ����û����Ͳ����ڲ����û���
##       # --               Ҳ��������SIM����IMEI�û�����
##       # --          Step2.��KPI��ȡ�û���������Ϊ�����û���
##       # --IMEI��
##       set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##     	          BIGINT(COUNT(DISTINCT AB.USER_ID))
##                FROM
##    	        (
##    	           SELECT 
##         	     USER_ID 
##                   FROM BASS1.G_I_02047_MONTH
##                   WHERE TIME_ID=$op_month 
##                )AB,
##          
##    	       (  
##    	          SELECT 
##    		    T.USER_ID
##    	          FROM 
##    		  (
##    		    SELECT
##    		      A.TIME_ID,
##    		      A.USER_ID,
##    		      A.USERTYPE_ID,
##    		      A.SIM_CODE
##    		    FROM BASS1.G_A_02004_DAY  A,
##    		   (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##    	 	    WHERE TIME_ID <= $this_month_last_day
##    	  	    GROUP BY USER_ID
##    	           ) B
##    	           WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
##    	         )T,  	          	  	    
##    		 (
##    		    SELECT
##    	 	      A.TIME_ID,
##    	 	      A.USER_ID,
##    	 	      A.USERTYPE_ID
##    		    FROM BASS1.G_A_02008_DAY A,
##    		   (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
##    		    WHERE TIME_ID <=$this_month_last_day
##    		    GROUP BY USER_ID
##    	           ) B
##    		   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    		) M		
##   	       WHERE T.USER_ID = M.USER_ID
##    	         AND T.TIME_ID <=$this_month_last_day
##    		 AND M.TIME_ID <=$this_month_last_day
##    		 AND T.USERTYPE_ID <> '3'
##    		 AND T.SIM_CODE <> '1'
##    		 AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')
##         ) CD
##        WHERE AB.USER_ID=CD.USER_ID;"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##       puts $DEC_RESULT_VAL1
##       set DEC_RESULT_VAL2 [exec get_kpi.sh ${this_month_last_day} 2 2]
##       
##       #--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]    
##	puts $DEC_TARGET_VAL1
##	#--�ж�
##        #--�쳣
##        #--1��80�� �� IMEI��ռ�����û������� �� 105������ 
##	if {$DEC_TARGET_VAL1>1.05 || $DEC_TARGET_VAL1<0.8} {
##		set grade 2
##	        set alarmcontent "׼ȷ��ָ��Z6�������ſ��˷�Χ"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }                                                                                                                                                                               
##
	return 0
}