#################################################################
#��������: INT_CHECK_D9E234F2_651TO56_MONTH.tcl
#�����ţ�D9,E2,E3,E4,F2,F3,F4,F5,F6,51,52,53,54,55,56
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��D9: ��ҵ���ռ���
#          E2: ���Ÿ����û���ռ��
#          E3: ������ʾ�����û���ռ��
#          E4: GPRS���ѿͻ���ռ��
#          F2: WAPҵ������ռ��
#          F3: ������ʾҵ������ռ��
#          F4: ����ҵ������ռ��
#          F5: ��������ռ��
#          F6: GPRS����ռ��
#          51: ҵ�������±䶯��
#          52: ȫ��ͨҵ�������±䶯��             + �澯�账��
#          53: ������ҵ������䶯��               + �澯�账��
#          54: ���еش�ҵ�������±䶯��           + �澯�账��
#          55: ��ҵ������ռ��
#          56: ����ҵ������ռ��
#����������D9: 70�� �� (������ҵ���û��� / ���µ����û���) < 100%�����ռ��ʽ����±䶯���� �� 5��
#          E2: 8% �� ���Ÿ����û���/��ҵ�񸶷��û��� �� 15%
#          E3: ������ʾ�����û���/��ҵ�񸶷��û��� > 40%
#          E4: 8% �� GPRS�������û���/��ҵ�񸶷��û��� �� 20%
#          F2: ��WAPҵ������ռ/��ҵ�����룩�±䶯�� �� 30%
#          F3: 12% �� ������ʾҵ������/��ҵ������ �� 35%
#          F4: ������ҵ��ҵ������/��ҵ�����룩�±䶯�� �� 30%
#          F5: ������ҵ������/��ҵ�����룩�±䶯�� �� 30%
#          F6: ��GPRSҵ������/��ҵ�����룩�±䶯�� �� 30%
#        +  51: | (������ƽ��ҵ������ / ������ƽ��ҵ������ - 1) x 100% | �� 10%
#        +  52: | (����ȫ��ͨ��ƽ��ҵ������ / ����ȫ��ͨ��ƽ��ҵ������ - 1) x 100% | �� 10%
#        +  53: | (������������ƽ��ҵ������ / ������������ƽ��ҵ������ - 1) x 100% | �� 10%
#        +  54: | (���¶��еش���ƽ��ҵ������ / ���¶��еش���ƽ��ҵ������ - 1) x 100% | �� 20%
#          55: 15�� �� (������ҵ������ / ����ҵ������) �� 25%
#          56: 20�� �� (���¶���ҵ������ / ������ҵ������) �� 65%
#У�����
#          BASS1.G_S_03004_MONTH
#          BASS1.G_S_03005_MONTH
#          BASS1.G_S_03012_MONTH
#          BASS1.G_S_04006_DAY
#          BASS1.G_A_02004_DAY
#          BASS1.G_A_02008_DAY
#�������:                        
# ����ֵ:   0 �ɹ�; -1 ʧ��       
#***************************************************/

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#���� yyyy-mm
	set opmonth $optime_month	
	#���� yyyy-mm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #��������
        set this_month_days [GetThisMonthDays ${op_month}01]
        #�������� 
        set this_year_days [GetThisYearDays ${op_month}01]
        #��������
        set last_month_days [GetThisMonthDays ${last_month}01]
        #�������һ�� yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]   
        #��������
        set app_name "INT_CHECK_D9E234F2_651TO56_MONTH.tcl"
        
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
            delete from bass1.g_rule_check where time_id=$op_month
              and rule_code in ('D9','E2','E3','E4','F2','F3','F4','F5','F6','51','52','53','54','55','56')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
       
       
        #--DEC_CHECK_VALUE_1:������ҵ������
        #--DEC_CHECK_VALUE_3:������ҵ������
        #--DEC_CHECK_VALUE_5:��ҵ�񸶷��û���
        #--DEC_CHECK_VALUE_2:����ҵ������
        #--DEC_CHECK_VALUE_4:����ҵ������
        #--������ҵ������
       
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
                     SUM(T.CNT)
                   FROM 
                     (
      		            SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		            WHERE TIME_ID =$op_month
      	                    AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	                    UNION ALL
                        SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		            WHERE TIME_ID =$op_month
      	                     AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	             ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	puts "������ҵ������:$DEC_CHECK_VALUE_1" 

	#--����ҵ������
	set handle [aidb_open $conn]
        set sqlbuf "\
            SELECT
              SUM(T.CNT)
            FROM 
               (
            	  SELECT SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                  WHERE TIME_ID = $op_month
                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
                   UNION ALL
                  SELECT SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                  WHERE TIME_ID =$op_month
               ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "����ҵ������:$DEC_CHECK_VALUE_2"
	
	#--������ҵ������
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
                     SUM(T.CNT)
                   FROM 
                     (
      		            SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		            WHERE TIME_ID =$last_month
      	                    AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	                     UNION ALL
                        SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		            WHERE TIME_ID =$last_month
      	                     AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	             ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_3 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "������ҵ������:$DEC_CHECK_VALUE_3"	

	#----����ҵ������
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
              SUM(T.CNT)
            FROM 
               (
            	  SELECT SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                  WHERE TIME_ID = INT($last_month)
                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
                  UNION ALL
                  SELECT SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                  WHERE TIME_ID =INT($last_month)
               ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_4 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "����ҵ������:$DEC_CHECK_VALUE_4"

	#--DEC_CHECK_VALUE_5:��ҵ�񸶷��û���
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
               COUNT(DISTINCT A.USER_ID)
             FROM 
             (
             	SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
             	WHERE BIGINT(A.FEE_RECEIVABLE)>0
             	    AND A.TIME_ID =$op_month
             	    AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
             	UNION ALL
			    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
			    WHERE A.TIME_ID =$op_month
			        AND BIGINT(A.INCM_AMT)>0
			        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
			  ) A,  
             (
             	SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
                FROM BASS1.G_A_02004_DAY A,
                    (    
            		  SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                      WHERE TIME_ID <=$this_month_last_day
                      GROUP BY USER_ID
            		 ) B
                WHERE A.USER_ID = B.USER_ID
                   AND A.TIME_ID = B.TIME_ID
             )B
            WHERE A.USER_ID = B.USER_ID
            	 AND B.USERTYPE_ID <> '3'
            	 AND B.SIM_CODE <> '1';"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_5 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	puts "��ҵ�񸶷��û���:$DEC_CHECK_VALUE_5"                  

#	#D9: ��ҵ���ռ���
#        #������ҵ���ռ���*100
#        #�ֱ��KPI����ȡ��(������ҵ���û��� / ���µ����û���)
#        #������ҵ���û���/���µ����û���
#        #������ҵ������
#         
#        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $this_month_last_day 2 2]
#        set DEC_RESULT_VAL1 [format "%.2f" [expr ${DEC_CHECK_VALUE_5}/1.00/${DEC_CHECK_VALUE_7} *100]] 
#        
#        puts "���µ����û�����$DEC_CHECK_VALUE_7"
#        puts "������ҵ���ռ��ʣ�$DEC_RESULT_VAL1"
#        
#        #--��������
#        set $DEC_CHECK_VALUE_7 "0";
#        
#        #������ҵ���û���/���µ����û���
#        set DEC_CHECK_VALUE_6 [exec get_kpi.sh $last_month 7 2];
#        puts "������ҵ���û���:$DEC_CHECK_VALUE_6"
#        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $last_month_last_day 2 2]
#        puts "���µ����û���:$DEC_CHECK_VALUE_7"        
#        set DEC_RESULT_VAL2 [format "%.2f" [expr ${DEC_CHECK_VALUE_6}/1.00000/${DEC_CHECK_VALUE_7}*100.000]] 
#        
#        #��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'D9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#�ж�
#        #D9:��70�� �� (������ҵ���û��� / ���µ����û���) < 100%�����ռ��ʽ����±䶯���� �� 5������
#	if {${DEC_RESULT_VAL1}<70 || ${DEC_RESULT_VAL1}>=100 || ${DEC_RESULT_VAL1}-${DEC_RESULT_VAL2}>5 || ${DEC_RESULT_VAL1}-${DEC_RESULT_VAL2}<-5} {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��D9�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#       puts "D9 end"  	

#       #--E2: ���Ÿ����û���ռ��
#       #--���Ÿ����û���
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0615' AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0615' AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "���Ÿ����û���:$DEC_RESULT_VAL1"
#	
#	#--��ҵ�񸶷��û���
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1��8% �� ���Ÿ����û���/��ҵ�񸶷��û��� �� 15%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#	if {${DEC_RESULT}<0.08 || ${DEC_RESULT}>0.15} {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��E2�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E2 end"       
#
#       #--E3: ���Ÿ����û���ռ��
#       #--������ʾ�����û���
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501' AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501' AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "������ʾ�����û���:$DEC_RESULT_VAL1"
#	
#	
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1��������ʾ�����û���/��ҵ�񸶷��û��� > 40%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#        set handle [aidb_open $conn]
#	if {${DEC_RESULT}<=0.4} {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��E3�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E3 end"         
#
#         #--E4: GPRS���ѿͻ���ռ��
#       #--GPRS�������û���
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624') AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624') AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "$DEC_RESULT_VAL1"
#	
#	
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#	#set DEC_RESULT_VAL2 "50"
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1��8% �� GPRS�������û���/��ҵ�񸶷��û��� ��20%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#	if {${DEC_RESULT}<0.08 ||${DEC_RESULT}>0.20 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��E4�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E4 end"         
#
#       #--F2: WAPҵ������ռ��
#       #--����ռ��*100.0
#       #--WAPҵ������
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0618','0638')
#              UNION ALL
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0618','0638')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	puts "$DEC_CHECK_VALUE_1"
#	puts "($DEC_RESULT_VAL1 /1.0000/$DEC_CHECK_VALUE_1* 100.000)"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr ($DEC_RESULT_VAL1 /1.0000/$DEC_CHECK_VALUE_1* 100.000)]] 
#	puts $DEC_RESULT_VAL1
#	puts "1111111"
#	#--����ռ��*100.0
#        #--WAPҵ������
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0618','0638')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0618','0638')
#            )T"
#
#	puts "222222"
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "3333333"
#	
#	
#	puts $DEC_RESULT_VAL2
#        #set DEC_RESULT_VAL2 "45"
#	#set DEC_CHECK_VALUE_3 "50.0"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.0000/${DEC_CHECK_VALUE_3} * 100.00)]]  
#	#set DEC_RESULT_VAL2 "0.8"
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--F2����WAPҵ������ռ/��ҵ�����룩�±䶯�� �� 30%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#	if {${DEC_RESULT}<-0.3 ||${DEC_RESULT}>0.3 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��F2�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F2 end"      
#
#       #--F3: ������ʾҵ������ռ��
#       #-- ������ʾҵ������
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501'
#              UNION ALL
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501'
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "������ʾҵ������:$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /100.00)]] 
#	
#	#--��ҵ������
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /100.00)]]  
#	#set DEC_RESULT_VAL2 "0.8"
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--F3��12% �� ������ʾҵ������/��ҵ������ �� 35%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#
#	if {${DEC_RESULT}<0.12 ||${DEC_RESULT}>0.35 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��F3�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F3 end"  
#
#         #--F4: ����ҵ������ռ��
#         #--����ռ��*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0615','0637')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "���²���ҵ������ռ��:$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100)]] 
#	
#	#--����ռ��*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	puts "���²���ҵ������ռ��:$DEC_RESULT_VAL2"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]  
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1��������ҵ��ҵ������/��ҵ�����룩�±䶯�� �� 30%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��F4�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F4 end"                   
#         
#         #--F5: ��������ռ��
#      #--����ռ��*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0519','0639')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0519','0639')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "���²������룺$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100.00)]] 
#	
#	#--����ռ��*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0519','0639')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn	
#	aidb_close $handle
#	
#	puts "���²������룺$DEC_RESULT_VAL2"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]     
#	
#	
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1��������ҵ������/��ҵ�����룩�±䶯�� �� 30%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��F5�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F5 end"     
#
#         #--F6: ��GPRSҵ������/��ҵ�����룩�±䶯�� �� 30%
#         #--����ռ��*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100)]] 
#	
#	#--����ռ��*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	puts $DEC_RESULT_VAL2
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]  
#
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--�ж�
#        #--�쳣
#        #--1����GPRSҵ������/��ҵ�����룩�±䶯�� �� 30%����
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "׼ȷ��ָ��F6�������ſ��˷�Χ"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F6 end"
#        
        #--51: ҵ�������±䶯��
        #--������ƽ��ҵ������
        set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_CHECK_VALUE_2} /1.00 /${this_month_days}/100.00]] 
        #--������ƽ��ҵ������
        set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_CHECK_VALUE_4} /1.00 /${last_month_days}/100.00]] 
        
        #--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'51',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	#--�ж�
        #--�쳣
        #--1��| (������ƽ��ҵ������ / ������ƽ��ҵ������ - 1) x 100% | �� 10%����
        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��51�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        } 
         puts "51 end"        

         #--52: ȫ��ͨҵ�������±䶯��
         #--����ȫ��ͨ��ƽ��ҵ������
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $op_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$this_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='1'
 	           )T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #����ȫ��ͨ��ƽ��ҵ������
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = INT($last_month)
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$last_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='1'
 	           )T"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--��У��ֵ����У������
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'52',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--�ж�
         #--�쳣
         #--1��| (����ȫ��ͨ��ƽ��ҵ������ / ����ȫ��ͨ��ƽ��ҵ������ - 1) x 100% | �� 10%����
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

 	if {${DEC_RESULT}<-0.10 ||${DEC_RESULT}>0.10 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��52�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "52 end"
 
         #--53: ������ҵ�������±䶯��
         #--������������ƽ��ҵ������
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(A.CNT)
            FROM 
               (
            	SELECT USER_ID,COALESCE(SUM(BIGINT(SHOULD_FEE)),0) AS CNT FROM BASS1.G_S_03005_MONTH
                WHERE TIME_ID = $op_month
                GROUP BY USER_ID   
 	            UNION ALL
            	SELECT USER_ID,SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                WHERE TIME_ID = $op_month
                GROUP BY USER_ID
               ) A,
              (
 	            SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                FROM BASS1.G_A_02004_DAY A,
                  (
                    SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                    WHERE TIME_ID <=$this_month_last_day
                    GROUP BY USER_ID
                   )B
                 WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	           )B
	          WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='2';"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #--������������ƽ��ҵ������
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(A.CNT)
            FROM 
               (
            	SELECT USER_ID,COALESCE(SUM(BIGINT(SHOULD_FEE)),0) AS CNT FROM BASS1.G_S_03005_MONTH
                WHERE TIME_ID = INT($last_month)
                GROUP BY USER_ID   
 	            UNION ALL
            	SELECT USER_ID,SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                WHERE TIME_ID = INT($last_month)
                GROUP BY USER_ID
               ) A,
              (
 	            SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                FROM BASS1.G_A_02004_DAY A,
                  (
                    SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                    WHERE TIME_ID <=$last_month_last_day
                    GROUP BY USER_ID
                   )B
                 WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	           )B
	          WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='2';"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--��У��ֵ����У������
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'53',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--�ж�
         #--�쳣
         #--1��| (������������ƽ��ҵ������ / ������������ƽ��ҵ������ - 1) x 100% | �� 10%����
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]]  

 	if {${DEC_RESULT}<-0.10 ||${DEC_RESULT}>0.10 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��53�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "53 end"
          
          #--54: ���еش�ҵ�������±䶯��
         #--���¶��еش���ƽ��ҵ������
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $op_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$this_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='3'
 	           )T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #----���¶��еش���ƽ��ҵ������
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $last_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$last_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='3'
 	           )T"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--��У��ֵ����У������
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'54',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--�ж�
         #--�쳣
         #--1�� (���¶��еش���ƽ��ҵ������ / ���¶��еش���ƽ��ҵ������ - 1) x 100% | �� 20%����
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

 	if {${DEC_RESULT}<-0.20 ||${DEC_RESULT}>0.20 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��54�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "54 end"
          
           #--55: ��ҵ������ռ��
           #--������ҵ������
           #set DEC_CHECK_VALUE_1 "50"
           #set DEC_CHECK_VALUE_2 "50"
           set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /${this_month_days}/100)]] 
           #--����ҵ������
           set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_2} /1.00 /${last_month_days}/100)]] 
	 #--��У��ֵ����У������
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'55',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	 #--�ж�
         #--�쳣
         #--1�� (15�� �� (������ҵ������ / ����ҵ������) �� 25%����
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} ]] 

 	if {${DEC_RESULT}<0.15 ||${DEC_RESULT}>0.25 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��55�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "55 end"
          
          #--56: ����ҵ������ռ��
         #--���¶���ҵ������
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            (A.CNT+B.CNT)/100.0
          FROM 
            (
      		   SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		   WHERE TIME_ID =$op_month
      	           AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	           AND ACCT_ITEM_ID IN ('0601','0603','0605','0606','0607','0609','0611','0613')
      	     ) A,
            (
               SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		   WHERE TIME_ID =$op_month
      	            AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	            AND ACCT_ITEM_ID IN ('0601','0603','0605','0606','0607','0609','0611','0613')
      	    ) B"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	         
        #----������ҵ������
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /100)]] 
	 #--��У��ֵ����У������
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'56',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--�ж�
         #--�쳣
         #--1�� 20�� �� (���¶���ҵ������ / ������ҵ������) �� 65%����
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} ]] 

 	if {${DEC_RESULT}<0.2  ||${DEC_RESULT}>0.65 } {
	           set grade 2
	           set alarmcontent "׼ȷ��ָ��56�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "56 end"
            
#################################################      
	return 0
}