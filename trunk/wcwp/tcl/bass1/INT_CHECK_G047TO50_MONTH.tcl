#################################################################
#��������: INT_CHECK_G047TO50_MONTH.tcl
#��������:
#�������:
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
#�������ԣ�ƽ���ϵ
#�������ͣ���
#ָ��ժҪ��G0: ȫ��ͨ�߼�ֵ�û�ռ��
#          47: ARPU��Ʒ�ƺϼƣ�
#          48: ARPU��ȫ��ͨ��
#          49: ARPU�������У�
#          50: ARPU�����еش���
#����������G0: 23% �� ȫ��ͨ�߼�ֵ�û���/ȫ��ͨ�û������� ��35%
#          47: 35Ԫ �� ����ARPUֵ �� 150Ԫ ��RMB��
#          48: 80Ԫ �� ����ȫ��ͨARPUֵ �� 350Ԫ ��RMB��
#          49: 25Ԫ �� ����������ARPUֵ �� 120Ԫ ��RMB��
#          50: 40Ԫ �� ���¶��еش�ARPUֵ �� 150Ԫ ��RMB��
#У�����
#          BASS1.G_S_03005_MONTH    
#          BASS1.G_A_02004_DAY    
#          BASS1.G_S_03012_MONTH  
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
# liuzhilong 2009-8-3 ���׳���
#***************************************************/

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##	#���� yyyymm
##	#set optime_month 2007-12-02
##	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
##	#���� yyyy-mm
##	set opmonth $optime_month
##	#�������һ�� yyyymmdd
##        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
##        #��������
##        set this_month_days [GetThisMonthDays ${op_month}01]
##        #�������� 
##        set this_year_days [GetThisYearDays ${op_month}01]
##        #�������һ�� yyyymmdd
##        set last_month [GetLastMonth [string range $op_month 0 5]]
##        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
##        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]    
##        #��������
##        set app_name "INT_CHECK_G047TO50_MONTH.tcl"
##
##
##        #ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "
##		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$op_month
##    	              AND RULE_CODE IN ('G0','47','48','49','50') "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##
##	#--G0: ȫ��ͨ�߼�ֵ�û�ռ��
##        #--ȫ��ͨ�߼�ֵ�û���
##        set handle [aidb_open $conn]
##        set sqlbuf "\
##	        SELECT
##	           COUNT(DISTINCT A.USER_ID)
##	        FROM 
##	        (
##	        	SELECT USER_ID FROM BASS1.G_S_03005_MONTH
##	        	WHERE TIME_ID = $op_month
##	        	GROUP BY USER_ID
##	        	HAVING SUM(INT(SHOULD_FEE))/700 >=$this_month_days
##	        	) A,      
##	        (
##	           SELECT A.USER_ID,A.USERTYPE_ID
##	           FROM  BASS1.G_A_02004_DAY A,
##	        	      (
##	        	        SELECT USER_ID,MAX(TIME_ID) AS TIME_ID 
##	        	        FROM BASS1.G_A_02004_DAY 
##	        	        WHERE TIME_ID <=$this_month_last_day
##	        	        GROUP BY USER_ID
##	                 ) B
##	            WHERE A.USER_ID = B.USER_ID
##	            AND A.TIME_ID = B.TIME_ID
##	            AND A.BRAND_ID='1'
##	        )B
##	        WHERE A.USER_ID = B.USER_ID
##	        AND B.USERTYPE_ID <> '3'"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "$DEC_RESULT_VAL1"
##
##	set DEC_RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 5 2]
##
##	puts "$DEC_RESULT_VAL2"
##
##	#--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'G0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	#--�ж�
##        #--�쳣
##        #--23% �� ȫ��ͨ�߼�ֵ�û���/ȫ��ͨ�û������� �� 35%����
##        set DEC_RESULT [format "%.5f" [expr ${DEC_RESULT_VAL1} /1.00000/${DEC_RESULT_VAL2}]]
##        #set DEC_RESULT "50"
##	if {${DEC_RESULT}<0.23 ||${DEC_RESULT}>0.35 } {
##	           set grade 2
##	           set alarmcontent "׼ȷ��ָ��G0�������ſ��˷�Χ"
##	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
##         puts "G0 end"
##
##        #--47: ARPU��Ʒ�ƺϼƣ�
##        #--����ҵ������
##	set handle [aidb_open $conn]
##        set sqlbuf "\
##            SELECT
##              SUM(T.CNT)
##            FROM
##               (
##            	  SELECT SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
##                  WHERE 
##                      TIME_ID =$op_month
##                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
##                  UNION ALL
##                  SELECT SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
##                  WHERE 
##                      TIME_ID =$op_month
##               )T "
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	puts "$DEC_CHECK_VALUE_1"
##	set DEC_CHECK_VALUE_2 [exec get_kpi.sh $this_month_last_day 2 2]
##	set DEC_CHECK_VALUE_3 [exec get_kpi.sh $last_month_last_day 2 2]
##        
##        set DEC_RESULT_VAL1 [format "%.5f" [expr ${DEC_CHECK_VALUE_1}/(${this_month_days} *12.0000 /${this_year_days})/(($DEC_CHECK_VALUE_2 + $DEC_CHECK_VALUE_3)/2.0000)]]
##	set DEC_RESULT_VAL1 [format "%.5f" [expr ${DEC_RESULT_VAL1} /100.0000]]
##	#--��������
##        set DEC_CHECK_VALUE_1 "0"
##        set DEC_CHECK_VALUE_2 "0"
##        set DEC_CHECK_VALUE_3 "0"
##
##        #--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'47',$DEC_RESULT_VAL1,0,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	#--�ж�
##        #--�쳣
##        #--35Ԫ �� ����ARPUֵ �� 150Ԫ ��RMB������
##	if {$DEC_RESULT_VAL1<35 || $DEC_RESULT_VAL1>150 } {
##	           set grade 2
##	           set alarmcontent "׼ȷ��ָ��47�������ſ��˷�Χ"
##	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
##         puts "47 end"
##
##         #--48: ARPU��ȫ��ͨ��
##         #--����ȫ��ͨҵ������
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT
##            SUM(T.CNT)
##            FROM
##               (
##                 SELECT A.CNT FROM
##                 (
##            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
##                   WHERE TIME_ID =$op_month
##                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
##                   GROUP BY USER_ID
##                  ) A,
##                  (
## 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
##                     FROM BASS1.G_A_02004_DAY A,
##                          (
##                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##                            WHERE TIME_ID <=$this_month_last_day
##                            GROUP BY USER_ID
##                           )B
##                     WHERE A.USER_ID = B.USER_ID
##                     AND A.TIME_ID = B.TIME_ID
## 	               )B
##	              WHERE A.USER_ID = B.USER_ID
##	                AND B.USERTYPE_ID <> '3'
##	                AND B.SIM_CODE <> '1'
##                    AND B.BRAND_ID='1'
## 	           )T"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	puts "$DEC_CHECK_VALUE_1"
##
##	set DEC_CHECK_VALUE_2 [exec get_kpi.sh $this_month_last_day 5 2]
##	set DEC_CHECK_VALUE_3 [exec get_kpi.sh $last_month_last_day 5 2]
##
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_CHECK_VALUE_1} /(($DEC_CHECK_VALUE_2 + $DEC_CHECK_VALUE_3)/2.0000)]]
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /100.0000]]
##	#--��������
##        set DEC_CHECK_VALUE_1 "0"
##        set DEC_CHECK_VALUE_2 "0"
##        set DEC_CHECK_VALUE_3 "0"
##
##        #--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'48',$DEC_RESULT_VAL1,0,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	#--�ж�
##        #--�쳣
##        #--80Ԫ �� ����ȫ��ͨARPUֵ �� 350Ԫ ��RMB������
##	if {$DEC_RESULT_VAL1<80 || $DEC_RESULT_VAL1>350 } {
##	           set grade 2
##	           set alarmcontent "׼ȷ��ָ��48�������ſ��˷�Χ"
##	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
##         puts "48 end"
##
##         #--49: ARPU�������У�
##
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT
##            SUM(T.CNT)
##            FROM
##               (
##                 SELECT A.CNT AS CNT FROM
##                 (
##            	   SELECT USER_ID,COALESCE(SUM(BIGINT(SHOULD_FEE)),0) AS CNT FROM BASS1.G_S_03005_MONTH
##                   WHERE TIME_ID = $op_month
##                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
##                   GROUP BY USER_ID
##                  ) A,
##                  (
## 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
##                     FROM BASS1.G_A_02004_DAY A,
##                          (
##                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##                            WHERE TIME_ID <=$this_month_last_day
##                            GROUP BY USER_ID
##                           )B
##                     WHERE A.USER_ID = B.USER_ID
##                     AND A.TIME_ID = B.TIME_ID
## 	               )B
##	              WHERE A.USER_ID = B.USER_ID
##	                AND B.USERTYPE_ID <> '3'
##	                AND B.SIM_CODE <> '1'
##                    AND B.BRAND_ID='2'
## 	           UNION ALL
##                 SELECT A.CNT AS CNT FROM
##                 (
##            	   SELECT USER_ID,SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
##                   WHERE TIME_ID = $op_month
##                   GROUP BY USER_ID
##                  ) A,
##                  (
## 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
##                     FROM BASS1.G_A_02004_DAY A,
##                          (
##                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##                            WHERE TIME_ID <=$this_month_last_day
##                            GROUP BY USER_ID
##                           )B
##                     WHERE A.USER_ID = B.USER_ID
##                     AND A.TIME_ID = B.TIME_ID
## 	               )B
##	              WHERE A.USER_ID = B.USER_ID
##	                AND B.USERTYPE_ID <> '3'
##	                AND B.SIM_CODE <> '1'
##                    AND B.BRAND_ID='2'
## 	           )T;"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	puts "$DEC_CHECK_VALUE_1"
##
##	set DEC_CHECK_VALUE_2 [exec get_kpi.sh $this_month_last_day 6 2]
##	set DEC_CHECK_VALUE_3 [exec get_kpi.sh $last_month_last_day 6 2]
##
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_CHECK_VALUE_1}/(($DEC_CHECK_VALUE_2 + $DEC_CHECK_VALUE_3)/2.0000)]]
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /100.0000]]
##	#--��������
##        set DEC_CHECK_VALUE_1 "0"
##        set DEC_CHECK_VALUE_2 "0"
##        set DEC_CHECK_VALUE_3 "0"
##
##        #--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'49',$DEC_RESULT_VAL1,0,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#--�ж�
##        #--�쳣
##        #--25Ԫ �� ����������ARPUֵ �� 120Ԫ ��RMB������
##	if {$DEC_RESULT_VAL1<25 || $DEC_RESULT_VAL1>120 } {
##	           set grade 2
##	           set alarmcontent "׼ȷ��ָ��49�������ſ��˷�Χ"
##	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
##         puts "49 end"
##
##         #--50: ARPU�����еش���
##
##        set handle [aidb_open $conn]
##        set sqlbuf "select sum(t.cnt) from 
##                (
##                 SELECT A.CNT FROM
##                 (
##            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
##                   WHERE TIME_ID = $op_month
##                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
##                   GROUP BY USER_ID
##                  ) A,
##                  (
## 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
##                     FROM BASS1.G_A_02004_DAY A,
##                          (
##                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##                            WHERE TIME_ID <=$this_month_last_day
##                            GROUP BY USER_ID
##                           )B
##                     WHERE A.USER_ID = B.USER_ID
##                     AND A.TIME_ID = B.TIME_ID
## 	               )B
##	              WHERE A.USER_ID = B.USER_ID
##	                AND B.USERTYPE_ID <> '3'
##	                AND B.SIM_CODE <> '1'
##                        AND B.BRAND_ID='3'
##                     )t;"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "$DEC_CHECK_VALUE_1"
##
##	set DEC_CHECK_VALUE_2 [exec get_kpi.sh $this_month_last_day 7 2]
##	set DEC_CHECK_VALUE_3 [exec get_kpi.sh $last_month_last_day 7 2]
##        puts $DEC_CHECK_VALUE_2
##        puts $DEC_CHECK_VALUE_3
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_CHECK_VALUE_1}/(($DEC_CHECK_VALUE_2 + $DEC_CHECK_VALUE_3)/2.0000)]]
##	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /100.0000]]
##	#--��������
##        set DEC_CHECK_VALUE_1 "0"
##        set DEC_CHECK_VALUE_2 "0"
##        set DEC_CHECK_VALUE_3 "0"
##
##        #--��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'50',$DEC_RESULT_VAL1,0,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#--�ж�
##        #--�쳣
##        #--40Ԫ �� ���¶��еش�ARPUֵ �� 150Ԫ ��RMB������
##	if {$DEC_RESULT_VAL1<40 || $DEC_RESULT_VAL1>150 } {
##	           set grade 2
##	           set alarmcontent "׼ȷ��ָ��50�������ſ��˷�Χ"
##	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
##         puts "50 end"
##
##
##
##

	return 0
}
