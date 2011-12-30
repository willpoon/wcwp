#################################################################
#��������: INT_CHECK_E5_MONTH.tcl
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
#
#�����ţ�E5
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��E5: WAP�����û���ռ��
#����������E5: WAP�����û���/��ҵ�񸶷��û���>0����ռ�Ƚ����±䶯��<30%
#У�����
#          BASS1.G_S_04006_DAY     ����WAP����
#          BASS1.G_A_02004_DAY     �û�
#          BASS1.G_S_03004_MONTH   ��ϸ�ʵ�
#          BASS1.G_S_03012_MONTH   �������û���ϸ����
#�������:
#�� �� ֵ:   0 �ɹ�; -1 ʧ��
#�޸�:   ָ�꿼�˲���ȷ
#        �� if { $RESULT_VAL1<=0 || $RESULT<0.30 ||$RESULT>-0.30 } ��Ϊ
#  	     if { $RESULT_VAL1<=0 || $RESULT<-0.30||$RESULT>0.30  }
#
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #���µ�һ�� yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #�������һ�� yyyymmdd
        set last_month [GetLastMonth [string range $op_month 0 5]]
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]
        #��������
        set app_name "INT_CHECK_E5_MONTH.tcl"
        
#####ɾ����������
####        set handle [aidb_open $conn]
####	set sql_buff "\
####		delete from bass1.g_rule_check where time_id=$op_month
####    	              and rule_code='E5' "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####
####        #����ռ�ȳ�100	
####        #WAP�����û���
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####	           SELECT 
####             	     COUNT(DISTINCT A.PRODUCT_NO)
####                   FROM  bass1.G_S_04006_DAY A, 
####                   (
####             	     SELECT A.TIME_ID,A.PRODUCT_NO,A.USERTYPE_ID
####                     FROM bass1.G_A_02004_DAY A,
####                          (    
####            		    SELECT PRODUCT_NO,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
####                            WHERE TIME_ID <=$this_month_last_day
####                            GROUP BY PRODUCT_NO
####            		  ) B
####                     WHERE A.PRODUCT_NO = B.PRODUCT_NO
####                        AND A.TIME_ID = B.TIME_ID
####             	   )B
####            	   WHERE A.TIME_ID/100=$op_month
####           	        AND A.PRODUCT_NO=B.PRODUCT_NO 
####            	        AND BIGINT(A.WAP_BASEFEE)+BIGINT(A.INFO_FEE)+BIGINT(A.MONTH_FEE)>0
####            	        AND B.USERTYPE_ID <> '3'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "����WAP�����û���=$CHECK_VAL1"
####       #��ҵ�񸶷��û���
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####                   SELECT
####                     COUNT(DISTINCT A.USER_ID)
####                   FROM 
####                   (
####             	    SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
####             	    WHERE BIGINT(A.FEE_RECEIVABLE)>0
####             	        AND A.TIME_ID =$op_month
####             	        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####             	    UNION ALL
####                    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
####	            WHERE A.TIME_ID =$op_month
####			AND BIGINT(A.INCM_AMT)>0
####			AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####	           ) A, 
####	            
####                  (
####             	   SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
####                   FROM BASS1.G_A_02004_DAY A,
####                   (    
####            	      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
####                      WHERE TIME_ID <=$this_month_last_day
####                      GROUP BY USER_ID
####                   ) B
####                   WHERE A.USER_ID = B.USER_ID
####                    AND A.TIME_ID = B.TIME_ID
####                 )B
####                 WHERE A.USER_ID = B.USER_ID
####            	    AND B.USERTYPE_ID <> '3'
####            	    AND B.SIM_CODE <> '1'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "������ҵ�񸶷��û���=$CHECK_VAL2"
####	    set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.0000/${CHECK_VAL2}*100.00000)]]
####	
####           #��������
####           set CHECK_VAL1 0.00
####           set CHECK_VAL2 0.00
####
####        #����ռ�ȳ�100	
####        #WAP�����û���
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####	           SELECT 
####             	     COUNT(DISTINCT A.PRODUCT_NO)
####                   FROM  bass1.G_S_04006_DAY A, 
####                   (
####             	     SELECT A.TIME_ID,A.PRODUCT_NO,A.USERTYPE_ID
####                     FROM bass1.G_A_02004_DAY A,
####                          (    
####            		    SELECT PRODUCT_NO,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
####                            WHERE TIME_ID <=$last_month_last_day
####                            GROUP BY PRODUCT_NO
####            		  ) B
####                     WHERE A.PRODUCT_NO = B.PRODUCT_NO
####                        AND A.TIME_ID = B.TIME_ID
####             	   )B
####            	   WHERE A.TIME_ID/100=$last_month
####           	        AND A.PRODUCT_NO=B.PRODUCT_NO 
####            	        AND BIGINT(A.WAP_BASEFEE)+BIGINT(A.INFO_FEE)+BIGINT(A.MONTH_FEE)>0
####            	        AND B.USERTYPE_ID <> '3'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	      puts "����WAP�����û���=$CHECK_VAL1"
####       #��ҵ�񸶷��û���
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####                   SELECT
####                     COUNT(DISTINCT A.USER_ID)
####                   FROM 
####                   (
####             	    SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
####             	    WHERE BIGINT(A.FEE_RECEIVABLE)>0
####             	        AND A.TIME_ID =$last_month
####             	        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####             	    UNION ALL
####                    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
####	            WHERE A.TIME_ID =$last_month
####			AND BIGINT(A.INCM_AMT)>0
####			AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####	           ) A, 
####	            
####                  (
####             	   SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
####                   FROM BASS1.G_A_02004_DAY A,
####                   (    
####            	      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
####                      WHERE TIME_ID <=$last_month_last_day
####                      GROUP BY USER_ID
####                   ) B
####                   WHERE A.USER_ID = B.USER_ID
####                    AND A.TIME_ID = B.TIME_ID
####                 )B
####                 WHERE A.USER_ID = B.USER_ID
####            	    AND B.USERTYPE_ID <> '3'
####            	    AND B.SIM_CODE <> '1'"
####              puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "������ҵ�񸶷��û���=$CHECK_VAL2"
####	    set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2}*100.00000)]]
####
####             #��У��ֵ����У������
####             set handle [aidb_open $conn]
####             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
####             		($op_month,
####             		'E5',
####             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
####             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
####             		0.00,
####             		0.00)"
####            puts $sqlbuf
####             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
####             	WriteTrace $errmsg 003
####             	return -1
####             }
####             aidb_commit $conn
####             aidb_close $handle	
####             
####             set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1.00000)]]
####             puts $RESULT
####             
####             #�ж�E5��WAP�����û���/��ҵ�񸶷��û���>0����ռ�Ƚ����±䶯��<30%����
####	     if { $RESULT_VAL1<=0 || $RESULT<-0.30||$RESULT>0.30  } {	
####	     	puts '1111'
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��E5�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}  
####	        puts '222'  		
####	     }            
######################################END#######################
	return 0
}	    	     