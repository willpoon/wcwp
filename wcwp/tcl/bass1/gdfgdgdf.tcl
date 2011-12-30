#################################################################
# ��������: INT_CHECK_SAMPLE_TO_DAY.tcl
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
#�����ţ�87,89,91,92,93,H5,H6
#�������ԣ�������֤
#�������ͣ���
#ָ��ժҪ��87���˾�ͨ����
#          89���˾�����ͨ��ʱ��
#          91���˾�ͨ��ʱ��
#          92���˾�����ͨ������
#          93������ͨ���ѵ���
#          H5���˾��Ʒ�ʱ��
#          H6���˾���;�Ʒ�ʱ��
#����������87��|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0
#          89��|�����굥�˾�����ͨ��ʱ��/���������˾�����ͨ��ʱ�� - 1| �� 5�������˾�����ͨ��ʱ����0
#          91��|�����굥�˾�ͨ��ʱ��/���������˾�ͨ��ʱ�� - 1| �� 5�������˾�ͨ��ʱ����0
#          92��|�����굥�˾�����ͨ������/���������˾�����ͨ������ - 1| �� 5�������˾�����ͨ��������0
#          93��|�����굥����ͨ���ѵ��ۣ��������ݻ���ͨ���ѵ���| �� 0.03���һ���ͨ���ѵ��ۣ�0��Ԫ/���ӣ�
#          H5��|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0
#          H6��|�����굥�˾���;�Ʒ�ʱ��/���������˾���;�Ʒ�ʱ�� - 1| �� 5�������˾���;�Ʒ�ʱ����0
#У�����
#          BASS1.G_S_04008_DAY     ����GSM��ͨ��������
#          BASS1.G_S_04009_DAY     ������������������
#          BASS1.G_S_21003_TO_DAY  GSM��ͨ����ҵ����ʹ��(��ʹ��)
#          BASS1.G_S_21006_TO_DAY  ����������ҵ����ʹ��(��ʹ��)
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
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
        set app_name "INT_CHECK_SAMPLE_TO_DAY.tcl"


#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE IN ('87','89','91','92','93','H5','H6') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
#################################87:�˾�ͨ����#########################
        if {$today_dd>9 } {
#�����굥�˾�ͨ����
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	      SUM(T.JB+T.CT) AS FY,
            	      COUNT(DISTINCT T.PRODUCT_NO) AS RS
                    FROM (
                           SELECT PRODUCT_NO,SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                           		 SUM(BIGINT(TOLL_CALL_FEE)) AS CT  
                           FROM BASS1.G_S_04008_DAY
                           WHERE TIME_ID/100=$this_month
                           GROUP BY PRODUCT_NO
                           UNION 
                           SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS JB,SUM(BIGINT(TOLL_BILL_DURATION))AS CT
                           FROM BASS1.G_S_04009_DAY
                           WHERE TIME_ID/100=$this_month
                           GROUP BY PRODUCT_NO
                         )T"

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
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1  
	     
#��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00      	

#��������
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	      SUM(T.JB+T.CT) AS FY,
            	      COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE)) AS CT  
                         FROM BASS1.G_S_21003_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                         ) T"    

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

	     set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2  
	     
#��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00   	     
             
             set RESULT [format "%.5f" [expr (($RESULT_VAL1/1.0000/$RESULT_VAL2)-1.0000)]]
             puts $RESULT

             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'87',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�87��|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0����
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {	
	     	set handle [aidb_open $conn]
	     	set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		2,
	       		'�۷���ָ��87�������ſ��˷�Χ',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		return -1
	         }
	        aidb_commit $conn
	        aidb_close $handle	     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set handle [aidb_open $conn]
	     	   set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		3,
	       		'�۷���ָ��87�ӽ����ſ��˷�Χ',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		 return -1
	     	    }
	     	   aidb_commit $conn
	           aidb_close $handle	
	    }

##################################89���˾�����ͨ��ʱ��#################
#�����굥�˾�����ͨ��ʱ��   
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	     SUM(T.SC),
            	     COUNT(DISTINCT T.PRODUCT_NO)
                   FROM (
                         SELECT SUM(BIGINT(CALL_DURATION)) AS SC, PRODUCT_NO	  
                         FROM bass1.G_S_04008_DAY
                         WHERE TIME_ID/100=$this_month
                              AND ADVERSARY_ID<>'010000'
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                         FROM bass1.G_S_04009_DAY
                         WHERE TIME_ID/100=$this_month
                               AND ADVERSARY_ID<>'010000'
                         GROUP BY PRODUCT_NO
                         )T"

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
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1  
	     
#��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00      	

#��������
	     set handle [aidb_open $conn]
	     set sqlbuf "
                  SELECT 
            	      SUM(T.SC),
            	      COUNT(DISTINCT T.PRODUCT_NO)
                    FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM BASS1.G_S_21003_TO_DAY
                          WHERE TIME_ID/100=$this_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          UNION 
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM BASS1.G_S_21006_TO_DAY
                          WHERE TIME_ID/100=$this_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          ) T"    

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

	     set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2  
	     
#��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00   	     
             
             set RESULT [format "%.5f" [expr (($RESULT_VAL1/1.0000/$RESULT_VAL2)-1.0000)]]
             puts $RESULT

             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'89',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�89��|�����굥�˾�����ͨ��ʱ��/���������˾�����ͨ��ʱ�� - 1| �� 5�������˾�����ͨ��ʱ����0����
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {	
	     	set handle [aidb_open $conn]
	     	set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		2,
	       		'׼ȷ��ָ��89�������ſ��˷�Χ',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		return -1
	         }
	        aidb_commit $conn
	        aidb_close $handle		     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set handle [aidb_open $conn]
	     	   set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		3,
	       		'׼ȷ��ָ��89�ӽ����ſ��˷�Χ',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		 return -1
	     	    }
	     	   aidb_commit $conn
	           aidb_close $handle
	    }    
	           	
        }
##################################END#######################
	return 0
}	    
