#################################################################
# ��������: INT_CHECK_SAMPLE_MONTH.tcl
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
#          GBAS.G_S_04008_DAY     --����GSM��ͨ��������             
#          GBAS.G_S_04009_DAY     --������������������              
#          GBAS.G_S_21003_MONTH  --GSM��ͨ����ҵ����ʹ��
#          GBAS.G_S_21006_MONTH  --����������ҵ����ʹ��

#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
##
##|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0
##|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0
##|�����굥�˾���Ե���żƷ���/���������˾���Ե���żƷ��� - 1| �� 8%���ҼƷ�����0
##
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]

	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#���� yyyy-mm
	set opmonth $optime_month	     

        #��������
        set app_name "INT_CHECK_SAMPLE_MONTH.tcl"


#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$op_month
    	              AND RULE_CODE IN ('87','89','91','92','93','H5','H6') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###################################87���˾�ͨ����##########################
        #�����굥�˾�ͨ����
	set handle [aidb_open $conn]
	set sqlbuf "\
                      SELECT 
            	        SUM(T.JB+T.CT+T.CF) AS FY,
            	        COUNT(DISTINCT T.PRODUCT_NO) AS RS
                      FROM (
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(callfw_toll_fee)) AS CF
                             FROM 
                               BASS1.G_S_04008_DAY
                             WHERE TIME_ID/100=$op_month and roam_type_id not in ('122','202','302','401')
                             GROUP BY PRODUCT_NO
                             UNION ALL
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(0)) AS CF
                             FROM 
                               BASS1.G_S_04009_DAY
                             WHERE TIME_ID/100=$op_month
                             GROUP BY PRODUCT_NO
                           )T"
   puts $sqlbuf
	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
		puts $CHECK_VAL1
		puts $CHECK_VAL2

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #��������
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
                   SELECT 
                     SUM(T.JB+T.CT) AS FY,
                     COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE)) AS CT  
                         FROM BASS1.G_S_21003_MONTH
                         WHERE TIME_ID=$op_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_MONTH
                         WHERE TIME_ID=$op_month
                         GROUP BY PRODUCT_NO
                       ) T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'87',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�87��|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��87�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��87�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    }                 
#################################89���˾�����ͨ��ʱ��#############
        #�����굥�˾�����ͨ��ʱ��
#20110805 ȥ����
#                          UNION 
#                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
#                          FROM BASS1.G_S_04009_DAY
#                          WHERE TIME_ID/100=$op_month
#                                AND ADVERSARY_ID<>'010000'
#                          GROUP BY PRODUCT_NO
			  
	set handle [aidb_open $conn]
	set sqlbuf "\
                  SELECT 
            	    SUM(T.SC),
            	    COUNT(DISTINCT T.PRODUCT_NO)
                  FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC, PRODUCT_NO	  
                          FROM BASS1.G_S_04008_DAY
                          WHERE TIME_ID/100=$op_month
                               AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                        )T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #���������˾�����ͨ��ʱ��
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
                  SELECT 
            	    SUM(T.SC),
            	    COUNT(DISTINCT T.PRODUCT_NO)
                  FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM bass1.G_S_21003_MONTH
                          WHERE TIME_ID=$op_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          UNION 
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM bass1.G_S_21006_MONTH
                          WHERE TIME_ID=$op_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                        ) T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'89',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�89��|�����굥�˾�����ͨ��ʱ��/���������˾�����ͨ��ʱ�� - 1| �� 5�������˾�����ͨ��ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��89�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "׼ȷ��ָ��89�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	    
################################91���˾�ͨ��ʱ��###############
        #�����굥�˾�ͨ��ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "\
            SELECT 
            	SUM(T.SC),
            	COUNT(DISTINCT T.PRODUCT_NO)
            FROM (
                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO	  
                  FROM BASS1.G_S_04008_DAY
                  WHERE TIME_ID/100=$op_month  and roam_type_id not in ('122','202','302','401')  
                  GROUP BY PRODUCT_NO
                  UNION 
                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                  FROM BASS1.G_S_04009_DAY
                  WHERE TIME_ID/100=$op_month
                  GROUP BY PRODUCT_NO
                  )T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #���������˾�ͨ��ʱ��
	     set handle [aidb_open $conn]
	     set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)
	            FROM (
	                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  ) T	  "
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'91',
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
             
             
             
             #�ж�91��|�����굥�˾�ͨ��ʱ��/���������˾�ͨ��ʱ�� - 1| �� 5�������˾�ͨ��ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��91�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "׼ȷ��ָ��91�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	    
############################92���˾�����ͨ������#####################
        #�����굥�˾�����ͨ������
	set handle [aidb_open $conn]
	set sqlbuf "\
            SELECT 
            	SUM(T.CS),
            	COUNT(DISTINCT T.PRODUCT_NO)
            FROM (
                  SELECT COUNT(*) AS CS , PRODUCT_NO		  
                  FROM BASS1.G_S_04008_DAY
                  WHERE TIME_ID/100=$op_month
                        AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                  GROUP BY PRODUCT_NO
                  UNION 
                  SELECT COUNT(*) AS CS, PRODUCT_NO 
                  FROM BASS1.G_S_04009_DAY
                  WHERE TIME_ID/100=$op_month
                       AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                  GROUP BY PRODUCT_NO
                  )T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             
             #���������˾�����ͨ������
	set handle [aidb_open $conn]
        set sqlbuf "\
                SELECT 
            	   SUM(T.CS),
            	   COUNT(DISTINCT T.PRODUCT_NO)
                FROM (
                        SELECT SUM(BIGINT(CALL_COUNTS)) AS CS,PRODUCT_NO	  
                        FROM BASS1.G_S_21003_MONTH
                        WHERE TIME_ID=$op_month
                              AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                        GROUP BY PRODUCT_NO
                        UNION 
                        SELECT SUM(BIGINT(CALL_COUNTS)) AS CS,PRODUCT_NO	  
                        FROM BASS1.G_S_21006_MONTH
                        WHERE TIME_ID=$op_month
                              AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                        GROUP BY PRODUCT_NO
                      )T"	     
             puts $sqlbuf
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
	      
	      puts $CHECK_VAL1
	      puts $CHECK_VAL2
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'92',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�92��|�����굥�˾�����ͨ������/���������˾�����ͨ������ - 1| �� 5�������˾�����ͨ��������0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��92�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "׼ȷ��ָ��92�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	  	    	  
###########################93������ͨ���ѵ���#################	   
        #�����굥����ͨ���ѵ���
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.FY),
	            	SUM(T.SC)
	            FROM (
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC ,
	                    SUM(BIGINT(BASE_CALL_FEE)) AS FY		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month  and roam_type_id not in ('122','202','302','401')  
	                  UNION ALL
	                  SELECT
	                    SUM(CEIL(BIGINT(CALL_DURATION)/60.0))AS SC, 
	                    SUM(BIGINT(BASE_CALL_FEE)) AS FY
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
	                  )T"
             puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #�������ݻ���ͨ���ѵ���
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
	            SELECT 
	            	SUM(T.FY),
	            	SUM(T.SC)
	            FROM (
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC,
	                    SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS FY	  
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  UNION ALL
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC,
	                    SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS FY	  
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
	                  )T"
             puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ-����ֵ)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr ($RESULT_VAL1-$RESULT_VAL2)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'93',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #�ж�93��|�����굥����ͨ���ѵ��ۣ��������ݻ���ͨ���ѵ���| �� 0.03���һ���ͨ���ѵ��ۣ�0��Ԫ/���ӣ�����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.03 ||$RESULT<-0.03 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��93�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.02 ||$RESULT<-0.02 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��93�ӽ����ſ��˷�Χ0.03,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 
#############################H5���˾��Ʒ�ʱ��#################
        #�����굥�˾��Ʒ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)AS RS
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
	                  GROUP BY PRODUCT_NO
	                  )T"
            puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #���������˾��Ʒ�ʱ��
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO) 
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC	  
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  )T"
              puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'H5',
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
             
             
             
             #�ж�H5��|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��H5�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��H5�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	  
###########################################H6���˾���;�Ʒ�ʱ��##########
        #�����굥�˾���;�Ʒ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)
	            FROM (
	                  SELECT SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO	  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month
	                        AND TOLL_TYPE_ID<>'010'
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
	                        AND TOLL_TYPE_ID<>'010'
	                  GROUP BY PRODUCT_NO
	                  )T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #���������˾���;�Ʒ�ʱ��
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
		            SELECT 
		            	SUM(T.SC),
		            	COUNT(DISTINCT T.PRODUCT_NO)
		            FROM (
		                  SELECT  SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO
		                  FROM BASS1.G_S_21003_MONTH
		                  WHERE TIME_ID=$op_month
		                        AND TOLL_TYPE_ID<>'010'
		                  GROUP BY PRODUCT_NO
		                  UNION 
		                  SELECT SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO
		                  FROM BASS1.G_S_21006_MONTH
		                  WHERE TIME_ID=$op_month
		                        AND TOLL_TYPE_ID<>'010'
		                  GROUP BY PRODUCT_NO
		                  )T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'H6',
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
             
             
             
             #�ж�H6��|�����굥�˾���;�Ʒ�ʱ��/���������˾���;�Ʒ�ʱ�� - 1| �� 5�������˾���;�Ʒ�ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��H6�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "׼ȷ��ָ��H6�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    }           
##################################END#######################
	return 0
}	    	    