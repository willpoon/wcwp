######################################################################################################
#�������ƣ�INT_CHECK_VOICE_MONTH.tcl
#У��ӿڣ�R092 R093 R094
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-06-23
#�����¼��
#�޸���ʷ: liuqf 20090805 �޸�����ָ��ץȡ�ھ�
#          liuzhilong 20091003  �޸�R109У���߼� �������� CDR_TYPE IN ('00','10','21')   
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $last_month
        
        #��Ȼ�µ�һ�� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        puts $timestamp
        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        puts $l_timestamp
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #������
        set app_name "INT_CHECK_VOICE_MONTH.tcl"

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #�������һ��#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #�������һ�� yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day


	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month 
	                    and rule_code in ('R092','R093','R094','R097','R109','R108','R107','R089','R090','R091')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



   #--R092 |��(����ÿ��GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ� - 1|��1��
   
   set sqlbuf "select sum(bigint(BASE_BILL_DURATION))+sum(bigint(TOLL_BILL_DURATION))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(BASE_BILL_DURATION))+sum(bigint(TOLL_BILL_DURATION))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R092',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R092 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|��(����ÿ��GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õļƷ�ʱ���ϼ� - 1|��1��"




   #--R093 |��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1��
   
   set sqlbuf "select sum(bigint(CALL_DURATION))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur
               "
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(CALL_DURATION))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
   
   puts $sqlbuf         
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R093',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R093 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ�)/����GSM��ͨ����ҵ����ʹ�õ�ͨ��ʱ���ϼ� - 1|��1��"



   #--R094 |��(����ÿ��GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ�)/����GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ� - 1|��1��
   
   set sqlbuf "select sum(bigint(FAVOURED_CALL_FEE))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur"
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(FAVOURED_CALL_FEE))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
   
   puts $sqlbuf             
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R094',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R094 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|��(����ÿ��GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ�)/����GSM��ͨ����ҵ����ʹ�õ��ܷ��úϼ� - 1|��1��"



   #--R097 |��(����ÿ����ͨ����ҵ����ʹ�õ�ͨ�����ϼ�)/������ͨ����ҵ����ʹ�õ�ͨ�����ϼ� - 1|��1��
   
   set sqlbuf "select sum(bigint(SMS_COUNTS))
               from BASS1.G_S_21007_DAY
               where time_id/100=$op_month
                with ur  "
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(SMS_COUNTS))
               from BASS1.G_S_21008_MONTH 
               where time_id=$op_month
               with ur"
   puts $sqlbuf             
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R097',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R097 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|��(����ÿ����ͨ����ҵ����ʹ�õ�ͨ�����ϼ�)/������ͨ����ҵ����ʹ�õ�ͨ�����ϼ� - 1|��1��"




#############################R109���˾���Ե�Ʒ���#################
        #�����굥�˾��Ʒ�ʱ��
	set handle [aidb_open $conn]
	set sqlbuf "\
	            select count(*),
	                   count(distinct product_no)
              from BASS1.G_S_04011_DAY
              where time_id/100=$op_month
                and svc_type in ('11','12','13')
                AND CDR_TYPE IN ('00','10','21')
		         	   AND SMS_STATUS='0'
               with ur "
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
	            select sum(bigint(SMS_COUNTS)),
	                        count(distinct product_no)
                   from BASS1.G_S_21008_MONTH
                    where time_id=$op_month
                     and END_STATUS='0' 
                     and svc_type_id in ('11','12','13')
                     AND CDR_TYPE_ID IN ('00','10','21') 
                   with ur"
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
             		'R109',
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
             
             
             
             #�ж�R109��|�����굥�˾���Ե���żƷ���/���������˾���Ե���żƷ��� - 1| �� 5%���ҼƷ�����0
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��R109�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��R109�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $optime $grade ${alarmcontent}
	          
	    } 	  


#############################R108���˾��Ʒ�ʱ��#################
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
	                  FROM BASS1.G_S_21003_TO_DAY
	                  WHERE TIME_ID/100=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_TO_DAY
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
             		'R108',
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
             
             
             
             #�ж�R139��|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��R108�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } 
	     
##	     elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
##	           set grade 3
##	           set alarmcontent "�۷���ָ��R108�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
##	           WriteAlarm $app_name $optime $grade ${alarmcontent}
##	          
##	    } 	  


###################################R107���˾�ͨ����##########################

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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	     puts $CHECK_VAL2
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
                         FROM BASS1.G_S_21003_TO_DAY
                         WHERE TIME_ID/100=$op_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_TO_DAY
                         WHERE TIME_ID/100=$op_month
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
             		'R107',
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
             
             
             
             #�ж�R107��|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��R107�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��R107�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $optime $grade ${alarmcontent}
	          
	    }                 
	    
	    
	    
	    
#add by zhanght on 20090708

	 #--DEC_CHECK_VALUE_11����������Ʒ�ƺϼ�����ͨ����,DEC_CHECK_VALUE_12����������Ʒ�ƺϼ�����ͨ����
         #--������Ʒ�ƺϼ�����ͨ����
         set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(T.FY)
   	         FROM 
   	         (
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21003_TO_DAY
   	            WHERE TIME_ID/100=${op_month}
   	            UNION
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21006_TO_DAY
   	            WHERE TIME_ID/100=${op_month}
   	         )T"
  puts $sqlbuf

  if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_11 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}


#############
	 #--DEC_CHECK_VALUE_99����������Ʒ�ƺϼ�����ͨ����
         #--������Ʒ�ƺϼ�����ͨ����
         set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(T.FY)
   	         FROM 
   	         (
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21003_TO_DAY
   	            WHERE TIME_ID/100=${last_month}
   	            UNION
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21006_TO_DAY
   	            WHERE TIME_ID/100=${last_month}
   	         )T"
  puts $sqlbuf

  if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_99 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}


	aidb_commit $conn


	#--R089: ȫ��ͨ����ͨ����ռ��
        #--����ռ�ȳ�100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_month
   	       WHERE TIME_ID=${op_month}
   	             AND BRAND_ID='1'"
   puts $sqlbuf

   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       
       puts "------------------------R089-----------------------------"
       puts "------------------------1-----------------------------"
       puts $DEC_RESULT_VAL1
       #--��������
       set DEC_CHECK_VALUE_1 "0"
       #--����ռ�ȳ�100
        set handle [aidb_open $conn]
###        set sqlbuf "SELECT TARGET1 
###   	         FROM BASS1.G_RULE_CHECK 
###   	         WHERE TIME_ID=${last_month_day}
###                   AND RULE_CODE='R089'"

      set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
 	       FROM BASS1.G_S_21003_month
 	      WHERE TIME_ID=${last_month}
 	        AND BRAND_ID='1'"

  puts $sqlbuf

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	
	aidb_commit $conn
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]

  puts "------------------------2-----------------------------"
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}
		
  puts "------------------------R089--result-----------------------------"
	puts ${DEC_TARGET_VAL1}
	
##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R089'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	
	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R089',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
  puts $sql_buff
  
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--�ж�
        #--�쳣��
#R089: ���±����±䶯�ʣ�ȫ��ͨ����ͨ����/�ϼ�����ͨ���ѣ��� 10%����15%��ռ�ȡ�40%

	
#add by zhanght on 20090623 ȫ��ͨ����ͨ����ռ�ȱ䶯�ʡ�10%	
		if {${DEC_TARGET_VAL1}>0.10} {
		set grade 2
	  set alarmcontent "׼ȷ��ָ��R089�������ſ��˷�Χ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	  } 

	
	puts "R089 finish"
	#----------------------------------------------------------
	#--R090: ����������ͨ����ռ��
        #--����ռ�ȳ�100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_TO_DAY
   	       WHERE TIME_ID/100=${op_month}
   	             AND BRAND_ID='2'"
  puts $sqlbuf

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       #--��������
       set DEC_CHECK_VALUE_1 "0"
       #--����ռ�ȳ�100
        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R090'"  

    set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
       FROM BASS1.G_S_21003_TO_DAY
      WHERE TIME_ID/100=${last_month}
        AND BRAND_ID='2'"

  puts $sqlbuf

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]
	
	#set DEC_RESULT_VAL2 "100"
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}

	puts ${DEC_TARGET_VAL1}

##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R090'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R090',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--�ж�
        #--�쳣��
#R090: ���±����±䶯�ʣ�����������ͨ����/�ϼ�����ͨ���ѣ��� 8%����35%��ռ�ȡ�65%
        

#add by zhanght on 20090623	 	����������ͨ����ռ�ȱ䶯�ʡ�10%
	 	if { ${DEC_TARGET_VAL1}>0.10 } {
		set grade 2
	        set alarmcontent "׼ȷ��ָ��R090�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	        } 
	 	
	 	
     puts "R090 finish"
     #------------------------------------------------------------------
     #--R091: ���еش�����ͨ����ռ��
   #--����ռ�ȳ�100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_TO_DAY
   	       WHERE TIME_ID/100=${op_month}
   	             AND BRAND_ID='3'"
  puts $sqlbuf

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       #--��������
       set DEC_CHECK_VALUE_1 "0"
       #--����ռ�ȳ�100
        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R091'"

    set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
       FROM BASS1.G_S_21003_TO_DAY
      WHERE TIME_ID/100=${last_month}
        AND BRAND_ID='3'"

  puts $sqlbuf

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	#set DEC_RESULT_VAL2 "100"
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]
	
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}

	puts ${DEC_TARGET_VAL1}
	
##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $timestamp and rule_code = 'R091'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R091',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--�ж�
        #--�쳣��
 #R091: ���±����±䶯�ʣ����еش�����ͨ����/�ϼ�����ͨ���ѣ��� 10%����3<ռ��<18%
       

	 	
#add by zhanght on 20090623 ���еش�����ͨ����ռ�ȱ䶯�ʡ�12%	 	
	if { ${DEC_TARGET_VAL1}>0.12 } {
		set grade 2
	        set alarmcontent "׼ȷ��ָ��R091�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	        } 

	 	
	puts "R091 finish"

  aidb_close $handle



		
	return 0
}

#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle

	
	return $result
}