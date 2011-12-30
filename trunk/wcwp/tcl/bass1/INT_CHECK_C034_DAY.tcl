#################################################################
#��������: INT_CHECK_C034_DAY.tcl
#��������: 
#�����ţ�C0,C3,C4,V6016
#�������ԣ�C0:ҵ���߼�;C3,C4:������֤
#�������ͣ���
#ָ��ժҪ��C0��æʱ�Ʒ�ʱ��ռ��
#          C3����������ʱ�μƷ�ʱ��/�������ջ��ܼƷ�ʱ��
#          C4�����еش���ʱ�μƷ�ʱ��/���еش��ջ��ܼƷ�ʱ��
#����������C0���������½��æʱ�Ʒ�ʱ��ռ�ȡ��������Ͻ磬
#              ����������߽��ɵ��ո�ʡæʱ�Ʒ�ʱ��ռ�ȵ�ͳ�ƾ�ֵ�ͱ�׼�ͬȷ����
#              �����Ͻ�=��ֵ+3*��׼������½�=��ֵ-3*��׼��
#          C3��|��������ʱ�μƷ�ʱ��/�������ջ��ܼƷ�ʱ�� - 1| �� 0.1%
#          C4��|���еش���ʱ�μƷ�ʱ��/���еش��ջ��ܼƷ�ʱ�� - 1| �� 0.1%
#У�����
#          1.GBAS.G_S_21001_DAY   GSM��ͨ����ҵ����ʹ��           
#          2.GBAS.G_S_21002_DAY   GSM��ͨ����ҵ����ʱ��ʹ��
#          3.GBAS.G_S_21004_DAY   ����������ҵ����ʹ��            (Ŀǰ�Ϳ�)
#          4.GBAS.G_S_21005_DAY   ����������ҵ����ʱ��ʹ��        (Ŀǰ�Ϳ�)
#          5.GBAS.G_S_21009_DAY   ������VPMN����ҵ����ʹ��      
#          6.GBAS.G_S_21016_DAY   ������VPMN����ҵ����ʱ��ʹ��  
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1. V6016 ����GSM��ͨ����ҵ����ʹ�õ�У�� zhanght 20090616
#�޸���ʷ: 1.�ж�C3��C4����2ֵ�Աȵ�ʱ����С��λ�٣�Ӧ�ñ���3λС������ ���� 2008-01-17
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #��������
        set app_name "INT_CHECK_C034_DAY.tcl"

        #--ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$timestamp
		and rule_code in('C0','C3','C4') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


###################################C0��æʱ�Ʒ�ʱ��ռ��#############################
#        #--æʱ�Ʒ�ʱ��
#        set handle [aidb_open $conn]
#	set sqlbuf "\
#	       SELECT 
#	         SUM(P.FEE)
#	       FROM
#	       (
#	    	 SELECT 
#	    	   T.CALL_MOMENT_ID,
#	    	   SUM(T.SC) AS FEE
#	    	 FROM
#	    	 (
#	    	   SELECT CALL_MOMENT_ID ,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21002_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALL_MOMENT_ID
#	    	   UNION ALL
#	    	   SELECT CALL_MOMENT_ID ,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21005_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALL_MOMENT_ID
#	    	   UNION ALL
#	    	   SELECT CALLMOMENT_ID as CALL_MOMENT_ID,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21016_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALLMOMENT_ID
#	    	  )T
#	    	  GROUP BY T.CALL_MOMENT_ID
#	    	  ORDER BY 2 DESC
#	    	  FETCH FIRST 2 ROWS ONLY
#	    	) P"
#
#         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#	   	WriteTrace $errmsg 1001
#	   	return -1
#	 }      
#	 if [catch {set BUSI_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
#	   	WriteTrace $errmsg 1002
#	   	return -1
#	 }        
#	aidb_commit $conn
#	set BUSI_DURATION [format "%.2f" [expr ${BUSI_DURATION} /1.00]]
#	#puts $BUSI_DURATION
#	
#	#--�����Ʒ�ʱ��
#	set handle [aidb_open $conn]
#	set sqlbuf "\
#	    SELECT SUM(T.SC)
#	   	FROM
#	   	(
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21001_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   		UNION ALL
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21004_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   		UNION ALL
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21009_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   	)T"
#        
#          if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#	   	WriteTrace $errmsg 1001
#	   	return -1
#	    }
#        
#	    if [catch {set BASE_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
#	   	WriteTrace $errmsg 1002
#	   	return -1
#	   }        
#	   aidb_commit $conn
#	   set BASE_DURATION [format "%.2f" [expr ${BASE_DURATION}/1.00]]
#	   #puts $BASE_DURATION
#	
#	   #--��У��ֵ����У������
#	   set handle [aidb_open $conn]
#	   set sql_buff "\
#	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C0',$BUSI_DURATION,$BASE_DURATION,0,0) "
#	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#	   	WriteTrace "$errmsg" 2005
#	   	aidb_close $handle
#	   	return -1
#	   }
#	   aidb_commit $conn
#
#	    #--�ж�
#        #--�쳣
#        #--1���������½��æʱ�Ʒ�ʱ��ռ�ȡ��������Ͻ糬��
#        #--���ݾ���,�����ݶ�������Ϊ17%(2006/12/30���۽�����ֵ��Ϊ16%)
#         if {[format "%.2f" [expr ${BUSI_DURATION} / ${BASE_DURATION}]]>0.16 || [format "%.2f" [expr ${BUSI_DURATION} / ${BASE_DURATION} ]]<-0.16} {
#	  	set grade 2
#	        set alarmcontent "�۷���ָ��C0�������ſ��˷�Χ"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#	 }
#
#         puts "C0����"


################################### V6016 GSM��ͨ����ҵ����ʹ��  #############################

  set handle [aidb_open $conn]
	set sql_buff "\
	           select  value(count(*),0)
             from BASS1.G_S_21001_DAY
             where time_id=$timestamp
             and ROAM_TYPE_ID='500'
             and CALL_TYPE_ID='02'
             and 
             (int(TOLL_BILL_DURATION)<>0
             or int(TOLL_CALL_FEE)<>0
             or int(FAVOURED_TOLLCALL_FEE)<>0)
             with ur "


     puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	
	puts $DEC_RESULT_VAL1
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'V6016',$DEC_RESULT_VAL1,0,0,0) "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]] > 0 } {
		set grade 2
	        set alarmcontent "  V6016 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
##################################################################################################################################################    



#################################C3����������ʱ�μƷ�ʱ��/�������ջ��ܼƷ�ʱ��###########
       #--��������ʱ�μƷ�ʱ��
       set handle [aidb_open $conn]
       set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT
              	value(SUM(BIGINT(BASE_BILL_DURATION )),0) AS SC
              FROM BASS1.G_S_21004_DAY
              WHERE TIME_ID=$timestamp
                     AND BRAND_ID ='2'
              UNION
              SELECT
              	value(SUM(BIGINT(BASE_BILL_DURATION )),0) AS SC
              FROM BASS1.G_S_21001_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID ='2'
             )T"

         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	 }

	 if [catch {set SZXTIME_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn

	 set SZXTIME_DURATION [format "%.2f" [expr ${SZXTIME_DURATION} /1.00]]
	 puts $SZXTIME_DURATION

	 #----�������ջ��ܼƷ�ʱ��
	 set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
               SELECT 
               	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
               FROM BASS1.G_S_21005_DAY
               WHERE TIME_ID=$timestamp
                     AND BRAND_ID='2'
               UNION
               SELECT 
               	value(SUM(BIGINT(BASE_BILL_DURATION)),0)  AS SC
               FROM BASS1.G_S_21002_DAY
               WHERE TIME_ID=$timestamp
                     AND BRAND_ID='2'
             )T ;"
     
     puts $
     if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	  }

	  if [catch {set SZXALL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set SZXALL_DURATION [format "%.2f" [expr ${SZXALL_DURATION} /1.00]]
	 #puts $SZXALL_DURATION

	 #--��У��ֵ����У������
	 set handle [aidb_open $conn]
	 set sql_buff "\
	 	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C3',$SZXTIME_DURATION,$SZXALL_DURATION,0,0) "
	 if [catch { aidb_sql $handle $sql_buff } errmsg ] {
	 	WriteTrace "$errmsg" 2005
	 	aidb_close $handle
	 	return -1
	 }
	 aidb_commit $conn

	#--�ж�
        #--�쳣
        #--1��|��������ʱ�μƷ�ʱ��/�������ջ��ܼƷ�ʱ�� - 1|<=0.1%����
         if {[format "%.3f" [expr ${SZXTIME_DURATION} / ${SZXALL_DURATION} -1 ]]>0.001 || [format "%.3f" [expr ${SZXTIME_DURATION} / ${SZXALL_DURATION} - 1]]<-0.001} {
		set grade 2
	        set alarmcontent "�۷���ָ��C3�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
			  	}
	  	
	puts "C3����"
	#--------------------------------------------------------------  	
	  	
	   #--C4�����еش���ʱ�μƷ�ʱ��/���еش��ջ��ܼƷ�ʱ��
           #--���еش���ʱ�μƷ�ʱ��
          set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21004_DAY
              WHERE TIME_ID=$timestamp 
                     AND BRAND_ID='3'
              UNION
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21001_DAY
              WHERE TIME_ID=$timestamp 
                    AND BRAND_ID='3'
            ) T"
     
     puts $sqlbuf
     if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	 }

	 if [catch {set MZONETIME_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set MZONETIME_DURATION [format "%.2f" [expr ${MZONETIME_DURATION} /1.00]]
	 #puts $MZONETIME_DURATION

	 #----���еش��ջ��ܼƷ�ʱ��
	 set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21005_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID='3'
              UNION
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21002_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID='3'
             )T"

         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	  }

	  if [catch {set MZONEALL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set MZONEALL_DURATION [format "%.2f" [expr ${MZONEALL_DURATION} /1.00]]
	 #puts $MZONEALL_DURATION

	 #--��У��ֵ����У������
	 set handle [aidb_open $conn]
	 set sql_buff "\
	 	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C4',$MZONETIME_DURATION,$MZONEALL_DURATION,0,0) "
	 if [catch { aidb_sql $handle $sql_buff } errmsg ] {
	 	WriteTrace "$errmsg" 2005
	 	aidb_close $handle
	 	return -1
	 }
	 aidb_commit $conn

	#--�ж�
        #--�쳣
        #--1��|���еش���ʱ�μƷ�ʱ��/���еش��ջ��ܼƷ�ʱ�� - 1|<=0.1%����
         if {[format "%.3f" [expr ${MZONETIME_DURATION} / ${MZONEALL_DURATION} -1]]>0.001 || [format "%.3f" [expr ${MZONETIME_DURATION} / ${MZONEALL_DURATION} - 1]]<-0.001} {
		set grade 2
	        set alarmcontent "�۷���ָ��C4�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
		 	}
	  	puts "C4����"	

	aidb_close $handle
	return 0
}

