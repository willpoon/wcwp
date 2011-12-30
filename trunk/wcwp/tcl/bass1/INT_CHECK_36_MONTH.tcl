#################################################################
#��������: INT_CHECK_36_MONTH.tcl
#��������: 
#�����ţ�36
#�������ԣ�ָ���춯
#�������ͣ���
#�������ȣ���
#ָ��ժҪ��36: ��ͻ���������
#����������36: ����������ͻ��� /������������12/����������/ �����û������� �� 3%
#У�����1.BASS1.G_A_02004_DAY  �û�
#          2.BASS1.G_A_02008_DAY  �û�״̬
#          3.BASS1.G_I_02005_MONTH ��ͻ�
#�������:							
#�������: ����ֵ:0 �ɹ�;-1 ʧ��							
#�� д �ˣ�����							
#��дʱ�䣺2007-03-22							
#�����¼��1.							
#�޸���ʷ: 1.							
#################################################################							
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #�������� 
        set this_year_days [GetThisYearDays ${op_month}01]
        #��������
        set this_month_days [GetThisMonthDays ${op_month}01]        
        #��������
        set app_name "INT_CHECK_36_MONTH.tcl"
        
        #--ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
                     and rule_code='36'"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#########################################36: ��ͻ���������################################
        #--����������ͻ���
        set handle [aidb_open $conn]
        set sqlbuf "\
	 SELECT 
	   SUM(P.RC) 
	 FROM 
         (	
    	   SELECT 
             COUNT(USER_ID) AS RC
    	   FROM 
    	     BASS1.G_I_02005_MONTH
    	   WHERE 
    	     TIME_ID=${last_month}
    	     AND USER_ID IN 
    		         (
    		           SELECT
    	 		     A.USER_ID
    		           FROM 
    		             BASS1.G_A_02008_DAY A,
    		             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		              WHERE TIME_ID<= $this_month_last_day
    		              GROUP BY USER_ID
    	                     ) B
    		           WHERE 
    		             A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  
    			     AND A.USERTYPE_ID IN ('2010','2020','2030')
    		          ) 
    	   UNION ALL
    	   SELECT 
             COUNT(*) AS RC
    	   FROM 
    	     ( SELECT USER_ID FROM BASS1.G_I_02005_MONTH WHERE TIME_ID=${op_month}
    	       EXCEPT
    	       SELECT USER_ID FROM BASS1.G_I_02005_MONTH WHERE TIME_ID=${last_month}
    	      )T
    	   WHERE 
    	     USER_ID IN 
    	     	      (
    	     	        SELECT
    	 		  A.USER_ID
    		        FROM 
    		          BASS1.G_A_02008_DAY A,
    		          (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		           WHERE TIME_ID<= $this_month_last_day
    		           GROUP BY USER_ID
    	                  ) B
    		        WHERE 
    		           A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  
    			   AND A.USERTYPE_ID IN ('2010','2020','2030')
    		      )
    	  )P;"
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	aidb_close $handle	  
        
        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/${this_month_days} *12 /${this_year_days})]]
        
        ##--�����û�������
        set DEC_RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
        
        #--����ͳ��У��ֵ
        set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($op_month),'36',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#--�ж�
        #--�쳣��
        #1������������ͻ��� /������������12/����������/ �����û������� 
        
	if { ${DEC_TARGET_VAL1}>0.03 } {
		set grade 2
	        set alarmcontent "׼ȷ��ָ��36����ͻ��������ʳ������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	  }         
	return 0
}
