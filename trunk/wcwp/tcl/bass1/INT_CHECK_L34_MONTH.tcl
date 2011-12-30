#################################################################
#��������: INT_CHECK_L34_MONTH.tcl
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
#�����ţ�L3,L4
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��L3�����Ÿ��˿ͻ���ռ��
#          L4�����Ÿ߼�ֵ�ͻ�������
#����������L3��10% �� ���Ÿ��˿ͻ���/�û������� �� 60%    
#          L4��20% �� ���Ÿ��˿ͻ��еĸ߼�ֵ�ͻ���/�߼�ֵ�ͻ��� �� 80%
#У�����
#          BASS1.G_A_02004_DAY    �û�
#          BASS1.G_A_02008_DAT    �û�״̬
#          BASS1.G_I_02049_MONTH  �����û���Ա
#          BASS1.G_S_03005_MONTH  �ۺ��ʵ�
#          BASS1.G_S_03012_MONTH  �������û���ϸ����
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ�� 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #��������
        set this_month_days [GetThisMonthDays ${op_month}01]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #��������
        set app_name "INT_CHECK_L34_MONTH.tcl"

#ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('L3','L4') "
    	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

############################L3�����Ÿ��˿ͻ���ռ��#############
       #���Ÿ��˿ͻ���
	set handle [aidb_open $conn]
	set sqlbuf "
	     SELECT
               COUNT(DISTINCT A.USER_ID)
             FROM 
            (SELECT USER_ID  AS USER_ID FROM BASS1.G_I_02049_MONTH
             WHERE TIME_ID=$op_month
             ) A,      
	     ( SELECT
	        A.TIME_ID,
    		A.USER_ID,
    		A.USERTYPE_ID,
    		A.SIM_CODE
    	       FROM BASS1.G_A_02004_DAY  A,
    	       (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	WHERE TIME_ID <=$this_month_last_day
    	  	GROUP BY USER_ID
    	       ) B
    	       WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
    	    )T				
            WHERE A.USER_ID = T.USER_ID
    		AND T.TIME_ID <=$this_month_last_day
    		AND T.USERTYPE_ID <> '3'
    		AND T.SIM_CODE <> '1'"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#�û�������
	set RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
        puts $RESULT_VAL2
        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month,
			'L3',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
        puts $sqlbuf			
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
        
	#�ж�L3��10% �� ���Ÿ��˿ͻ���/�û������� �� 60%����
	#����2007��У�����L3�ķ�Χ��10%-60%������8%-23% @20070517 By TYM
	if { $RESULT<0.08 ||$RESULT>0.23  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��L3�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

############################L4�����Ÿ߼�ֵ�ͻ�������#############
       #���Ÿ��˿ͻ��еĸ߼�ֵ�ͻ���
	set handle [aidb_open $conn]
	set sqlbuf "
	    SELECT
               COUNT(DISTINCT A.USER_ID)
            FROM 
            (
        	SELECT USER_ID AS USER_ID FROM BASS1.G_I_02049_MONTH
        	WHERE TIME_ID =$op_month
            ) A,      
            ( SELECT  USER_ID AS USER_ID FROM ( 
	                            SELECT USER_ID FROM bass1.G_S_03005_MONTH
 	                            WHERE TIME_ID =$op_month
                                    GROUP BY USER_ID
                                    HAVING SUM(INT(SHOULD_FEE))/700 >=$this_month_days
                                    UNION 
                                    SELECT USER_ID AS USER_ID  FROM bass1.G_S_03012_MONTH
                                    WHERE TIME_ID = $op_month
                                    GROUP BY USER_ID
                                    HAVING SUM(INT(INCM_AMT))/700 >= $this_month_days
                                   )T	    
          )B
          WHERE A.USER_ID = B.USER_ID"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#�߼�ֵ�ͻ���
	set handle [aidb_open $conn]
	set sqlbuf "
		SELECT  
            	COUNT(DISTINCT A.USER_ID)
                FROM 
	           (
	            SELECT USER_ID FROM BASS1.G_S_03005_MONTH
 	            WHERE TIME_ID =$op_month
                    GROUP BY USER_ID
                    HAVING SUM(INT(SHOULD_FEE))/700 >=$this_month_days
	            UNION ALL
	            SELECT USER_ID FROM BASS1.G_S_03012_MONTH
 	            WHERE TIME_ID =$op_month
                    GROUP BY USER_ID
                    HAVING SUM(INT(INCM_AMT))/700 >=$this_month_days
	           )A,
		   ( SELECT
		       A.TIME_ID,
    		       A.USER_ID,
    		       A.USERTYPE_ID,
    		       A.SIM_CODE
    		     FROM BASS1.G_A_02004_DAY  A,
    		           (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	            WHERE TIME_ID <= $this_month_last_day
    	  	            GROUP BY USER_ID
    	                    )B
    	             WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
    	            )T					
                WHERE A.USER_ID = T.USER_ID
    		  AND T.TIME_ID <= $this_month_last_day
    		  AND T.USERTYPE_ID <> '3'
    		  AND T.SIM_CODE <> '1'"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle


        #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'L4',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
        
	#�ж�L4��20% �� ���Ÿ��˿ͻ��еĸ߼�ֵ�ͻ���/�߼�ֵ�ͻ��� �� 80%����
	if { $RESULT<0.20 ||$RESULT>0.80  } {	
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��L4�������ſ��˷�Χ"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
##################################END#######################
	return 0
}