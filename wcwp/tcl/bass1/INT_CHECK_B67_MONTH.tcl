#################################################################
#��������: INT_CHECK_B67_MONTH.tcl
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
#�����ţ�B6,B7
#�������ԣ�ҵ���߼���B7������֤��
#�������ͣ���
#ָ��ժҪ��B6��ͨ��IVR����������WEB���ز�����û���/�ʵ����в�����õ��û���
#          B7��������ϼ�������������ʡ�20%
#����������B6��70%<ͨ��IVR����������WEB���ز�����û���/�ʵ����в�����õ��û���<96%
#          B7��|������ϼ�/������ - 1|��20%
#          50: 40Ԫ �� ���¶��еش�ARPUֵ �� 150Ԫ ��RMB��
#У�����
#          BASS1.G_S_04015_DAY         
#          BASS1.G_S_22038_DAY             
#          BASS1.G_S_03004_MONTH         
#          BASS1.G_S_03012_MONTH

#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#�޸ļ�¼�� �Ļ�ѧ 20071112 
#          B6�ھ����� bill_type not in ('1','2','5','6','8')
#***************************************************/
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#���� yyyy-mm
	set opmonth $optime_month
	#�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #��������
        set this_month_days [GetThisMonthDays ${op_month}01]
        #�������� 
        set this_year_days [GetThisYearDays ${op_month}01]
        #�������һ�� yyyymmdd
        set last_month [GetLastMonth [string range $op_month 0 5]]
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]    
        #��������
        set app_name "INT_CHECK_B67_MONTH.tcl"


        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$op_month
    	              AND RULE_CODE IN ('B6','B7')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
####################B6:ͨ��IVR����������WEB���ز�����û���#################
##        #����04015����ҵ�񻰵�����������Ǳ��£�����;����1����������2(WEB)��ҵ���ʶ��'1'
##        #(��DIY�Ǽ��Ÿ��˲���)������û���
##        set handle [aidb_open $conn]
##        set sqlbuf "\
##	    	SELECT 
##	          VALUE(COUNT(DISTINCT PRODUCT_NO),0)
##	        FROM BASS1.G_S_04015_DAY
##	        WHERE TIME_ID/100=$op_month
##	             AND CUSTOM_CHNL IN ('1','2')
##	             AND BUSI_TYPE='1'
##	             AND bill_type not in ('1','2','5','6','8') "
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
##	#�ʵ����в�����õ��û���
##        set handle [aidb_open $conn]
##        set sqlbuf "\
##	        SELECT COUNT(DISTINCT T.USER_ID)
##	        FROM 
##	        (
##	    	  SELECT 
##	            USER_ID
##	          FROM BASS1.G_S_03004_MONTH
##	          WHERE TIME_ID=$op_month
##	               AND ACCT_ITEM_ID IN('0519','0639')
##	          UNION ALL
##	    	  SELECT 
##	            USER_ID
##	          FROM BASS1.G_S_03012_MONTH
##	          WHERE TIME_ID=$op_month
##	               AND ACCT_ITEM_ID IN('0519','0639')
##	         )T"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "$DEC_RESULT_VAL2" 
##	
##	
##	set RESULT [format "%.5f" [expr (${DEC_RESULT_VAL1}/1.00000/${DEC_RESULT_VAL2})]]
##	
##	#��У��ֵ����У������
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'B6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        #�ж�
##        #�쳣��
##        #B6��70%<ͨ��IVR����������WEB���ز�����û���/�ʵ����в�����õ��û���<96%����
##       if { $DEC_RESULT_VAL1==0 || $DEC_RESULT_VAL2==0  } {
##       	set grade 2
##          set alarmcontent "�۷���ָ��B6IVR����������WEB���ز�����û���/�ʵ����в�����õ��û���Ϊ0"
##          WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
##       	     		
##       } elseif { ($RESULT<=0.05 ||$RESULT>0.30) } {	
##             set grade 3
##             set alarmcontent "�۷���ָ��B6��ͨ��IVR����������WEB���ز�����û���/�ʵ����в�����õ��û����������ſ��˷�Χ"
##             WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##            
##      }                	
##      
##########################B7:������ϼ�������������ʡ�20%##################
        #��22038��Ʒ���û���KPI�ӿ�����������ǵ��µĵ���������л������  
        set handle [aidb_open $conn]
        set sqlbuf "\
	    	SELECT 
	          SUM(BIGINT(INCOME))
	        FROM BASS1.G_S_22038_DAY
	        WHERE TIME_ID/100=$op_month"
        puts $sqlbuf
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
	
	#������
        set handle [aidb_open $conn]
        set sqlbuf "\
	        SELECT SUM(T.FEE)
	        FROM 
	        (
	    	  SELECT 
	            SUM(BIGINT(FEE_RECEIVABLE)) AS FEE
	          FROM BASS1.G_S_03004_MONTH
	          WHERE TIME_ID=$op_month
	          UNION ALL
	    	  SELECT 
	            SUM(BIGINT(INCM_AMT)) AS FEE
	          FROM BASS1.G_S_03012_MONTH
	          WHERE TIME_ID=$op_month
	         )T"
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
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"         	
	     		
	set RESULT [format "%.5f" [expr (${DEC_RESULT_VAL1}/1.00000/${DEC_RESULT_VAL2}-1)]]
	     		
	#��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'B7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	   

        #�ж�
        #�쳣��
        #B7��|������ϼ�/������ - 1|��20%����
	if {$RESULT<-0.20 || $RESULT>0.20} {
	           set grade 2
	           set alarmcontent "�۷���ָ��B7��|������ϼ�/������ - 1|��20%�������ſ��˷�Χ"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }	  	
        
        	
	return 0
}