#################################################################
# ��������: INT_CHECK_L2_TO_DAY.tcl
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
#�����ţ�L2
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��L2�����ſͻ��±䶯��
#����������L2��|���¼��ſͻ���/���¼��ſͻ��� - 1| �� 10%���ҿͻ���������
#У�����
#          BASS1.G_A_01001_DAY     �ͻ�
#          BASS1.G_A_01004_DAY     ���ſͻ�
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
##################################################################################
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
        
        #����µ���������ʽdd��������20070411 ����30)
        set DaysOfThisMonth [GetThisMonthDays ${this_month}01]
        puts $DaysOfThisMonth
        
#        #�������������ʽddd
#        set DaysOfThisYear [GetThisYearDays ${this_month}01]
#        puts $DaysOfThisYear
        
        #��������
        set app_name "INT_CHECK_L2_TO_DAY.tcl"

       
#####ɾ����������
#####ɾ����������
####        set handle [aidb_open $conn]
####	set sql_buff "
####		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
####    	              AND RULE_CODE='L2' "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
#############################################
####      if {$today_dd>19} {
#####��ֹ���գ����ſͻ�������
####          set handle [aidb_open $conn]
####	  set sql_buff "
####                 SELECT
####                   COUNT(DISTINCT T.ENTERPRISE_ID)
####                 FROM (
####                       SELECT A.ENTERPRISE_ID,A.CUST_STATU_TYP_ID FROM bass1.G_A_01004_DAY A,
####                       (
####                        SELECT ENTERPRISE_ID, MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01004_DAY
####                        WHERE TIME_ID <=$p_timestamp  GROUP BY ENTERPRISE_ID 
####                        )B
####                        WHERE   A.TIME_ID=B.TIME_ID AND A.ENTERPRISE_ID=B.ENTERPRISE_ID  
####                        AND A.CUST_STATU_TYP_ID ='20' 
####                      )T,
####                      (
####                        SELECT  A.CUST_ID AS CUST_ID FROM bass1.G_A_01001_DAY A,
####                        (
####                         SELECT CUST_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01001_DAY
####                         WHERE TIME_ID <=$p_timestamp GROUP BY CUST_ID 
####                        )B
####                        WHERE A.TIME_ID=B.TIME_ID AND A.CUST_ID = B.CUST_ID  
####                        AND A.ORG_TYPE_ID ='2'
####                      )P 
####                  WHERE T.ENTERPRISE_ID=P.CUST_ID "
####	if [catch {aidb_sql $handle $sql_buff } errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	}
####	aidb_commit $conn
####	aidb_close $handle
####       
####       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
####       
#####���¼��ſͻ�������
####          set handle [aidb_open $conn]
####	  set sql_buff "
####                 SELECT
####                   COUNT(DISTINCT T.ENTERPRISE_ID)
####                 FROM (
####                       SELECT A.ENTERPRISE_ID,A.CUST_STATU_TYP_ID FROM bass1.G_A_01004_DAY A,
####                       (
####                        SELECT ENTERPRISE_ID, MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01004_DAY
####                        WHERE TIME_ID <=$last_day_month  GROUP BY ENTERPRISE_ID 
####                        )B
####                        WHERE   A.TIME_ID=B.TIME_ID AND A.ENTERPRISE_ID=B.ENTERPRISE_ID  
####                        AND A.CUST_STATU_TYP_ID ='20' 
####                      )T,
####                      (
####                        SELECT  A.CUST_ID AS CUST_ID FROM bass1.G_A_01001_DAY A,
####                        (
####                         SELECT CUST_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01001_DAY
####                         WHERE TIME_ID <=$last_day_month GROUP BY CUST_ID 
####                        )B
####                        WHERE A.TIME_ID=B.TIME_ID AND A.CUST_ID = B.CUST_ID  
####                        AND A.ORG_TYPE_ID ='2'
####                      )P 
####                  WHERE T.ENTERPRISE_ID=P.CUST_ID "
####	if [catch {aidb_sql $handle $sql_buff } errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	}
####	aidb_commit $conn
####	aidb_close $handle
####       
####      set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
####       
####       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2} - 1)]]
####       
####       puts $RESULT_VAL1
####       puts $RESULT_VAL2
####       puts $RESULT
####       
####      
####    #��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
####			($p_timestamp ,
####			'L2',
####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
####			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
####			cast ($RESULT as  DECIMAL(18, 5) ),
####			0)"
####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
####		WriteTrace $errmsg 003
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####
#####ϵ�� 
####      set coefficient [format "%.5f" [expr (${today_dd}/1.000/${DaysOfThisMonth})]]
####      puts $coefficient
####      set coefficient 1.00
####       set target1 [format [expr (0.100*$coefficient)]]
####       set target2 [format [expr (-0.100*$coefficient)]]
####       set target3 [format [expr (0.095*$coefficient)]]
####       set target4 [format [expr (-0.095*$coefficient)]]  
####       puts $target1
####       puts $target2
####       puts $target3
####       puts $target4
####          
####	#�ж�L2��|���¼��ſͻ���/���¼��ſͻ��� - 1| �� 10%���ҿͻ��������㳬��
####	if { $RESULT<$target2 || $RESULT>$target1 } {
####		set grade 2
####	        set alarmcontent "׼ȷ��ָ��L2�������ſ��˷�Χ"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
####		} elseif { $RESULT<$target4 || $RESULT>$target3 } {
####		set grade 3
####	        set alarmcontent "׼ȷ��ָ��L2�ӽ����ſ��˷�Χ10%,�ﵽ${RESULT}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}			
####		        	
####        }     
####}
##################################END#######################

##   INT_CHECK_L2_TO_DAY.tcl
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Trans91003 $op_time $optime_month
	return 0
}    