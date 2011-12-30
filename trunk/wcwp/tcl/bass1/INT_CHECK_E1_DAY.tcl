#################################################################
# ��������: INT_CHECK_E1_DAY.tcl
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
#�����ţ�E1
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��E1����Ե�����ռ��ʸ�Ϊ�����û���ռ��
#����������E1: 10% �� �����û���/�û������� �� 50%
#У�����
#          1.BASS1.G_A_02004_DAY     �û�
#          2.BASS1.G_A_02008_DAY     �û�״̬
#          3.BASS1.G_A_02011_DAY     �û���ͨҵ������ʷ
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
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
        #���µ�һ�� yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        #���µ�һ�� yyyymmdd
        set next_month_first_day [string range $this_month 0 5]01
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #��������
        set app_name "INT_CHECK_E1_DAY.tcl"


##ɾ����������
#  set handle [aidb_open $conn]
#	set sql_buff "
#		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
#    	              AND RULE_CODE='E1' "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
###################################E1����Ե�����ռ��ʸ�Ϊ�����û���ռ��################
#	#1-24��ֱ�ӷ��أ�25�ſ�ʼ�ж�
#	#�����û���
#	if { $today_dd>=20 } {
#	
#	
#	
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE bass1.check_02011_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02011_e1_mid 
#	SELECT 
#               	     A.USER_ID
#                     FROM bass1.G_A_02011_DAY A,
#                    (
#                      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02011_DAY
#	    			  WHERE TIME_ID<=$p_timestamp 
#	    			       AND int(VALID_DATE)<$this_month_first_day
#	    			       AND int(INVALID_DATE)>=$next_month_first_day
#	                   GROUP  BY USER_ID
#                     )B
#                     WHERE  A.TIME_ID = B.TIME_ID
#                        AND A.USER_ID = B.USER_ID
#                        AND BUSI_CODE IN ('370')  with ur"
#              puts $sqlbuf
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#		set handle [aidb_open $conn]
#	
#	  set sqlbuf "ALTER TABLE bass1.check_02004_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#              puts $sqlbuf
#	 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02004_e1_mid 
#	SELECT
#    		 	    A.TIME_ID,
#    		 	    A.USER_ID,
#    		 	    A.USERTYPE_ID,
#    		 	    A.SIM_CODE
#    		       FROM bass1.G_A_02004_DAY  A,
#    		      (
#    		       SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
#    	 	       WHERE TIME_ID <=$p_timestamp 
#    	  	       GROUP BY USER_ID
#    	         ) B
#    	              WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID with ur"
#              puts $sqlbuf
#	 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	      
#
#		set handle [aidb_open $conn]
#	
#	  set sqlbuf "ALTER TABLE bass1.check_02008_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#              puts $sqlbuf
#	  	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02008_e1_mid 
#	 SELECT
#    	 		    A.TIME_ID,
#    	 		    A.USER_ID,
#    	 		    A.USERTYPE_ID
#    		      FROM bass1.G_A_02008_DAY A,
#    		      ( SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02008_DAY
#    		        WHERE TIME_ID <=$p_timestamp
#    		        GROUP BY USER_ID
#    	         ) B
#    		     WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  with ur"
#              puts $sqlbuf
#	  	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	      	
#	     set handle [aidb_open $conn]
#	     set sqlbuf "
#               SELECT COUNT(DISTINCT P.USER_ID) FROM 
#                  bass1.check_02011_e1_mid P,  
#		             ( SELECT  T.USER_ID 
#    	                       FROM  bass1.check_02004_e1_mid T, 	    
#    		                           bass1.check_02008_e1_mid  M		
#   		             WHERE T.USER_ID = M.USER_ID
#    		              AND T.TIME_ID <=$p_timestamp 
#    		              AND M.TIME_ID <=$p_timestamp 
#    		              AND T.USERTYPE_ID <> '3'
#    		              AND T.SIM_CODE <> '1'		
#    		              AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')
#    		         )L
#          WHERE P.USER_ID=L.USER_ID"
#              puts $sqlbuf
# if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	      }
#	      while { [set p_row [aidb_fetch $handle]] != "" } {
#		set CHECK_VAL1 [lindex $p_row 0]
#	      }
#	      aidb_commit $conn
#	      aidb_close $handle
#	      
##�û�������	      
#	     set handle [aidb_open $conn]
#	     set sqlbuf "
#               SELECT 
#       	       	 COUNT(*) 
#       	       FROM 
#       	       	bass1.check_02004_e1_mid T, 	
#       	        bass1.check_02008_e1_mid M		
#      	       	WHERE T.USER_ID = M.USER_ID
#       	       	  AND T.TIME_ID <=$p_timestamp
#       	       	  AND M.TIME_ID <=$p_timestamp
#       	       	  AND T.USERTYPE_ID <> '3'
#       	       	  AND T.SIM_CODE <> '1'
#       	       	  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')"
#
#	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	      }
#	      while { [set p_row [aidb_fetch $handle]] != "" } {
#		set CHECK_VAL2 [lindex $p_row 0]
#	      }
#	      aidb_commit $conn
#	      aidb_close $handle
#	      
#	      set RESULT_VAL1 $CHECK_VAL1
#	      set RESULT_VAL2 $CHECK_VAL2
#	      
#             #��У��ֵ����У������
#             set handle [aidb_open $conn]
#             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#             		($p_timestamp ,
#             		'E1',
#             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
#             		0,
#             		0)"
#             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#             	WriteTrace $errmsg 003
#             	return -1
#             }
#             aidb_commit $conn
#             aidb_close $handle
#            
#            
#            set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
#             #�ж�E1:10% �� �����û���/�û������� �� 50%����
#	     if { $RESULT<0.10 || $RESULT>0.50 } {	
#	     set grade 2
#	     set alarmcontent "׼ȷ��ָ��E1�������ſ��˷�Χ"
#	     WriteAlarm $app_name $op_time $grade ${alarmcontent}     	
#             }
#       }
#       puts "E1����"
###################################END#######################



  #����02011����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select busi_code,user_id,count(*) cnt from bass1.g_a_02011_day
	              where time_id =$p_timestamp
	             group by busi_code,user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02011�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   }











	return 0
}

