#################################################################
#��������: INT_CHECK_A0L694B5_DAY.tcl
#��������: ����A0L694B5��У�����
#�����ţ�A0,L6,94(��У��),B5
#�������ԣ�ҵ���߼�
#�������ͣ���
#ָ��ժҪ��A0������ͨ�ŷѵ���
#          L6����Ե���ŵ���
#          94: ���Ż����߼����
#          B5: �ֻ����û���>0
#����������A0������ƽ��ͨ�ŷѵ��ۡ�0.9Ԫ
#          L6��0.10 �� ���µ�Ե����ͨ�ŷѺϼ�/���µ�Ե���żƷ��� �� 0.60��Ԫ/����
#          94: ���Ż���ҵ��������Ӧ�����Ͳ�ƥ��
#          B5: �ֻ����û�����0
#У�����1.BASS1.G_S_04004_DAY  ���Ż���
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.20070822 �޸��ֻ��������û������Ļ�ѧ
#�޸���ʷ: 1.
#####################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $op_time 8 9]      
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        #��������
        set app_name "INT_CHECK_A0L694B5_DAY.tcl"

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_rule_check where time_id=${timestamp} 
		        and rule_code in ('A0','L6','94','B5') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
                

###################################A0������ͨ�ŷѵ���##########################
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT
              	  SUM(BIGINT(CALL_FEE)),
              	  COUNT(*)
                FROM 
                  bass1.G_S_04004_DAY
                WHERE
                  TIME_ID=$timestamp
                  AND (  (APPLCN_TYPE='0' AND MM_BILL_TYPE='00')
                       OR (APPLCN_TYPE IN ('1','2','3','4')  )   ) "
        puts $sql_buff
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
        	WriteTrace $errmsg 1001
        	return -1
        }

        while {[set p_success [aidb_fetch $handle]] != "" } {
                set CHECK_VAL1 	      [lindex $p_success 0]
                set CHECK_VAL2        [lindex $p_success 1]
        }

#       puts $CHECK_VAL1
#       puts $CHECK_VAL2
#
     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /100.00]]
     set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

#
#    puts $RESULT_VAL1
#    puts $RESULT_VAL2

    #��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A0',
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
	
	#�ж�
	if { $RESULT_VAL1==0.00 || $RESULT_VAL2==0.00 || [format "%.2f" [expr ${RESULT_VAL1} / ${RESULT_VAL2}]]>0.90 } {		
		set grade 2
	        set alarmcontent "׼ȷ��ָ��A0�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
            }
           puts "A0����" 
           
##################################94: ���Ż����߼����#################
#           #--���Ż����ӿ���ȡ����������ǵ��£�
#           #--�Ҷ��Ż���������(1,11)���У�ͨ�ŷѲ�����ļ�¼��
#            set handle [aidb_open $conn]
#	    set sqlbuf "\
#	           SELECT
#                     COUNT(*)
#	           FROM 
#	             bass1.G_S_04004_DAY
#	           WHERE 
#	             TIME_ID/100=$op_month
#		     AND BUS_SRV_ID IN ('1','4')
#		     AND APPLCN_TYPE IN ('1','2','3','4')"
#
#	 if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set DEC_RESULT_VAL2 "0.0"
#	
#	#��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#			($timestamp ,
#			'94',
#			${DEC_RESULT_VAL1},
#			${DEC_RESULT_VAL2},
#			0,
#			0)"
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 003
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#�ж�
#        #1��������Ż�����ҵ�������ǣ�1���ڵ�Ե���ţ�4���ʵ�Ե���ţ�ʱ��
#        #����Ӧ������Ϊ1�� Email(MM3�ӿڵ����Ӧ��)���նˡ�2�� �ն˵�Email(MM3�ӿڵ����Ӧ��)��
#        #3�� VASӦ�õ��նˡ�4�� �ն˵�VASӦ�õĻ��������������������㣬��Υ���˹���
#	if {${DEC_RESULT_VAL1}>0} {
#		
#		set grade 2
#	        set alarmcontent "׼ȷ��ָ��94�������ſ��˷�Χ"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#		
#            }
#           puts "94����"            
#####################################################################################################
           if { $today_dd <= 19 } {
        	    	puts "���� $today_dd �ţ�δ��19�ţ��ݲ�����"
        	    	return 0
        	}
##################################L6����Ե���ŵ���###########################        	
           set handle [aidb_open $conn]
	   set sqlbuf "\
                SELECT 
            	  SUM(BIGINT(CALL_FEE)) AS TXF,
            	  COUNT(*) AS TXL
                FROM 
                  bass1.G_S_04004_DAY
                WHERE
                  TIME_ID/100=$op_month 
                  AND APPLCN_TYPE='0' 
                  AND MM_BILL_TYPE='00'
                  AND BUS_SRV_ID IN ('1','4') "      
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set DEC_CHECK_VALUE_1 [lindex $p_row 0]
	   	set DEC_CHECK_VALUE_2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set DEC_CHECK_VALUE_1 [format "%.5f" [expr ${DEC_CHECK_VALUE_1} /100.00]]
	   set DEC_CHECK_VALUE_2
	   #��У��ֵ����У������
           set handle [aidb_open $conn]
           set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($timestamp ,
             		'L6',
             		$DEC_CHECK_VALUE_1,
             		$DEC_CHECK_VALUE_2,
             		0,
             		0)"
           if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
           }
           aidb_commit $conn
           aidb_close $handle
           
           set RESULT [format "%.2f" [expr ($DEC_CHECK_VALUE_1/$DEC_CHECK_VALUE_2)]]
           puts $RESULT
           #puts $RESULT
           
           #--�ж�
           #--�쳣
           #----1����Ե����ͨ����=0,�쳣
           #--2��0.10<=���µ�Ե����ͨ�ŷѺϼ�/���µ�Ե���żƷ���<=0.60��Ԫ/����
	   if { $DEC_CHECK_VALUE_2==0 } {
	     	set grade 2
	        set alarmcontent "׼ȷ��ָ��L6:��Ե����ͨ����=0,�쳣"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}			     		     			  
	   } elseif {$RESULT<0.10 || $RESULT>0.60} {
		set grade 2
	        set alarmcontent "׼ȷ��ָ��L6�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
	         }
	    puts "L6����"
	    
############################B5: �ֻ����û���>0############################
            #--�ֻ��������û���
            #--ע�⣺SPҵ��������ʱ����
            set handle [aidb_open $conn]
#	    set sqlbuf "\
#                     SELECT 
#                       COUNT(DISTINCT PRODUCT_NO )
#		     FROM 
#		       bass1.G_S_04004_DAY
#		     WHERE 
#		       TIME_ID/100=$op_month
#		         AND BIGINT(INFO_FEE)>0
#		         AND SP_ENT_CODE='819234'
#		         AND BUS_CODE IN (
#      '110301', '110302', '110303', '110304', '110305', '110306', '110311', '110321', '110322', '110323', '110325', '110326', '110327', '110332', '110339', '110340',
#      '110349', '110361', '110362', '112301', '112304', '112305', '112306', '112307', '112308', '112309', '112310', '112311', '112312', '112314', '112315', '112316',
#      '112317', '112318', '112319', '112327', '112328', '112329', '112330', '112331', '112332', '112333', '112334', '112345', '112346', '112347', '112348', '112349',
#      '112350', '133302')"
    set sqlbuf "select  COUNT(DISTINCT PRODUCT_NO ) 
                   from bass1.G_S_04004_DAY
		              WHERE TIME_ID/100=$op_month  AND 
		                    sp_ent_code = '801234' AND 
		                    
		                    BUS_SRV_ID in ('1','2','3')  "
#svc_code='7000'        AND    20080131 �Ļ�ѧ�޸�  ԭ���ֻ����ھ��仯

	 if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 "0.0"
	
	#��У��ֵ����У������
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B5',
			${DEC_RESULT_VAL1},
			${DEC_RESULT_VAL2},
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	#�ж�
	if {${DEC_RESULT_VAL1}<=0} {
		set grade 2
	        set alarmcontent "�۷���ָ��B5�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
	}
       puts "B5����" 
           
      return 0
}