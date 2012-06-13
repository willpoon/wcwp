#################################################################
#��������: INT_CHECK_SAMPLE_TO_DAY.tcl
#��������: ��������У��
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
#�����ţ�R138��R139��R140��R141
#�������ԣ�������֤
#�������ͣ���
#ָ��ժҪ��
#        R138->R107	��	�˾�ͨ����(Ԫ)	
#        ��ȥ��R141	��	�˾��Ʒ�ʱ��	
#        R139->R108	��	�˾���;�Ʒ�ʱ��	
#        ��ȥ��R140	��	�˾�����ͨ������	

#����������R138->R107: |�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0
#          ��ȥ��R141��|�����굥�˾�����ͨ������/���������˾�����ͨ������ - 1| �� 5�������˾�����ͨ��������0
#          R139->R108��|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0
#          ��ȥ��R140��|�����굥�˾���;�Ʒ�ʱ��/���������˾���;�Ʒ�ʱ�� - 1| �� 5�������˾���;�Ʒ�ʱ����0
#У�����
#          BASS1.G_S_04008_DAY     --����GSM��ͨ��������             
#          BASS1.G_S_04009_DAY     --������������������              
#          BASS1.G_S_21003_TO_DAY  --GSM��ͨ����ҵ����ʹ��(��ʹ��)
#          BASS1.G_S_21006_TO_DAY  --����������ҵ����ʹ��(��ʹ��) 
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
# liuzhilong 20090910 �޸�У�������� R138->R107 R139->R108 ȥ��R140 R141У�飬�޸�ʱ��25���ٸ澯
##~   ������ int ����� G_S_04008_DAY
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	
  ##~   set op_time  2012-05-31
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
        
        ##��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #��������
        set app_name "INT_CHECK_SAMPLE_TO_DAY.tcl"


#ɾ����������
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE IN ('R107','R108') 
				"
exec_sql $sql_buff
###################################R107���˾�ͨ����##########################
        if { $today_dd<25 } {
            puts "���� $today_dd �ţ�δ��25�ţ��ݲ�����"
            return 0
        }
        #�����굥�˾�ͨ����
	set sql_buff "\
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
                             WHERE TIME_ID/100=$this_month and roam_type_id not in ('122','202','302','401')
                             GROUP BY PRODUCT_NO
                             UNION ALL
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(0)) AS CF
                             FROM 
                               BASS1.G_S_04009_DAY
                             WHERE TIME_ID/100=$this_month
                             GROUP BY PRODUCT_NO
                           )T
						with ur"
						
   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]

   set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #��������
	     set sql_buff "\	
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

   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set sql_buff "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'R107',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"

             
			exec_sql $sql_buff
             
             
             #�ж�R107��|�����굥�˾�ͨ����/���������˾�ͨ���� - 1| �� 5%�����˾�ͨ���ѣ�0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��R107(ԭR138)�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��R107(ԭR138)�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    }                 
    
#############################R108���˾��Ʒ�ʱ��#################
        #�����굥�˾��Ʒ�ʱ��
	set sql_buff "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)AS RS
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  )T"

   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #��������
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #���������˾��Ʒ�ʱ��
	     set sql_buff "\	
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO) 
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC	  
	                  FROM BASS1.G_S_21003_TO_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_TO_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  )T"
   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(����ֵ/����ֵ-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #��У��ֵ����У������
             set sql_buff "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'R108',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
			 exec_sql $sql_buff
             
             
             
             #�ж�R108��|�����굥�˾��Ʒ�ʱ��/���������˾��Ʒ�ʱ�� - 1| �� 5%�����˾��Ʒ�ʱ����0����
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "�۷���ָ��R108(ԭR139)�������ſ��˷�Χ"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "�۷���ָ��R108(ԭR139)�ӽ����ſ��˷�Χ5%,�ﵽ${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    } 	  
	return 0
}	    	    