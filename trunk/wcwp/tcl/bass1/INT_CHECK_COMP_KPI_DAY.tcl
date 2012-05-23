######################################################################################################
#�������ƣ�INT_CHECK_COMP_KPI_DAY.tcl
#У��ӿڣ�G_S_22073_DAY.tcl
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�
#��дʱ�䣺2009-09-08
#�����¼��
#�޸���ʷ: 20091123 ����R163-R172 10������У��
#�޸���ʷ: 2011-05-26 15:13:43 R171,R172 40% -> 50%
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_COMP_KPI_DAY.tcl"
		set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]		
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]

 puts " ɾ��������"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp 
 	                and rule_code in ('R153','R154','R155','R156','R157','R158','R163','R164','R165','R166','R167','R168','R169','R170','R171','R172') "        
	  exec_sql $sqlbuf     


 puts "R153	��	(������ͨ�����ͻ���-������ͨ�����ͻ���)��(������ͨ�ͻ�������-������ͨ�ͻ�������)��ƫ����1%����"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from 
				 (select ( int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) )- 
								 ( int(UNION_MOBILE_LOST_CNT)+int(UNION_NET_LOST_CNT)+int(UNION_FIX_LOST_CNT) ) val1
								 ,int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val2
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R153',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R153У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R154	��	(������ͨ�ƶ������ͻ���-������ͨ�ƶ������ͻ���)��(������ͨ�ƶ��ͻ�������-������ͨ�ƶ��ͻ�������)ƫ����1������"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from 
				 (select ( int(UNION_MOBILE_NEW_ADD_CNT)- int(UNION_MOBILE_LOST_CNT) ) val1
								 ,int(UNION_MOBILE_ARRIVE_CNT)  val2
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R154',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R154У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	
		 		 		 		 
 puts "R155	��	(���յ��������ͻ���-���յ��������ͻ���)�루���յ��ſͻ�������-���յ��ſͻ�����������ƫ����1%����"
   set sqlbuf " 
			select val1
						,val2-val3
						,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
			from
			 (select ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) )- 
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) val1
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val2
				from bass1.G_S_22073_DAY
				where time_id=$timestamp ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R155',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R155У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R156	��	(���յ����ƶ������ͻ���-���յ����ƶ������ͻ���)��(���յ����ƶ��ͻ�������-���յ����ƶ��ͻ�������)ƫ����1������"
   set sqlbuf " 
				select val1
							,val2-val3
							,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
				from
				(select ( int(TEL_MOBILE_NEW_ADD_CNT)- int(TEL_MOBILE_LOST_CNT) ) val1
						 ,int(TEL_MOBILE_ARRIVE_CNT)  val2
				from bass1.G_S_22073_DAY
				where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R156',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01  } {
		set grade 2
	  set alarmcontent "R156У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R157	��	������ͨ�ƶ��û�������ͨ�ƶ��ͻ�������"
   set sqlbuf " 
		select int(M_UNION_MOBILE_CNT)
					,int(UNION_MOBILE_ARRIVE_CNT)
					,int(UNION_MOBILE_ARRIVE_CNT)-int(M_UNION_MOBILE_CNT)
		from bass1.G_S_22073_DAY
		where time_id=$timestamp
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R157',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3<0 } {
		set grade 2
	  set alarmcontent "R157У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R158	��	���µ����ƶ��û����ܵ����ƶ��ͻ�������"
   set sqlbuf " 
				select int(M_TEL_MOBILE_CNT)
							,int(TEL_MOBILE_ARRIVE_CNT)
							,int(TEL_MOBILE_ARRIVE_CNT)-int(M_TEL_MOBILE_CNT)
				from bass1.G_S_22073_DAY
				where time_id=$timestamp
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R158',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3<0 } {
		set grade 2
	  set alarmcontent "R158У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##########################################################################
###1.6.4�淶�����Ĳ���У��
##########################################################################

 puts "R163	��	��ͨ����ͻ����ձ䶯�ʡ�1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R163',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R163У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R164	��	���ŵ���ͻ����ձ䶯�ʡ�1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R164',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R164У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R165	��	��ͨ�ƶ��ͻ������ձ䶯�ʡ�1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_MOBILE_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R165',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R165У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R166	��	�����ƶ��ͻ������ձ䶯�ʡ�1%"
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_MOBILE_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_MOBILE_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R166',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R166У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 puts "R167	��	��ͨ�̻��ͻ������ձ䶯�ʡ�1% "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(UNION_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(UNION_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R167',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R167У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



 puts "R168	��	���Ź̻��ͻ������ձ䶯�ʡ�1%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select int(TEL_FIX_ARRIVE_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select int(TEL_FIX_ARRIVE_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R168',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.01||$RESULT_VAL3<-0.01 } {
		set grade 2
	  set alarmcontent "R168У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



 puts "R169	��	��ͨ�����ͻ����ձ䶯�ʡ� 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R169',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R169У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##~   20120523 2012�°�У�飡
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R169У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R169У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 puts "R170	��	���������ͻ����ձ䶯�ʡ� 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R170',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R170У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012�°�У�飡
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R170У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R170У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 
 puts "R171	��	��ͨ�ƶ������ͻ����ձ䶯�ʡ� 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(UNION_MOBILE_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(UNION_MOBILE_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R171',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R171У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012�°�У�飡
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R171У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R171У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 

 puts "R172	��	�����ƶ������ͻ����ձ䶯�ʡ� 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT) val1
					from bass1.G_S_22073_DAY
					where time_id=$timestamp ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=$last_day ) N 
			    with ur
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R172',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.5||$RESULT_VAL3<-0.5 } {
		set grade 2
	  set alarmcontent "R172У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



##~   -----------------------------------------------------------------------------------------------------------


##~   20120523 2012�°�У�飡
 
if { $today_dd == 1 || $today_dd == 2 || $today_dd == 30 || $today_dd == 31 } { 
	if {$RESULT_VAL3>0.55||$RESULT_VAL3<-0.55 } {
		set grade 2
	  set alarmcontent "R172У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
} else {

	if {$RESULT_VAL3>0.25||$RESULT_VAL3<-0.25 } {
		set grade 2
	  set alarmcontent "R172У��ﵽ�ٽ�ֵ"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

}

##~   -----------------------------------------------------------------------------------------------------------
 

	return 0
}

