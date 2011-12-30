######################################################################################################
#�������ƣ� INT_CHECK_GPRS_FLOW_DAY.tcl
#У��ӿڣ� G_S_04002_DAY.tcl
#��������:  ��
#�������: 
#�������:  ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ� liuqf
#��дʱ�䣺 2009-11-23
#�����¼��
#�޸���ʷ: 20100125 �����ͻ��ھ��䶯 usertype_id NOT IN ('2010','2020','2030','9000') 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_GPRS_FLOW_DAY.tcl"

 puts " ɾ��������"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R173','R174') "        
	  exec_sql $sqlbuf


#������ʱ��1 ��������û�״̬ �ο�R027
	set sqlbuf "	declare global temporary table session.int_check_gprs_day_tmp1
									(
								   user_id        CHARACTER(15)
								  ,product_no     CHARACTER(15)
								  ,test_flag      CHARACTER(1)
								  ,sim_code       CHARACTER(15) 
								  ,usertype_id    CHARACTER(4)
									)                            
									partitioning key           
									 (
									   user_id    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	exec_sql $sqlbuf  

	set sqlbuf "	insert into session.int_check_gprs_day_tmp1 (
												 user_id    
												,product_no 
												,test_flag  
												,sim_code   
												,usertype_id  )
									select e.user_id
												,e.product_no  
												,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
												,e.sim_code
												,f.usertype_id           
									from (select user_id , create_date ,product_no,sim_code,usertype_id
												 			,row_number() over(partition by user_id order by time_id desc ) row_id   
									      from bass1.g_a_02004_day
									      where time_id<=$timestamp ) e 
									inner join ( select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
												       from bass1.g_a_02008_day
												       where time_id<=$timestamp ) f on f.user_id=e.user_id 
									where e.row_id=1 and f.row_id=1   "
	exec_sql $sqlbuf  




 puts "R173	��	GPRS���������ձ䶯��<= 50%  "
   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select sum(bigint(a.UP_FLOWS)) val1
					from bass1.G_S_04002_DAY a,
					     session.int_check_gprs_day_tmp1 b 
					where a.product_no = b.product_no 
						and b.usertype_id not IN ('2010','2020','2030','9000') 
						and b.test_flag='0'
					  and a.time_id=$timestamp ) M
				,(select sum(bigint(c.UP_FLOWS)) val3
					from bass1.G_S_04002_DAY c,
					     session.int_check_gprs_day_tmp1 d 
					where c.product_no = d.product_no 
						and d.usertype_id not IN ('2010','2020','2030','9000') 
						and d.test_flag='0'
					  and c.time_id=$last_day ) N 
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R173',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.45||$RESULT_VAL3<-0.45 } {
		set grade 2
	  set alarmcontent "R173У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 



 puts "R174	��	GPRS���������ձ䶯��<= 50% "

   set sqlbuf " 
				select val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select sum(bigint(a.DOWN_FLOWS)) val1
					from bass1.G_S_04002_DAY a,
					     session.int_check_gprs_day_tmp1 b 
					where a.product_no = b.product_no 
						and b.usertype_id not IN ('2010','2020','2030','9000') 
						and b.test_flag='0'
					  and a.time_id=$timestamp ) M
				,(select sum(bigint(c.DOWN_FLOWS)) val3
					from bass1.G_S_04002_DAY c,
					     session.int_check_gprs_day_tmp1 d 
					where c.product_no = d.product_no 
						and d.usertype_id not IN ('2010','2020','2030','9000') 
						and d.test_flag='0'
					  and c.time_id=$last_day ) N 
    "

   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R174',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.45||$RESULT_VAL3<-0.45 } {
		set grade 2
	  set alarmcontent "R174У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 




	return 0
}




#------------------------�ڲ���������--------------------------#	
#  get_row ���� SQL���Џ�
proc get_row {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	set p_row [aidb_fetch $handle]
	aidb_commit $conn
	aidb_close $handle
	return $p_row
}

#   exec_sql ִ��SQL
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
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
