######################################################################################################
#�������ƣ�INT_CHECK_TD_DAY.tcl
#У��ӿڣ�G_S_22201_DAY.tcl  G_S_22202_DAY.tcl  G_S_22203_DAY.tcl
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-05-30
#�����¼��
#�޸���ʷ:  20090909 liuzhilong ����TD��У�� 'R138','R139','R140','R141','R142','R143'
#           20100127 �޸������û��ھ� usertype_id not in ('2010','2020','2030','9000')
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        #���� yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#            
#        #����  yyyymm
#        set last_month [GetLastMonth [string range $op_month 0 5]]
#        
#        #��Ȼ�µ�һ�� yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
#        
#        #���µ�һ�� yyyymmdd
#        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_TD_DAY.tcl"

 puts " ɾ��������"
 	  set sqlbuf "
 	  delete from  BASS1.G_RULE_CHECK 
 	  where time_id=$timestamp 
 	  	and rule_code in ('R111','R112','R113','R114','R115','R116','R138','R139','R140','R141','R142','R143') 
 	  	"        
	  exec_sql $sqlbuf    

#	#R110
#
#  set handle [aidb_open $conn]
#	set sql_buff "select bigint(TD_CUSTOMER_CNT)-bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_DATACARD_CNT)
#  from BASS1.G_S_22201_DAY
#  where time_id=$timestamp
# with ur"
#
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
#
#	#puts $DEC_RESULT_VAL1
#
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R110',$DEC_RESULT_VAL1,0,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#
#	#--�ж�
#	#--�쳣
#	#--1
#
#
#	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
#		set grade 2
#	        set alarmcontent " R110У�鲻ͨ��"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#		 }
		 
		 
	#R111

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)
  from BASS1.G_S_22201_DAY
  where time_id=$timestamp
 with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R111',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R111У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
	#R112

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_DATACARD_CNT)-bigint(TD_157_DATACARD_CNT)- bigint(TD_147_DATACARD_CNT)-bigint(TD_GSM_DATACARD_CNT)
  from BASS1.G_S_22201_DAY
  where time_id=$timestamp with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R112',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R112У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
	#R113

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_BILL_DURATION)-bigint(TNET_BILL_DURATION)
  from BASS1.G_S_22202_DAY
  where time_id=$timestamp with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R113',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0 } {
		set grade 2
	        set alarmcontent " R113У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 

	#R114

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_DATA_FLUX)- bigint(TD_TNET_DATA_FLUX)
  from BASS1.G_S_22203_DAY
  where time_id=$timestamp with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R114',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0 } {
		set grade 2
	        set alarmcontent " R114У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 

	#R115

  set handle [aidb_open $conn]
	set sql_buff "select  bigint(TD_DATACARD_DATA_FLUX)- bigint(TD_DADACARD_TNET_DATA_FLUX)
  from BASS1.G_S_22203_DAY 
  where time_id=$timestamp
  with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R115',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0 } {
		set grade 2
	        set alarmcontent " R115У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 


	#R116

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_DATA_FLUX)-bigint(TD_DATACARD_DATA_FLUX)
  from BASS1.G_S_22203_DAY
  where time_id=$timestamp
  with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R116',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0 } {
		set grade 2
	        set alarmcontent " R116У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 

		 		 		 		 		 
##-- 20090909 liuzhilong ����TD��У�� 'R138','R139','R140','R141','R142','R143'
#������ʱ��1 ��������û�״̬
	set sqlbuf "	declare global temporary table session.int_check_td_day_tmp1 
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

	set sqlbuf "	insert into session.int_check_td_day_tmp1 (
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

# R138	��	ʹ��TD����Ŀͻ��� 
# ʹ��TD����Ŀͻ�����ʹ��TD������ֻ��ͻ���+ʹ��TD��������ݿ��ͻ���+ʹ��TD������������ͻ���+ʹ��TD�������Ϣ���ͻ���

   set sqlbuf " 
						select int(TD_CUSTOMER_CNT) val1
									,int(TD_MOBILE_CUSTOMER_CNT)+int(TD_DATACARD_CNT)+int(TD_3GBOOK_CNT)+int(TD_INFOMCH_CNT) val2
						from bass1.G_S_22201_DAY
						where time_id=$timestamp
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R138',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL2-$RESULT_VAL1,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL2-$RESULT_VAL1 <0 } {
		set grade 2
	  set alarmcontent "R138 У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


# R139	��	ʹ��TD����ͻ���ÿ�ղ�����
# ʹ��TD����ͻ���ÿ�ղ����ʣ�25%
   set sqlbuf " 
					select M.val1
								,N.val2
								,decimal(round(N.val2/1.00/M.val1-1,4),9,4) rate 
					from 
							( select int(TD_CUSTOMER_CNT) val1
							  from bass1.G_S_22201_DAY
							  where time_id=$timestamp ) M
							,
							( select int(TD_CUSTOMER_CNT) val2
								from bass1.G_S_22201_DAY
								where time_id=$last_day ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R139',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3 >=0.25||$RESULT_VAL3 <=-0.25 } {
		set grade 2
	  set alarmcontent "R139 У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


# R140	��	���ܽӿڡ�ʹ��TD����Ŀͻ�������ͨ���굥ͳ�Ƶ�ʹ��TD����ͻ�����ƫ����5%����
   set sqlbuf " 
						select M.val1
									,N.val2
									,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate 
					  from 
								( select int(TD_CUSTOMER_CNT) val1
									from bass1.G_S_22201_DAY
									where time_id=$timestamp ) M
								,
								( select count(distinct a.product_no) val2
									from  
									(	
									 select distinct product_no
										from bass1.g_s_04017_day
										where mns_type='1' and time_id=$timestamp
											union
										select distinct product_no
										from bass1.g_s_04002_day
										where mns_type='1' and time_id=$timestamp
											union
										select product_no
										from bass1.g_s_04018_day
										where mns_type='1' and time_id=$timestamp
										) a,session.int_check_td_day_tmp1 b 
										where a.product_no = b.product_no 
										  and b.usertype_id  not IN ('2010','2020','2030','9000') 
										  and b.test_flag='0'
									) N
    "    
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R140',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3 >=0.05||$RESULT_VAL3 <=-0.05 } {
		set grade 2
	  set alarmcontent "R140 У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
		 		 
# R141	��	TD�����еļƷ�ʱ������ܽӿ��е�ʹ��TD����Ŀͻ���T���ϼƷ�ʱ����ƫ����5%����
   set sqlbuf " 
						select M.val1
									,N.val2
									,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate  
					  from 
								( select sum(BASE_BILL_DURATION) val1
									from (select product_no,sum(bigint(BASE_BILL_DURATION)) BASE_BILL_DURATION
											 	from bass1.G_S_04017_DAY
											 	where mns_type='1' and  time_id=$timestamp
											 	group by product_no ) a
									inner join session.int_check_td_day_tmp1 b on a.product_no=b.product_no
									where b.usertype_id  NOT IN ('2010','2020','2030','9000') and b.test_flag='0' ) M
								,
								( select bigint(TNET_BILL_DURATION) val2
								  from bass1.G_S_22202_DAY
								  where time_id=$timestamp ) N
    "    
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R141',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3 >=0.05||$RESULT_VAL3 <=-0.05 } {
		set grade 2
	  set alarmcontent "R141 У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 	


# R142	��	TD�����еļƷ�ʱ�����ջ�����������������ΪTD����ļƷ�ʱ����ƫ����3%����
   set sqlbuf " 
						select M.val1
									,N.val2
									,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate 
					  from 
								( select sum(bigint(BASE_BILL_DURATION))  val1
									from  bass1.G_S_04017_DAY 
									where  mns_type='1' and time_id=$timestamp ) M
								,
								( select sum(bigint(BASE_BILL_DURATION))  val2
									from  bass1.G_S_21001_DAY 
									where time_id=$timestamp and mns_type='1' ) N
    "    
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R142',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3 >=0.03||$RESULT_VAL3 <=-0.03 } {
		set grade 2
	  set alarmcontent "R142 У�鲻ͨ��"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 	

# R143	��	GPRS������������������������������ΪTD�����������������������ܽӿ��е�T���ϵ�����������ƫ����8%����
   set sqlbuf " 
						select M.val1
									,N.val2
									,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate  
						from
							( select  sum(DATA_FLOW)/1024/1024  val1
								from ( select product_no,bigint(UP_FLOWS)+bigint(DOWN_FLOWS)  DATA_FLOW
												from bass1.g_s_04018_day
												where mns_type='1' and  time_id=$timestamp
														union all
												select product_no,bigint(UP_FLOWS)+bigint(DOWN_FLOWS) DATA_FLOW
												from bass1.g_s_04002_day
												where mns_type='1' and time_id=$timestamp ) A
								inner join session.int_check_td_day_tmp1 B on a.product_no=b.product_no
								where b.usertype_id  NOT IN ('2010','2020','2030','9000') and b.test_flag='0'  ) M
							,
							(	select int(td_tnet_data_flux) val2
								from bass1.g_s_22203_day 
								where time_id=$timestamp ) N
    "    
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]

  
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R143',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3 >=0.08||$RESULT_VAL3 <=-0.08 } {
		set grade 2
	  set alarmcontent "R143 У�鲻ͨ��"
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
