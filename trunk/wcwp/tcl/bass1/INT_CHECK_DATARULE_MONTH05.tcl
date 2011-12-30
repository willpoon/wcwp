#################################################################
#��������: INT_CHECK_DATARULE_MONTH05.tcl
#��������: 
#�����ţ�R007,R033,R034,R035,R036,R037
#�������ԣ����ݹ����Լ�ȡֵ��ΧУ��
#�������ͣ���

#ָ��ժҪ��
#          R007	�����û���Ա�ӿڵļ��ſͻ���ʶӦ�����ڼ��ſͻ��ӿ���                          
#          R033	��	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ����Ϊ�ջ�δ֪���ı���С��5��
#          R034	��	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ���볤��Ϊ6λ
#          R035	��	��������ҵ��SP��ҵ����	06012 SP��ҵ���� 	�������ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�4����7����ͷ
#          R036	��	����ҵ��SP��ҵ����	06012 SP��ҵ���� 	���ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�8����ͷ������ʡ�Ĳ���ͨ������ͨ����ҵ���ԡ�4����ͷ
#          R037	��	����WAPҵ��SP��ҵ����	06012 SP��ҵ���� 	����WAP���ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�8����ͷ

#����������
#          R007	�����û���Ա�ӿڵļ��ſͻ���ʶӦ�����ڼ��ſͻ��ӿ���                          
#          R033	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ����Ϊ�ջ�δ֪���ı���С��5��
#          R034	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ���볤��Ϊ6λ
#          R035	��������ҵ��SP��ҵ����	06012 SP��ҵ���� 	�������ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�4����7����ͷ
#          R036	����ҵ��SP��ҵ����	06012 SP��ҵ���� 	���ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�8����ͷ������ʡ�Ĳ���ͨ������ͨ����ҵ���ԡ�4����ͷ
#          R037	����WAPҵ��SP��ҵ����	06012 SP��ҵ���� 	����WAP���ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�8����ͷ

#У�����
#        01004	���ſͻ�
#        02049	�����û���Ա
#        06012	SP��ҵ����

#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ��Ļ�ѧ
#��дʱ�䣺2008-05-16
#�����¼��1.
#�޸���ʷ: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyymmdd
        #set op_time 2008-06-30
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time

        #��������
        set app_name "INT_CHECK_DATARULE_DAY.tcl"

        #--ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
		and rule_code in('R007','R033','R034','R035','R036','R037') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


#####################################R007	�����û���Ա�ӿڵļ��ſͻ���ʶӦ�����ڼ��ſͻ��ӿ���#############################
##        #--DR01	�û�ҵ������ȡֵ
##  set sqlbuf " select count(*) from G_I_02049_MONTH where enterprise_id not in (select enterprise_id from G_A_01004_DAY where time_id/100 <= $op_month );"
##   set RESULT_VAL1 [get_single $sqlbuf]
##  
##   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R007',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
##   exec_sql $sqlbuf
##
##	if {$RESULT_VAL1 > 0} {
##		set grade 2
##	        set alarmcontent "�۷���ָ��R007	�����û���Ա�ӿڵļ��ſͻ���ʶӦ�����ڼ��ſͻ��ӿ���"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##	} 
##	puts "R007	�����û���Ա�ӿڵļ��ſͻ���ʶӦ�����ڼ��ſͻ��ӿ��г������ſ��˷�Χ"



###################################R033	���ػ򱾵�ȫ��ҵ���SP��ҵ����Ϊ�ջ�δ֪���ı���С��5��#############################
  set sqlbuf " select count(*) from G_I_06012_MONTH where time_id = $op_month and (sp_name like '%δ֪%' or sp_name is null or rtrim(sp_name) = '') ;"
   set RESULT_VAL1 [get_single $sqlbuf]
  set sqlbuf " select count(*) from G_I_06012_MONTH where time_id = $op_month ;"
   set RESULT_VAL2 [get_single $sqlbuf]
  
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R007',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2}*100)]]

	if {$DEC_TARGET_VAL1 > 5} {
		set grade 2
	        set alarmcontent "�۷���ָ��R033	���ػ򱾵�ȫ��ҵ���SP��ҵ����Ϊ�ջ�δ֪���ı���С��5��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R033	���ػ򱾵�ȫ��ҵ���SP��ҵ����Ϊ�ջ�δ֪���ı���С��5���������ſ��˷�Χ"


###################################R034	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ���볤��Ϊ6λ#############################
        #--DR01	�û�ҵ������ȡֵ
  set sqlbuf "select  count(*) from G_I_06012_MONTH where time_id = $op_month and length(rtrim(sp_code)) <> 6;"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "�۷���ָ��R034	SP��ҵ����	06012 SP��ҵ���� ���ػ򱾵�ȫ��ҵ���SP��ҵ���볤��Ϊ6λ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R034	SP��ҵ����	06012 SP��ҵ���� 	���ػ򱾵�ȫ��ҵ���SP��ҵ���볤��Ϊ6λ�������ſ��˷�Χ"
         



###################################R035	��������ҵ��SP��ҵ����	06012 SP��ҵ���� �������ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�4����7����ͷ
        #--DR01	�û�ҵ������ȡֵ
  set sqlbuf "select  count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '01'  and substr(sp_code,1,1) not in ('9','4','7');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "�۷���ָ��R035	��������ҵ��SP��ҵ����	06012 SP��ҵ���� �������ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�4����7����ͷ�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R035	��������ҵ��SP��ҵ����	06012 SP��ҵ���� �������ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�4����7����ͷ"



###################################R036	����ҵ��SP��ҵ����	06012 SP��ҵ���� ���ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�8����ͷ������ʡ�Ĳ���ͨ������ͨ����ҵ���ԡ�4����ͷ
        #--DR01	�û�ҵ������ȡֵ
  set sqlbuf "select count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '02'  and substr(sp_code,1,1) not in ('8','4');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "�۷���ָ��R036	����ҵ��SP��ҵ����	06012 SP��ҵ���� ���ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�8����ͷ������ʡ�Ĳ���ͨ������ͨ����ҵ���ԡ�4����ͷ�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R036	����ҵ��SP��ҵ����	06012 SP��ҵ���� 	���ű��ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�8����ͷ������ʡ�Ĳ���ͨ������ͨ����ҵ���ԡ�4����ͷ"



###################################R037	����WAPҵ��SP��ҵ����	06012 SP��ҵ���� ����WAP���ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�8����ͷ
        #--DR01	�û�ҵ������ȡֵ
  set sqlbuf "select count(*) from G_I_06012_MONTH where time_id = $op_month and sp_operator_type = '03'  and substr(sp_code,1,1) not in ('9','8');"
   set RESULT_VAL1 [get_single $sqlbuf]
  
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R034',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
   exec_sql $sqlbuf

	if {$RESULT_VAL1 > 0} {
		set grade 2
	        set alarmcontent "�۷���ָ��R037	����WAPҵ��SP��ҵ����	06012 SP��ҵ���� ����WAP���ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�8����ͷ�������ſ��˷�Χ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R037	����WAPҵ��SP��ҵ����	06012 SP��ҵ���� ����WAP���ػ򱾵�ȫ��ҵ���SP��ҵ�����ԡ�9����ͷ������ʡ�ԡ�8����ͷ"


	return 0
}




	
#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
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
#--------------------------------------------------------------------------------------------------------------
#   	set sql_buff "INSERT INTO bass1.G_REPORT_CHECK(TIME_ID,RULE_ID,FLAG,RET_VAL) VALUES
#		(                                             
#			$op_month,
#			'B10',
#			1,
#			'$RESULT_VAL')"
#exec_sql $sql_buff
  
 

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------
#set RESULT_VAL [get_single $sql_buff]
#puts "10:�������żƷ���  $RESULT_VAL"
