######################################################################################################
#�������ƣ�INT_CHECK_RUNLOG_DAY.tcl
#У��ӿڣ�ÿ��7��ͳ�����ɵĽӿ��ļ�����
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2009-09-14
#�����¼��
#�޸���ʷ: 2010.11.01 �ϳ�06030 9����սӿڣ�ͬʱ�ϳ�22053���ܹ��սӿ�55����9��Ľӿ�8��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_RUNLOG_DAY.tcl"

    puts " ɾ��������"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('RUNLOG') "        
	  exec_sql $sqlbuf     

   
   puts "ÿ��7��ͳ�����ɵĽӿ��ļ����� val3(9���ӽӿڵ�����)"
   set sqlbuf " 
						select count(distinct a.control_code) val1
						      ,value(count(distinct b.control_code),0) val2
						      ,value(count(distinct case  
						      									when substr(b.control_code,15,5) in ('01002','01004','02004','02008','02011','02053','06031','06032') 
						      									then b.control_code 
						      								 end  ),0) val3
						from app.sch_control_task a
						left join ( select control_code 
						            from app.sch_control_runlog 
						            where date(endtime)=date(current date)
						                  and flag=0 ) b on a.control_code=b.control_code
						where a.control_code like 'BASS1_EXP%DAY%' 
						      and a.cc_flag=1
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

	puts " ��У��ֵ����У����� "   
	set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'RUNLOG',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	exec_sql $sqlbuf  
	puts "��У��ֵ����У�����ɹ�"
  
  puts "���뵽������Ϣ����ͷ"
	set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
							select '��������${timestamp}����------һ���ӿ�����:${RESULT_VAL1}��,����9��ӿ���:${RESULT_VAL3}��,�����ܹ����ɵĽӿ���:${RESULT_VAL2}����',
							phone_id 
							from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
	exec_sql $sqlbuf
  puts "���뵽������Ϣ����ͷ�ɹ�"

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

