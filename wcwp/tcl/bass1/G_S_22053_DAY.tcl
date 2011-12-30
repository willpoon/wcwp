######################################################################################################
#�ӿ����ƣ�ʵ�������ջ���
#�ӿڱ��룺22053
#�ӿ�˵������¼�й��ƶ�����ʵ����������ҵ����ջ��������
#��������: G_S_22053_DAY.tcl
#��������: ����22053������
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymmdd
#          2.dwd_product_busi_yyyymmdd ������ˮ��
#	         3.dwd_acct_busicharge_yyyymmdd  Ӫҵ�ɷѼ�¼��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xiahuaxue
#��дʱ�䣺2008-10-14
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22053_day where time_id=$timestamp"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� �����ͻ���  
	set sql_buff "insert into bass1.g_s_22053_day
                     select
                        88888888
                        ,'$timestamp'
                        ,char(Channel_ID)
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$timestamp
                     where day_new_mark = 1
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff
  
  #���� ����ȫ��ͨ�ͻ��� ����
	set sql_buff "insert into bass1.g_s_22053_day
                     select
                        88888888
                        ,'$timestamp'
                        ,char(Channel_ID)
                        ,'0'
                        ,char(count(distinct user_id))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                      from bass2.dw_product_$timestamp
                     where coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') = '1' and  day_new_mark = 1
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff



  #���� ҵ������� ����
	set sql_buff "insert into bass1.g_s_22053_day
                     select
                        88888888
                        ,'$timestamp'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,char(count(distinct SO_NBR))
                        ,'0'
                      from bass2.dwd_product_busi_$timestamp
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff




  #���� �ɷѱ��� ����
	set sql_buff "insert into bass1.g_s_22053_day
                     select
                        88888888
                        ,'$timestamp'
                        ,char(Channel_ID)
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,char(count(distinct SO_NBR))
                      from bass2.dwd_acct_busicharge_$timestamp
                     group by Channel_ID"
                        
  puts $sql_buff
  exec_sql $sql_buff



	set sql_buff "insert into bass1.g_s_22053_day
                     select
                        $timestamp
                        ,'$timestamp'
                        ,value(Channel_ID,'99999999')
                        ,char(sum(bigint(NewCustCount     )))
                        ,char(sum(bigint(NewQQTCustCount  )))
                        ,char(sum(bigint(TerminalCount    )))
                        ,char(sum(bigint(GPRSTerminalCount)))
                        ,char(sum(bigint(OperCount        )))
                        ,char(sum(bigint(ChargeCount      )))
                      from bass1.g_s_22053_day
                     where time_id = 88888888
                     group by value(Channel_ID,'99999999')"
                        
  puts $sql_buff
  exec_sql $sql_buff
  

	set sql_buff "delete from bass1.g_s_22053_day where time_id=88888888"
	puts $sql_buff
  exec_sql $sql_buff


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



