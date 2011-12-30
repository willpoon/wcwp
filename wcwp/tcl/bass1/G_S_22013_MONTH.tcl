######################################################################################################
#�ӿ����ƣ���KPI
#�ӿڱ��룺22013
#�ӿ�˵������¼��KPI��Ϣ
#��������: G_S_22013_MONTH.tcl
#��������: ����22013������
#��������: ��
#Դ    ��1.bass2.Dw_comp_cust_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 20090428 modify by zhanght ɾ��UNION_AWAY_NUMBER�ֶ�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          

	#�������һ�� yyyymmdd
  set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     

  set fuhao "-"
  puts $fuhao
	#�������һ��  yyyy-mm-dd
	set op_month_last_date [string range $this_month_last_day 0 3]$fuhao[string range $this_month_last_day 4 5]$fuhao[string range $this_month_last_day 6 7]
  puts $op_month_last_date  
     
     
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22013_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
#  BILL_MONTH         CHARACTER(6),
#  UNION_AWAY_NUMBER  CHARACTER(10),
#  NXT200701LJ        CHARACTER(10), --��07��1�·��ۼ�ʹ��ũ��ͨҵ����û���
#  DX_NUMBER          CHARACTER(10), --���¼��Ÿ��˿ͻ�����ҵ����
#  NXT_ONEYEAR        CHARACTER(10)  --��һ�����ۼ�ʹ��ũ��ͨҵ����û���

#	#��ͨ�����ͻ���
#        set handle [aidb_open $conn]
#        set sql_buff "\
#	            select
#	              count(distinct  comp_product_no)
#	            from 
#	              bass2.Dw_comp_cust_$op_month
#	            where	             
#	              comp_brand_id in (3,4,7)
#	              and  comp_month_off_mark=1 "     
#        puts $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set UNION_AWAY_NUMBER [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "��ͨ�����ͻ���:$UNION_AWAY_NUMBER"
		
		
	#���¼��Ÿ��˿ͻ�����ҵ����
	set sql_buff " select
            sum(b.COUNTS)
       from bass2.dw_enterprise_member_mid_$op_month a,(select user_id,sum(counts)as COUNTS from bass2.DW_NEWBUSI_SMS_$op_month group by user_id
                                               union all
                                               select user_id,sum(counts) as COUNTS from bass2.DW_NEWBUSI_ISMG_$op_month group by user_id
                                              ) b
       where a.dummy_mark = 0 and a.user_id=b.user_id "
  puts $sql_buff
  set DX_NUMBER [get_single $sql_buff]	
  

  #��07��1�·��ۼ�ʹ��ũ��ͨҵ����û���
	set sql_buff " select  
                   count(distinct a.user_id)
                   from bass2.dw_enterprise_industry_apply a
                   where  a.apptype_id in (3) and op_time < '$op_month_last_date' "
  puts $sql_buff
  set NXT200701LJ [get_single $sql_buff]	
  
  
  
	#��һ�����ۼ�ʹ��ũ��ͨҵ����û���	
	set sql_buff " select count(distinct a.user_id) from bass2.dw_enterprise_industry_apply a where  a.apptype_id in (3) and op_time >= '2008-01-01'"
  puts $sql_buff
  set NXT_ONEYEAR [get_single $sql_buff]	
		
		
  #���
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22013_month values
	             (
	                $op_month
	                ,'$op_month'	 
			            ,'$DX_NUMBER'
			            ,'$NXT200701LJ'
			            ,'$NXT_ONEYEAR'            
	             ) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}             

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
    