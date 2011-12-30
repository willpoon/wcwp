######################################################################################################
#�ӿ����ƣ�22052	���еش�Ʒ�ƿ�չ���
#�ӿڱ��룺22052
#�ӿ�˵������¼���еش�Ʒ�ƿ�չ�����
#��������: G_S_22052_MONTH.tcl
#��������: ����22052������
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymm
#          2.BASS2.DW_ACCT_SHOULDITEM_YYYYMM �˵��±�
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xiahuaxue
#��дʱ�䣺2008-10-30
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month
              

        #���µ�һ�� yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22052_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

	   
  #���� ���еش��˾������ײͷ�  
	set sql_buff "insert into bass1.g_s_22052_month 
                     select
                        88888888
                        ,'$op_month'
                        ,char(int(sum(should_fee)/count(distinct b.user_id)*100))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'                       

                     from BASS2.DW_ACCT_SHOULDITEM_$op_month a,
                          bass2.dw_product_$op_month b
	                  where a.user_id = b.user_id and b.crm_brand_id3 in (200,1400,2100)
	                       and a.item_id = 80000011 "
                        
  puts $sql_buff
  exec_sql $sql_buff
  
  #���� ���еش������ײ��˾������ײͷ�  
	set sql_buff "insert into bass1.g_s_22052_month 
                     select
                        88888888
                        ,'$op_month'
                        ,'0'
                        ,char(int(sum(should_fee)/count(distinct a.user_id)*100))
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'
                        ,'0'                       

                     from BASS2.DW_ACCT_SHOULDITEM_$op_month a,
                          bass2.dw_product_$op_month b
	                  where a.user_id = b.user_id and b.crm_brand_id3 in (2100)
	                       and a.item_id = 80000011 "
  puts $sql_buff
  exec_sql $sql_buff



	set sql_buff "insert into bass1.g_s_22052_month
                     select
                        $op_month
                        ,'$op_month'
                        ,char(sum(bigint(AvgPlanFee)))        
                        ,char(sum(bigint(AvgMusicPlanFee)))   
                        ,char(sum(bigint(LMSJCount)))         
                        ,char(sum(bigint(LMSJWDCount)))       
                        ,char(sum(bigint(YLLMSJCount)))       
                        ,char(sum(bigint(CYLMSJCount)))       
                        ,char(sum(bigint(GWLMSJCount)))       
                        ,char(sum(bigint(WTLMSJCount)))       
                        ,char(sum(bigint(QTLMJCount)))        
                        ,char(sum(bigint(LMSJPJFWKHCount)))   
                        ,char(sum(bigint(LMSJPJXFMZ)))        
                        ,char(sum(bigint(LMSJPJYJ)))          
                        ,char(sum(bigint(ThisMonthMXZYYYJ)))  
                        ,char(sum(bigint(ThisYearMXZYYYJ)))   
                        ,char(sum(bigint(SIMCost)))           
                        ,char(sum(bigint(MYYMJDPMTR)))        
                        ,char(sum(bigint(MYYMJDDSTR)))        
                        ,char(sum(bigint(MYYMJDHLWTR)))       
                        ,char(sum(bigint(MYYMJDQTMTTR)))      
                        ,char(sum(bigint(MYYMJDLJPMTR)))      
                        ,char(sum(bigint(MYYMJDLJDSTR)))      
                        ,char(sum(bigint(MYYMJDLJHLWTR)))     
                        ,char(sum(bigint(MYYMJDLJQTMTTR)))    
                        ,char(sum(bigint(MYYMJDWHHDTR)))      
                        ,char(sum(bigint(MYYMJDLMSJYJ)))    
                        ,char(sum(bigint(MYYMJDCXCost)))      
                        ,char(sum(bigint(MYYMJDZRJE)))        
                        ,char(sum(bigint(DGDDPPXXDCount)))    
                        ,char(sum(bigint(GXXQCount)))       
  
                      from bass1.g_s_22052_month
                     where time_id = 88888888 "
                        
  puts $sql_buff
  exec_sql $sql_buff
  

	set sql_buff "delete from bass1.g_s_22052_month where time_id=88888888"
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



