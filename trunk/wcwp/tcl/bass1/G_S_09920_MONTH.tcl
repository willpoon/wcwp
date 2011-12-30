######################################################################################################
#�ӿ����ƣ����������ƶ��绰�������
#�ӿڱ��룺09920
#�ӿ�˵�����������֣����ź���ͨ���ƶ��绰����ĳ������ݡ�
#��������: G_S_09920_MONTH.tcl
#��������:  
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺20090710
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
			 #����  yyyymm
			 set last_month [GetLastMonth [string range $op_month 0 5]]
			 #������ yyyymm
			 set last_last_month [GetLastMonth [string range $last_month 0 5]] 
           
       
  #ɾ����������
	set sql_buff "delete from bass1.G_S_09920_MONTH where time_id=$op_month"
	puts $sql_buff
	exec_sql $sql_buff
	 
 
  #������ʱ��1
	set sql_buff "	declare global temporary table session.G_S_09920_MONTH_tmp1 
									(
								  COMP_PRODUCT_NO      CHARACTER(15),
								  COMP_TYPE            CHARACTER(1)
									)                            
									partitioning key           
									 (
									   COMP_PRODUCT_NO    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	puts $sql_buff
	exec_sql $sql_buff
	
	
	  #������ʱ��2
	set sql_buff "	declare global temporary table session.G_S_09920_MONTH_tmp2 
									(
								  COMP_PRODUCT_NO      CHARACTER(15),
								  COMP_TYPE            CHARACTER(1)
									)                            
									partitioning key           
									 (
									   COMP_PRODUCT_NO    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	puts $sql_buff
	exec_sql $sql_buff
	
	  #������ʱ��3
	set sql_buff "	declare global temporary table session.G_S_09920_MONTH_tmp3 
									(
								  COMP_PRODUCT_NO      CHARACTER(15),
								  COMP_TYPE            CHARACTER(1)
									)                            
									partitioning key           
									 (
									   COMP_PRODUCT_NO    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	puts $sql_buff
	exec_sql $sql_buff
	
  #������ʱ��4
	set sql_buff "	declare global temporary table session.G_S_09920_MONTH_tmp4 
									(
								  COMP_PRODUCT_NO      CHARACTER(15),
								  COMP_TYPE            CHARACTER(1)
									)                            
									partitioning key           
									 (
									   COMP_PRODUCT_NO    
									 ) using hashing           
									with replace on commit preserve rows not logged in tbs_user_temp  "
	puts $sql_buff
	exec_sql $sql_buff
	
	#�������ϸ��������ͻ�����ͨ�����й����Ż��й���ͨ�ֻ����� 
    set sql_buff "insert into session.G_S_09920_MONTH_tmp1 
									select a.comp_product_no
												,case when a.comp_brand_id in (3,4) then '1' else '0' end  COMP_TYPE
									from bass2.dw_comp_cust_$last_last_month a
									where (month_in_call_counts+month_out_call_counts)>0 
												       and COMP_BRAND_ID in (3,4,9,10,11)  "
  	puts $sql_buff
	  exec_sql $sql_buff
	
	#�����ϸ��������ͻ�����ͨ�����й����Ż��й���ͨ�ֻ����� 
    set sql_buff "insert into session.G_S_09920_MONTH_tmp2 
									select a.comp_product_no
												,case when a.comp_brand_id in (3,4) then '1' else '0' end  COMP_TYPE
									from bass2.dw_comp_cust_$last_month a
									where (month_in_call_counts+month_out_call_counts)>0 
												       and COMP_BRAND_ID in (3,4,9,10,11)  "
  	puts $sql_buff
	  exec_sql $sql_buff	

	#���뵱�������ͻ�����ͨ�����й����Ż��й���ͨ�ֻ����� 
    set sql_buff "insert into session.G_S_09920_MONTH_tmp3 
									select a.comp_product_no
												,case when a.comp_brand_id in (3,4) then '1' else '0' end  COMP_TYPE
									from bass2.dw_comp_cust_$op_month a
									where (month_in_call_counts+month_out_call_counts)>0 
												       and COMP_BRAND_ID in (3,4,9,10,11)  "
  	puts $sql_buff
	  exec_sql $sql_buff


	#����������������ͻ�����ͨ�����й����Ż��й���ͨ�ֻ����� ��ȥ���ѳ�ȡ�ĺ���
    set sql_buff "insert into session.G_S_09920_MONTH_tmp4
									select a.comp_product_no
												,a.comp_type
									from session.G_S_09920_MONTH_tmp3  a
									inner join session.G_S_09920_MONTH_tmp2 b on a.comp_product_no=b.comp_product_no
									inner join session.G_S_09920_MONTH_tmp1 c on a.comp_product_no=c.comp_product_no 
									left join BASS1.G_S_09920_MONTH	 d on a.comp_product_no=d.comp_product_no
									where d.comp_product_no is null"
  	puts $sql_buff
	  exec_sql $sql_buff
	  
	  
	#============����ʽ���в��� ����============================
	#��ͨ����
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_09920_MONTH 
                  select ${op_month} 
                        ,comp_product_no
                        ,comp_type  
                 from session.G_S_09920_MONTH_tmp4 
								 where comp_type='1'
								 order by rand() fetch first 10000 rows only;"   
           puts $sql_buff                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}	
	aidb_commit $conn

	#���ź���
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_09920_MONTH 
                  select ${op_month} 
                        ,comp_product_no
                        ,comp_type  
                 from session.G_S_09920_MONTH_tmp4 
								 where comp_type='0'
								 order by rand() fetch first 10000 rows only;"   
           puts $sql_buff                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}	
	aidb_commit $conn 

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

