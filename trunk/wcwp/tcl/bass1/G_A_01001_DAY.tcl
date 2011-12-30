######################################################################################################
#�ӿ����ƣ��ͻ�
#�ӿڱ��룺01001
#�ӿ�˵�����ͻ������й��ƶ����ܻ��Ѿ���ȡ�����ϵ����������ͻ����Ѿ���ȡ�����ϵĲ������ͻ���������
#          �������������Ŀͻ��Լ�δ��������Ǳ�ڿͻ����ı�ʶ��
#��������: G_A_01001_DAY.tcl
#��������: ����01001������
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymmdd
#          2.bass2.dwd_enterprise_msg_yyyymmdd
#          3.bass1.g_a_01001_day
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
	      puts $op_time
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_01001_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
          
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_01001_day
                      (
                      time_id                        
                      ,cust_id                        
                      ,org_type_id                    
                      ) 
                     (select
                        $timestamp
                        ,cust_id
                        ,'1'
                      from
                        bass2.dw_product_$timestamp
                      where 
                        enterprise_mark=0
                        and test_mark=0
                        and usertype_id in (1,2,9)
                      except
                      select
                        $timestamp
                        ,cust_id
                        ,'1'
                      from
                        bass1.g_a_01001_day
                      where
                        time_id<$timestamp 
                        and org_type_id='1')
                      union all
                      (select
                        $timestamp
                        ,enterprise_id
                        ,'2'
                      from
                        bass2.dwd_enterprise_msg_$timestamp
                      where 
                        group_status=0
                      except
                      select
                        $timestamp
                        ,cust_id
                        ,'2'
                      from
                        bass1.g_a_01001_day
                      where
                        time_id<$timestamp
                        and org_type_id='2') "   
                        

                        


                                                                                          
#                      select
#                        $timestamp
#                        ,cust_id
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0029',char(custtype_id)),'1') as org_type_id
#                      from 
#                        bass2.dwd_cust_msg_$timestamp
#                      except
#                      select 
#                        $timestamp
#                        ,cust_id
#                        ,org_type_id
#                      from 
#                        bass1.g_a_01001_day"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle

	return 0
}