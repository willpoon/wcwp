######################################################################################################
#�ӿ����ƣ�ͨ���û��ۻ�
#�ӿڱ��룺
#�ӿ�˵����ͨ���û��ۻ�
#��������: INT_22038_YYYYMM.tcl
#��������: Ŀǰ��22038�ӿ�ʹ��
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymmdd
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 20090901 ���壺ͳ�������ڲ�����������������ͨ������������������VPMN����
#��Ե�������С���Ե�������С�GPRS���������ź˼�������������0���Ŀͻ������жϿͻ��Ƿ�����
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $op_time 8 9]
        #���µ�һ�� yyyymmdd
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        set this_month_first_day [string range $op_month 0 5]01

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.int_22038_$op_month where op_time=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
 ###��ͨ����(����������������)
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_call_dtl_$timestamp"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
 ###��Ե��������
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,case when user_id='89160000038050' then 4 else brand_id end
	               ,user_id
                from bass2.cdr_sms_dtl_$timestamp
               where calltype_id=0
                 and svcitem_id in (200001,200002,200003,200004,200005,200006)"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	

 ###��Ե��������
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_mms_dtl_$timestamp
               where calltype_id=0
                 and svcitem_id in (400001,400002)"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

 ###GPRS���������ź˼�������������0��
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_gprs_dtl_$timestamp
                where svcitem_id not in (400001,400002,400003,400004,400005,400006)
                  and bigint(upflow1)+bigint(upflow2)+bigint(downflow1)+bigint(downflow2)>0"
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
