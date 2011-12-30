######################################################################################################
#�ӿ����ƣ������û���Ա
#�ӿڱ��룺02049
#�ӿ�˵�������弯�ſͻ���ʹ���й��ƶ�����ĸ����û���Ա��Ϣ��
#��������: G_I_02049_MONTH.tcl
#��������: ����02049������
#��������: ��
#Դ    ��1.bass2.dw_enterprise_member_mid_yyyymm(���ſͻ���Ա�м��)
#          2.bass2.dw_product_yyyymm(�û�����)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.�ýӿ���L3��L4��У�飬����Ӧ��ֻ�ϴ������û��������û����ϴ�
#          2.�ýӿ�����ļ��ſͻ���2339������ϵͳ�Ķ�6����ԭ���������ϵͳ�����ݲ�׼ȷ��Ҫ�����·ݵ����ݡ�
#�޸���ʷ: 1.8�·ݷ���Υ����L3У��,����L3У����where�������������ų������û�������SIM���û��������ڵ���.@20070903 BY TYM 
#          2. ��д����
#          3.�޳����������Ա usertype_id=8 ���û���ֻץȡ�����ģ���֤���ݸ�02004�ӿڵ�һ����>�޸��ˣ�zhanght �޸�ʱ�䣺2009-05-26
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02049_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02049_month
                    select
                      $op_month
                      ,value(a.enterprise_id,' ')
                      ,a.user_id
                   from 
                     bass2.dw_enterprise_member_mid_$op_month a,
                     bass2.dw_enterprise_msg_$op_month b ,
                     bass2.dw_product_$op_month c
                   where  a.enterprise_id = b.enterprise_id
                      and a.user_id=c.user_id
                      and c.usertype_id in (1,2,3,6)
                    "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

aidb_runstats bass1.g_i_02049_month 3
  
#    set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_i_02049_month
#                    select
#                      $op_month
#                      ,a.enterprise_id
#                      ,b.user_id
#                   from 
#                     bass2.dw_enterprise_member_$op_month a,
#                     bass2.dw_product_$op_month b 
#                   where 
#                     a.custstatus_id=1 
#                     and b.userstatus_id in (1,2,3,6)
#                     and b.test_mark=0
#                     and b.crm_brand_id2<>70
#                     and a.cust_id=b.cust_id  "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle

	return 0
}