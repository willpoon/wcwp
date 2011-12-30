######################################################################################################
#�ӿ����ƣ����ѽɷѼ�¼
#�ӿڱ��룺03006
#�ӿ�˵�����ͻ����ɻ��ѷ��õļ�¼�������ͻ����ɻ���Ԥ��
#��������: G_S_03006_DAY.tcl
#��������: ����03006������
#��������: ��
#Դ    ��1.bass2.dw_acct_payitem_dm_yyyymm(����ɷѼ�¼��)     
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.�ɷ��д���Ϊ���ſͻ�����Щ�û�product_noΪ0�����Զ�����������ʱ��
#            ���ܹ�����user_id����������user_idΪNULLֵ������һ���ڳ������޳���
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #���� yyyy-mm-dd
	set optime $op_time
	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_03006_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_03006_day
           ( 
              time_id       
              ,channel_id   
              ,pay_meth     
              ,pay_date     
              ,pay_time     
              ,owe_fee      
              ,acct_id      
              ,chzh_id      
              ,user_id 
             )
           select
             $timestamp
             ,char(so_channel_id)
             ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(paytype_id)),'99')
             ,replace(char(date(pay_date)),'-','')
             ,replace(char(time(pay_date)),'.','')
             ,char(sum(case 
                        when rec_sts=1 then bigint(round(-recv_cash*100,0))
                        else bigint(round(recv_cash*100,0))
              end ))
             ,acct_id
             ,char(rec_sts)
             ,user_id
           from 
             bass2.dw_acct_payitem_dm_${op_month} 
           where 
             op_time=date('${optime}')
             and user_id is not null
           group by 
             char(so_channel_id)
             ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(paytype_id)),'99')
             ,replace(char(date(pay_date)),'-','')
             ,replace(char(time(pay_date)),'.','')
             ,acct_id
             ,char(rec_sts)
             ,user_id   "       
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