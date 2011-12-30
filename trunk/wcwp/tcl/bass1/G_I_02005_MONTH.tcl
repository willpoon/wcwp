######################################################################################################
#�ӿ����ƣ���ͻ�/���û�
#�ӿڱ��룺02005
#�ӿ�˵������¼���û���Ϊ��ͻ�ʱ��Ӧ�ļ�����Ϣ�Ϳͻ�������Ϣ����ע���������µ����һ��������������
#          ��ͻ����Լ������������ĸ��˴�ͻ�����
#��������: G_I_02005_MONTH.tcl
#��������: ����02005������
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymm(�û������±�)
#          2.bass2.dwd_vipcust_manager_rela_yyyymmdd(��ͻ��Ϳͻ�����Ķ�Ӧ��ϵ)
#          3.bass2.dwd_cust_vip_card_yyyymm(��ͻ�����Ϣ)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.�ýӿڵ��߼����ܹ����ֵ����������ĸ��˴�ͻ�
#          2."��Ϊ���û�������"�����ڣ�Ŀǰû��ר�ŵ��ֶΣ�����ֻ���ÿ�����Ч�����������΢��Щƫ�
#          3.Ŀǰ����һ����ͻ���2����2�����ϵĴ�ͻ���������Υ���ýӿڴ�ͻ���ʶΨһ��У�顣
#            ����ֻ�ʹ�ͻ��Ϳͻ�����Ķ�Ӧ��ϵ������Ч������С�ļ�¼��Ӧ�Ŀͻ�����
#�޸���ʷ: 1.20100120 �����ͻ��ھ��䶯(��ԭ�Ȳ���������������Ԥ�������뱣���ں����ݿ��ͻ�����Ϊ�����ͻ�)
#            ԭ�д���û�޳����ԣ��ּ��ϡ�
#
#�޸���ʷ: 2.2011-04-04 ȥ�� vip_source=0  ��������Ϊnull(��������) ��Ҳ������
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02005_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02005_month
                      select 
                        $op_month
                        ,a.user_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0013',char(a.custclass_id)),'4')
                        ,char(max(bigint(b.manager_id)))
                        ,replace(char(date(c.valid_date)),'-','')
                        ,case 
                           when a.custclass_id in (1,2,3,5) then '0' 
                           else '1' 
                          end
                      from 
                        (
                         select 
                           user_id,
                           cust_id,
                           custclass_id
                         from 
                           bass2.dw_product_$op_month 
                         where 
                           custclass_id in (1,2,3,5,7,8,9)
                           and userstatus_id in (1,2,3,6,8) 
                           and usertype_id in (1,2,9)
                           and vip_mark=1
                           and crm_brand_id1=1
                           and test_mark<>1
                        )a,
                        (
                         select 
                           cust_id,
                           manager_id,
                           min(valid_date)
                         from bass2.dwd_vipcust_manager_rela_$this_month_last_day 
                         where rec_status=1 
                         group by cust_id,manager_id
                        )b,
                        (
                          select 
                            user_id,
                            min(card_valid_date) as valid_date
                          from bass2.dwd_cust_vip_card_$op_month 
                          where 
                              rec_status=1
                          group by user_id
                        )c
                     where  a.cust_id=b.cust_id
                        and a.user_id=c.user_id 
                     group by 
                        a.user_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0013',char(a.custclass_id)),'4')
                        ,replace(char(date(c.valid_date)),'-','')
                        ,case 
                           when a.custclass_id in (1,2,3,5) then '0' 
                           else '1' 
                          end "
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

#################�ο�
#                         select 
#                           user_id,
#                           cust_id,
#                           custclass_id
#                         from 
#                           bass2.dw_product_$op_month 
#                         where 
#                           custclass_id in (1,2,3,5,7,8,9) --��ʯ�����𿨡��������������TS��ʯ����TS�𿨡�TS�����
#                           and userstatus_id in (1,2,3,6)  --�����û�
#                           and usertype_id in (1,2,9) --�������û�
#                           and vip_mark=1
#                           and crm_brand_id1=1
#                        )a,
#                        (
#                         select 
#                           cust_id,
#                           manager_id,
#                           min(valid_date)
#                         from bass2.dwd_vipcust_manager_rela_$this_month_last_day 
#                         where rec_status=1 --����
#                         group by cust_id,manager_id
#                        )b,
#                        (
#                          select 
#                            user_id,
#                            min(card_valid_date) as valid_date
#                          from bass2.dwd_cust_vip_card_$op_month 
#                          where vip_source=0 --���˴�ͻ�
#                             and rec_status=1 --��ǰ�û�
#                             --and bigint(replace(char(date(CARD_EXPIRE_DATE)),'-',''))<=$this_month_last_day
#                          group by user_id
#                        )c
#                     where  a.cust_id=b.cust_id
#                        and a.user_id=c.user_id
#################################################                     