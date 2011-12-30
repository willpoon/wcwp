######################################################################################################
#�ӿ����ƣ�SIM��
#�ӿڱ��룺06004
#�ӿ�˵����SIM����Subscriber Identity Module����ָ���ú���CPU�ļ��ɵ�·оƬ�����ܿ���
#          ����ΪGSM�ƶ�ͨ�������û������ʶ�𿨣��������û���ݼ���ͨѶ��Ϣ���ܡ����ݴ洢������
#          ��ʵ���¼CMCC������Ӫ��˾ӵ�е�ȫ��SIM����Ϣ��
#��������: G_I_06004_MONTH.tcl
#��������: ����06004������
#��������: ��
#Դ    ��1.bass2.dwd_res_sim_yyyymmdd(����Դ)
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�tym
#��дʱ�䣺2007-03-22
#�����¼��1.SIM���������ͱ���Ŀǰ���ܳ����Ϳ�
#          2.��ΪĿǰԴ���STSĿǰ����ȷ���Ƿ���ϼ���Ҫ�����Գ�����CASE��д
#          3.���м������
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06004_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #������ʱ��
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_06004_month_tmp
	              (
                        sim_iccid         character(20)  ,
                        sim_bus_srv_id    character(2)   ,
                        cmcc_id           character(5)   ,
                        sim_card_status   character(2)   ,
                        chnl_id           character(20)
	              )
	              partitioning key
	              (sim_iccid)
	              using hashing
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#����ʱ��������غ������
        set handle [aidb_open $conn]
	set sql_buff "insert into  session.g_i_06004_month_tmp
                       select
                         case
                           when a.sim_id is null then '89860'||imsi
                           else a.sim_id
                         end
                         ,case
                           when a.main_flag=1 then '07'
                           else '01'
                          end
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0001',CHAR(a.sts)),'39')
                         ,char(a.channel_id)
                       from
                         bass2.dwd_res_sim_$this_month_last_day a,
                         (select sim_id,max(res_date) as  res_date,max(channel_id) as channel_id 
                          from bass2.dwd_res_sim_$this_month_last_day
                          group by sim_id
                         ) b
                       where 
                         a.sim_id=b.sim_id 
                         and a.res_date=b.res_date and a.channel_id=b.channel_id 
                       group by 
                         case
                           when a.sim_id is null then '89860'||imsi
                           else a.sim_id
                         end
                         ,case
                           when a.main_flag=1 then '07'
                           else '01'
                          end
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0001',CHAR(a.sts)),'39')
                         ,char(a.channel_id) "                      
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #���ܵ������
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06004_month
                        select
                          $op_month
                          ,sim_iccid
                          ,sim_bus_srv_id
                          ,' '
                          ,cmcc_id
                          ,sim_card_status
                          ,chnl_id
                        from
                          session.g_i_06004_month_tmp"
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