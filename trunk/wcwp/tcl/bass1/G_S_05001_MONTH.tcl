######################################################################################################
#�ӿ����ƣ�������������
#�ӿڱ��룺05001
#�ӿ�˵������¼�й��ƶ���������Ӫ��֮�������������Ϣ��
#��������: G_S_05001_MONTH.tcl
#��������: ����05001������
#��������: ��
#Դ    ��1. bass2.dw_js_acct_dm_yyyymm(�����ʵ�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��ΪBOSSԴ��WJJS.MB_INTERFACE_JYFX_PARAֻ�����ֳ����е�ö��ֵ(N:��ͨ,R:��ͨ,T:����,U:��ͨ),
#            ���Բ���ϸ�ֵ����Ź淶�涨�ġ�����Է���Ӫ��Ʒ�����ͱ��롱��Ҫ��
#          2.��Ϊdw_call_roamin��Դ��WJJS.MI_CDR_WJ��baltypeֵΪ-1�����Բ������㡰����ҵ�����ͱ��롱Ҫ��
#          3.Ҫ�����ػ�����ͨ�����±���.xls�˶�����
#          4.������BOSSϵͳ������ı�����Э��<���ػ�����ͨ�����±���.xls>,����֪�ñ���ͳ�Ʒǳ�����
#            ��Ҫ�ȵ�7�·�BOSSϵͳ����ϵͳ��˵��������ʱ���γ���
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


#        #���� yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
#      
#        #ɾ����������
#        set handle [aidb_open $conn]
#	set sql_buff "delete from bass1.g_s_05001_month where time_id=$op_month"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#        
##������ʱ��
#        set handle [aidb_open $conn]
#	set sql_buff "declare global temporary table session.g_s_05001_month_tmp
#	              (
#                        self_cmcc_code              varchar(5),  
#                        self_svc_brnd_id            varchar(1),
#                        other_cmcc_code             varchar(6),
#                        other_svc_brnd_id           varchar(6),
#                        stlmnt_fee_item_id          varchar(2),
#                        out_durn                    bigint,         
#                        in_durn                     bigint,
#                        stlmnt_fee                  bigint,
#                        pay_stlmnt_fee              bigint                                      
#	              )with replace on commit preserve rows not logged in tbs_user_temp"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#������ʱ��
#        set handle [aidb_open $conn]
#	set sql_buff "insert into session.g_s_05001_month_tmp
#                      select 
#                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(feecode)),'13100')
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(selftype)),'2')
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0069',char(opertype)),'990000')
#                        ,case
#                           when baltypeid='N' then '041000'
#                           when baltypeid='R' then '051000'
#                           when baltypeid='U' then '021000'
#                           else '031000'
#                         end
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0079',char(baltype)),'10')
#                        ,case
#                           when BALMODE=0 then sum(chrgmin)
#                           else 0
#                          end
#                         ,case
#                            when BALMODE=1 then sum(chrgmin)
#                            else 0     
#                          end
#                         ,case 
#                            when BALMODE=1 then sum(balfee)*10
#                            else 0
#                           end
#                         ,case 
#                            when BALMODE=0 then sum(balfee)*10
#                             else 0
#                          end 
#                       from bass2.dw_js_acct_dm_$op_month
#                       group by
#                           coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(feecode)),'13100')
#                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(selftype)),'2')
#                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0069',char(opertype)),'990000')
#                           ,case
#                             when baltypeid='N' then '041000'
#                             when baltypeid='R' then '051000'
#                             when baltypeid='U' then '021000'
#                             else '031000'
#                            end
#                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0079',char(baltype)),'10') 
#                          ,BALMODE "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#        
#        #���ܵ������
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_s_05001_month
#                      select 
#                        $op_month
#                        ,'$op_month'
#                        ,self_cmcc_code
#                        ,self_svc_brnd_id
#                        ,other_cmcc_code
#                        ,other_svc_brnd_id
#                        ,stlmnt_fee_item_id
#                        ,char(sum(out_durn))
#                        ,char(sum(in_durn))
#                        ,char(sum(stlmnt_fee))
#                        ,char(sum(pay_stlmnt_fee))                        
#                      from session.g_s_05001_month_tmp
#                      group by
#                        self_cmcc_code
#                        ,self_svc_brnd_id
#                        ,other_cmcc_code
#                        ,other_svc_brnd_id
#                        ,stlmnt_fee_item_id "
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