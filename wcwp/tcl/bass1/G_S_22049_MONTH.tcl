######################################################################################################
#�ӿڵ�Ԫ���ƣ����г�ֵ������Ϣ
#�ӿڵ�Ԫ���룺22049
#�ӿڵ�Ԫ˵����ͳ�����о߱����г�ֵ���ܵ��������
#��������: G_S_22049_MONTH.tcl
#��������: ����22049������
#��������: ��
#Դ    ��1.bass2.dw_acct_payitem_yyyymm(�ɷѱ�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ��Ļ�ѧ
#��дʱ�䣺2007-10-24
#�����¼��1.
#�޸���ʷ: liuqf 1.7.0�淶20101225 ��Դ�ڳ�𱨱�ר��stat_channel_reward_0007
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
 
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22049_month where time_id=$op_month"
  exec_sql $sql_buff         



set sql_buff "insert into bass1.g_s_22049_month
select
$op_month
  ,'$op_month'
  ,a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CITY_ID)),'13101') 
  ,char(a.channel_id)
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
  ,char(bigint(sum(a.fee )))
from bass2.stat_channel_reward_0007 a
WHERE a.channel_type in (90105,90102)
  and a.op_time=$op_month
group by
  a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') 
  ,char(a.channel_id)  
  ,case when a.channel_type=90102 
	 and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end "
                       
  exec_sql $sql_buff         


	return 0
}