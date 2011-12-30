######################################################################################################
#�ӿ����ƣ�ҵ����������
#�ӿڱ��룺22025
#�ӿ�˵������¼ҵ�����������������Ϣ������"02  �ͻ�ҵ���������ͱ���" ����άֵ�����ṩ��
#          ���Ѳ�ѯ���ṩ�ʵ�/�굥��������ʾҵ�񿪡�GPRSҵ�񿪡�WLAN ҵ�񿪡��ƶ����鿪���������俪��
#          ��������ҵ�񿪡�����ҵ���ܿ�������ʾҵ��ء�GPRSҵ��ء�WLAN ҵ��ء��ƶ�����ء���������ء�
#          ��������ҵ��ء�����ҵ���ܹء��������Ŷ��ơ���������ȡ�����������Ŷ��ơ���������ȡ����
#          ����ҵ���ơ�����ҵ��ȡ����ͣ����������������롢�ͻ���Ϣ������ɷѿ���ֵ���ʼ��˵���
#          ת��ҵ��Ʒ��/�Żݣ������ֲ�ѯ�����ֶҽ���������ҵ�񡢿���ҵ��ԤԼ����SIM����
#��������: G_S_22025_MONTH.tcl
#��������: ����22025������
#��������: ��
#Դ    ��1.bass2.dw_product_busi_dm_yyyymm(������ˮ��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.20110105 liuqf ����busi_code��so_mode�䶯���ھ����б䶯
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22025_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
     
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22025_month
                   select  $op_month,
                           '$op_month',
                          case when busi_code in (1079) then '0101'
													     when busi_code in (1040) then '0103'
													     when busi_code in (1198) then '0104'
													     when busi_code in (1196) then '0105'
													     when busi_code in (1101) then '0112'
													     when busi_code in (2210) then '0119'
													     when busi_code in (1046,1047,1048) then '0301'
													     when busi_code in (1001) then '0302'
													     when busi_code in (1002) then '0303'
													     when busi_code in (1022) then '0304'
													     when busi_code in (1021) then '0306'
													     when busi_code in (1011,1013,1015) then '0308'
													     when busi_code in (1006) then '0309'
													     when busi_code in (1206) then '0310'
													     when busi_code in (2602) then '0312'
													     when busi_code in (2737) then '0318'
													     when busi_code in (1070) then '0321'
													     when busi_code in (1080) then '0322'
													     when busi_code in (1415) then '0325'
													     when busi_code in (1071) then '0328'
													     when busi_code in (1084) then '0329'
													     when busi_code in (1416) then '0332'
													else '9999' 
													end as bus_type_id,
                            case  when so_mode in ('5') then '01'
														      when so_mode in ('7') then '08'
														      when so_mode in ('0','1','2') then '11'
														      else '99' 
														end as CHNL_TYPE_ID,
                            char(count(*)) BUS_FREQ         
                     from   bass2.dw_product_busi_dm_$op_month
                  group by  case when busi_code in (1079) then '0101'
														     when busi_code in (1040) then '0103'
														     when busi_code in (1198) then '0104'
														     when busi_code in (1196) then '0105'
														     when busi_code in (1101) then '0112'
														     when busi_code in (2210) then '0119'
														     when busi_code in (1046,1047,1048) then '0301'
														     when busi_code in (1001) then '0302'
														     when busi_code in (1002) then '0303'
														     when busi_code in (1022) then '0304'
														     when busi_code in (1021) then '0306'
														     when busi_code in (1011,1013,1015) then '0308'
														     when busi_code in (1006) then '0309'
														     when busi_code in (1206) then '0310'
														     when busi_code in (2602) then '0312'
														     when busi_code in (2737) then '0318'
														     when busi_code in (1070) then '0321'
														     when busi_code in (1080) then '0322'
														     when busi_code in (1415) then '0325'
														     when busi_code in (1071) then '0328'
														     when busi_code in (1084) then '0329'
														     when busi_code in (1416) then '0332'
														else '9999' 
														end,
														case  when so_mode in ('5') then '01'
														      when so_mode in ('7') then '08'
														      when so_mode in ('0','1','2') then '11'
														      else '99' 
														end
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

	return 0
}