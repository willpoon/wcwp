######################################################################################################
#�ӿ����ƣ�������Ӫ�̾�Ӫ��������
#�ӿڱ��룺06014
#�ӿ�˵������¼������Ӫ�̾�Ӫ�ĵ����������͡�
#��������: G_I_06014_MONTH.tcl
#��������: ����06014������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��Ϊ���ص�����Ӫ�̺��٣�ĿǰҲû��ר�ŵı���ά����Щ��Ϣ�����Ըýӿڲ���
#          ��ϵͳ��������ֱ�Ӳ��뼸�����ݡ�
#�޸���ʷ: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]            
            
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06014_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06014_month values
                       ($op_month,'020000','01'),
                       ($op_month,'020000','02'),
                       ($op_month,'020000','03'),
                       ($op_month,'020000','04'),
                       ($op_month,'020000','05'),
                       ($op_month,'020000','06'),
                       ($op_month,'020000','10'),
                       ($op_month,'030000','01'),
                       ($op_month,'030000','05'),
                       ($op_month,'030000','11'),
                       ($op_month,'040000','01'),
                       ($op_month,'040000','05'),
                       ($op_month,'040000','11'),
                       ($op_month,'010000','02'),
                       ($op_month,'010000','04'),
                       ($op_month,'010000','05'),
                       ($op_month,'050000','01'),
                       ($op_month,'050000','05') "
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
#####################�ο�##############################
#��ͨ��020000
#01	�̶�
#02	GSM
#03	CDMA
#04	GSM������
#05	IP��
#06	GPRS
#10	CDMA-1X
#����030000����ͨ040000
#01	�̶�
#05	IP��
#11	PHS��С��ͨ��
#�ƶ���010000
#02	GSM
#04	GSM������
#05	IP��
#��ͨ050000
#01	�̶�
#05	IP��
#######################################################