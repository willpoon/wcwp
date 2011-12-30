######################################################################################################
#�ӿ����ƣ�������������
#�ӿڱ��룺06025
#�ӿ�˵������¼���й��ƶ�������˾ǩ������Э���Ϊ��������������ʵ�塣
#��������: G_I_06025_MONTH.tcl
#��������: ����06025������
#��������: ��
#Դ    ��1.bass2.dim_pub_channel(����ά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ��������BOSSû���κ�ʱ�䣬������������ͳһ��20000101����ֹ����ͳһ��20300101��
#          2.��Ҫͨ���������ж�
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06025_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06025_month
                       select
                         $op_month
                         ,char(channel_id)
                         ,substr(channel_name,1,20)
                         ,channel_name
                         ,'20000101'
                         ,'20300101'
                         ,char(int(city_id)+12210)
		         ,case 
		            when city_id ='891' then '540100'
		            when city_id ='892' then '542300'
		            when city_id ='893' then '542200'
		            when city_id ='894' then '542600'
		            when city_id ='895' then '542100'
		            when city_id ='896' then '542400'
		            when city_id ='897' then '542500'		
		            else '540000' 
		           end  
                        from bass2.dim_pub_channel
                        where sts=1 
                          and channeltype_id=13"
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
