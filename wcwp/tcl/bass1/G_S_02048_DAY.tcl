
######################################################################################################		
#�ӿ�����: WLAN�û����ֻ�����IMEI��Ϣ                                                               
#�ӿڱ��룺02048                                                                                          
#�ӿ�˵������¼ʹ���ֻ�����WLAN������Ϊ���û�IMEI��Ϣ��
#��������: G_S_02048_DAY.tcl                                                                            
#��������: ����02048������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120328
#�����¼��
#�޸���ʷ: 1. panzw 20120328	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.7.9) 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyy-mm-dd
        set optime $op_time
	set sql_buff "
	insert into bass1.G_S_02048_DAY
		  (
         TIME_ID
        ,USER_ID
        ,MSISDN
        ,IMEI
		  )
 select	TIME_ID
        ,USER_ID
        ,MSISDN
        ,IMEI
        from G_S_02048_DAY
where USER_ID = 'xxxxxxxxxxx'
with ur
  "
	exec_sql $sql_buff
        
	return 0
}
