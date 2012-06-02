######################################################################################################
#�ӿ����ƣ�WLAN����
#�ӿڱ��룺04003
#�ӿ�˵����
#��������: G_S_04003_DAY.tcl
#��������: ����04003������
#��������: ��
#Դ    ��1.bass2.cdr_wlan_yyyymmdd(wlan�嵥)        
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ:  
##~   20090901 1.6.2�淶ȥ��imei�ֶ�
##~   20120331 1.7.9:
##~   522	�޸Ľӿ�04003��WLAN��������
##~   1��	���������ֶΣ���֤ƽ̨���͡��û��ն����͡��û�MAC��ַ��AP��MAC��ַ��
##~   2��	�޸��ֶ�"��ʼ����"��"��ʼʱ��"��"��������"��"����ʱ��"��������д��ʽ����������
##~   3��	�޸��ֶ�"ҵ���ʶ"��ɾ�����ֶ���������"01��WLANҵ���ʶ"�����ӱ�ע˵��ҵ���ʶ���ࣻ
##~   4��	�޸��ֶ�"WLAN��֤���ͱ���"������"03��PEAP��֤"��"04���ͻ�����֤"������μ���������淶�е�BASS_STD2_0006��
##~   5��	�޸��ֶ�"�ȵ������ʶ"����������������ʽ��д˵�������޸ı�ע���ݣ�	1.7.9	2012-03-20	����������20120401����Ч
		##~  20120430 FN_CHANGE_DTOH --> FN_CHANGE_DTOH2
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
	set sql_buff "delete from bass1.g_s_04003_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       
     
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04003_DAY
                        (
							 TIME_ID
							,PRODUCT_NO
							,IMSI
							,HOME_LOCN
							,ROAM_LOCN
							,USER_TYPE
							,ROAM_TYPE_ID
							,START_DATE
							,START_TIME
							,END_DATE
							,END_TIME
							,CALL_DURATION
							,FLOWUP
							,FLOWDOWN
							,SVCITEM_ID
							,SERVICE_CODE	--ҵ���ʶ 
							,WLAN_ATTESTATION_CODE
							,WLAN_ATTESTATION_TYPE --��֤ƽ̨����
							,HOTSPOT_AREA_ID
							,AS_IP
							,ATTESTATION_AS_IP
							,USER_TERM_TYPE	--�û��ն�����
							,USER_MAC_ADDR	--�û�MAC��ַ
							,AP_MAC	--AP��MAC��ַ
							,CALL_FEE
							,INFO_FEE
							,SERVICE_ID
							,ISP_ID
							,BELONG_OPER_ID
							,ROAM_OPER_ID
							,REASON_OF_STOP_CODE
                         ) 
               select
                        ${timestamp} 
                        ,PRODUCT_NO 
                        ,IMSI
                        ,value(char(PROVINCE_ID),'891')
                        ,value(char(ROAM_PROVINCE_ID),'891')
                        ,'1'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAMTYPE_ID)),'500') 
                        ,substr(char(START_TIME),1,4)||substr(char(START_TIME),6,2)||substr(char(START_TIME),9,2)
                        ,substr(char(START_TIME),12,2)||substr(char(START_TIME),15,2)||substr(char(START_TIME),18,2)
                        ,substr(char(STOP_TIME),1,4)||substr(char(STOP_TIME),6,2)||substr(char(STOP_TIME),9,2)
                        ,substr(char(STOP_TIME),12,2)||substr(char(STOP_TIME),15,2)||substr(char(STOP_TIME),18,2)
                        ,char(DURATION)
                        ,char(DATA_FLOW_UP)
                        ,char(DATA_FLOW_DOWN)
                        ,'03'
                        ,case  when RESERVE1  = '01' then '01'
							   when RESERVE1 = '02' then '02'
							   when RESERVE1 = '03' then '03'
						else '01' end SERVICE_CODE /*1.7.9��������δʵ�֣��޷����֣�����ԭ�ھ�*/
                        ,case when AUTH_TYPE=1 THEN '01' else '02' end WLAN_ATTESTATION_CODE
						,'01' WLAN_ATTESTATION_TYPE --��֤ƽ̨���� /*1.7.9��������δʵ�֣��޷�����,�� 01������ͳһ��֤ƽ̨���� �ϱ�*/
                        ,HOSTSPOT_ID
                        ,COALESCE(FN_CHANGE_DTOH2(AS_ADDRESS),' ') AS_IP
                        ,COALESCE(FN_CHANGE_DTOH2(AC_ADDRESS),' ') ATTESTATION_AS_IP
						,'' USER_TERM_TYPE /*�û��ն����� ������δʵ�֣��޷�����;����*/
						,'' USER_MAC_ADDR	--�û�MAC��ַ/* ������δʵ�֣��޷���ȡ;����*/
						,'' AP_MAC	--AP��MAC��ַ/* ������δʵ�֣��޷���ȡ;����*/
                        ,char(CHARGE1/10 + CHARGE4/10) 
                        ,char(CHARGE4/10)
                        ,''
                        ,''
                        ,substr(char(bigint(IMSI)),1,5)
                        ,substr(char(bigint(IMSI)),1,5)
                        ,''
                        from bass2.cdr_wlan_${timestamp}
                        where IMSI is not null and DATA_FLOW_DOWN >= 0"
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