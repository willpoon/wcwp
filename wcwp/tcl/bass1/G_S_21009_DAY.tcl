######################################################################################################
#�ӿ����ƣ�������VPMNҵ��ʹ��
#�ӿڱ��룺21009
#�ӿ�˵������¼�й��ƶ���˾�û�ͨ��������ƽ̨������VPMN����ͨ����ҵ��ʹ����Ϣ��
#          �������������û����������ҵ�񻰵���Ҳ������BOSSƽ̨ʵ�ֵ�VPMN����ͨ���굥��
#          �������������û�����¼��
#��������:  G_S_21009_DAY.tcl
#��������: ����2100������
#��������: ��
#Դ    ��1.bass1.int_210012916_yyyymm
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: liuzhilong 20090911  ���ַ�ȷ�����޴�ҵ�� ���״��� 
#######################################################################################################



proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##        #���� yyyy-mm-dd
##	set optime $op_time
##	#���� yyyymmdd
##        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
##        #���� yyyymm
##        set op_month [string range $op_time 0 3][string range $op_time 5 6]
##        
##        #ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_s_21009_day where time_id=$timestamp"
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##       
##
##
##
##
##
##        set handle [aidb_open $conn]
##
##	set sql_buff "insert into bass1.G_S_21009_DAY
##       (
##            TIME_ID
##            ,BILL_DATE
##            ,PRODUCT_NO
##            ,TOLL_TYPE_ID
##            ,ROAM_TYPE_ID
##            ,CALL_TYPE_ID
##            ,CALL_COUNTS
##            ,CALL_DURATION
##            ,BASE_BILL_DURATION
##            ,TOLL_BILL_DURATION
##            ,FAVOURED_BASECALL_FEE
##            ,FAVOURED_TOLLCALL_FEE
##            ,FAVOURED_CALL_FEE
##         )
##        SELECT
##		$timestamp
##        	,'$timestamp'
##        	,product_no
##        	,toll_type_id
##        	,roam_type_id
##        	,call_type_id
##        	,char(bigint(sum(call_counts		)))
##        	,char(bigint(sum(call_duration		)))
##        	,char(bigint(sum(base_bill_duration	)))
##        	,char(bigint(sum(toll_bill_duration	)))
##        	,char(bigint(sum(favoured_basecall_fee	)))
##        	,char(bigint(sum(favoured_tollcall_fee	)))
##        	,char(bigint(sum(favoured_call_fee	)))
##        	 FROM
##        	 	bass1.int_210012916_${op_month}
##        	 WHERE
##        	 	op_time=$timestamp
##        	 	and svcitem_id in ('9901','9902','9903')
##        	 GROUP BY
##        	 	op_time
##        	 	,product_no
##        	 	,toll_type_id
##        	 	,roam_type_id
##        	 	,call_type_id "
##        puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle

	return 0
}