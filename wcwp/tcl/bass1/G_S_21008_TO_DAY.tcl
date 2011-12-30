######################################################################################################
#�ӿ����ƣ�
#�ӿڱ��룺
#�ӿ�˵����
#��������: G_S_21008_TO_DAY.tcl
#��������: ����21008���м������
#��������: ��
#Դ    ��1.bass1.INT_21007_YYYYMM
#
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ:
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyy-mm-dd
  #set op_time 2008-06-01     
	set optime $op_time
	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21008_to_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle






        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21008_to_day
            (
               TIME_ID
              ,PRODUCT_NO
              ,BRND_ID
              ,SVC_TYPE_ID
              ,CDR_TYPE_ID
              ,END_STATUS
              ,ADVERSARY_ID
              ,SMS_COUNTS
              ,SMS_BASE_FEE
              ,SMS_INFO_FEE
              ,SMS_MONTH_FEE
              )
           select
	     $timestamp
	     ,product_no
	     ,brand_id
	     ,svc_type_id
	     ,cdr_type_id
	     ,end_status
	     ,adversary_id
	     ,char(bigint(sum(sms_counts )  ))
	     ,char(bigint(sum(sms_base_fee) ))
	     ,char(bigint(sum(sms_info_fee) ))
	     ,char(bigint(sum(sms_month_fee)))
	   from
	     bass1.int_21007_$op_month
           where
             op_time=$timestamp
	     and SVC_TYPE_ID in ('11','12','13','70')
	   group by
	     product_no
	     ,brand_id
	     ,svc_type_id
	     ,cdr_type_id
	     ,end_status
	     ,adversary_id"
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