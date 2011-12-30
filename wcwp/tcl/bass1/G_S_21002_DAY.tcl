######################################################################################################
#�ӿ����ƣ�GSM��ͨ����ҵ����ʱ��ʹ��
#�ӿڱ��룺21002
#�ӿ�˵������¼���ۺ������ҵ����ʱ�η���ʹ����Ϣ���������ͻ�����ʹ�ü�¼���⣩��
#          ���а����й��ƶ��ͻ�ͨ���ֻ������й��ƶ�IP17951��17950����ʹ�ü�¼��IP������������
#          ������������Ӫ��IPϵͳ�����ķ���ʹ�ü�¼��������������ֵҵ�����ʹ�ü�¼�����������������û�����
#��������:  G_S_21002_DAY.tcl
#��������: ����2100������
#��������: ��
#Դ    ��1.bass1.int_210012916_yyyymm
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���������־�ֶ�@20070801 By tym
#�޸���ʷ: liuzhilong 20090911 ȥ��where ����  and DRTYPE_ID  not in (1700,9901,9902,9903)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyy-mm-dd
	set optime $op_time
	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
	#����  yyyymm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#������ yyyymm
	set last_last_month [GetLastMonth [string range $last_month 0 5]]
	#��������ڣ���ʽdd(��������20070411 ����11,����20070708������8)
	set today_dd [format "%.0f" [string range $timestamp 6 7]]
	       
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21002_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       #���������־�ֶ�@20070801 By tym
       if {$today_dd > 12} {
           set RegDatFrmMis "bass2.stat_zd_village_users_$last_month" 
       } else {
	   set RegDatFrmMis "bass2.stat_zd_village_users_$last_last_month" 
       }           
       
       
  puts $RegDatFrmMis     
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_21002_DAY
                      (
                       TIME_ID
                       ,BILL_DATE
                       ,BRAND_ID
                       ,CALL_MOMENT_ID
                       ,CALL_COUNTS
                       ,BASE_BILL_DURATION
                       ,TOLL_BILL_DURATION
                       ,CALL_DURATION
                       ,FAVOURED_BASECALL_FEE
                       ,FAVOURED_TOLLCALL_FEE
                       ,FAVOURED_CALL_FEE      
                      )
                      select
                         $timestamp
                      	,'$timestamp'
                      	,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(a.BRAND_ID)),'2') 
                      	,a.callmoment_id
                      	,char(bigint(sum(a.call_counts	          )))
                      	,char(bigint(sum(a.base_bill_duration     )))
                      	,char(bigint(sum(a.toll_bill_duration     )))
                      	,char(bigint(sum(a.call_duration	  )))
                      	,char(bigint(sum(a.favoured_basecall_fee  )))
                      	,char(bigint(sum(a.favoured_tollcall_fee  )))
                      	,char(bigint(sum(a.favoured_call_fee	  )))                      	
                      FROM
                        (
                         select 
                           * 
                         from 
                           bass1.int_210012916_$op_month
                         where 
                           op_time=$timestamp

                      	) a
                      left join 
                          $RegDatFrmMis  b
                      on 
                       a.user_id=b.user_id
                      GROUP BY
                        COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(a.brand_id)),'2') 
                      	,a.CALLMOMENT_ID "
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
#####################################
#and svcitem_id not in ('1700','9901','9902','9903') 
#--����1700:�������ߡ����Ż���ҵ�� others:VPMN����--

#ԭ�����򱸷� 20070801
#       #���� yyyy-mm-dd
#	set optime $op_time
#	#���� yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
#        #���� yyyymm
#        set op_month [string range $op_time 0 3][string range $op_time 5 6]
#	       
#        #ɾ����������
#        set handle [aidb_open $conn]
#	set sql_buff "delete from bass1.g_s_21002_day where time_id=$timestamp"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#       
#       
#       
#       
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.G_S_21002_DAY
#                      (
#                       TIME_ID
#                       ,BILL_DATE
#                       ,BRAND_ID
#                       ,CALL_MOMENT_ID
#                       ,REGION_FLAG
#                       ,CALL_COUNTS
#                       ,BASE_BILL_DURATION
#                       ,TOLL_BILL_DURATION
#                       ,CALL_DURATION
#                       ,FAVOURED_BASECALL_FEE
#                       ,FAVOURED_TOLLCALL_FEE
#                       ,FAVOURED_CALL_FEE      
#                      )
#                      select
#                         $timestamp
#                      	,'$timestamp'
#                      	,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(a.BRAND_ID)),'2') 
#                      	,a.callmoment_id
#                      	,'2'
#                      	,char(bigint(sum(a.call_counts	          )))
#                      	,char(bigint(sum(a.base_bill_duration     )))
#                      	,char(bigint(sum(a.toll_bill_duration     )))
#                      	,char(bigint(sum(a.call_duration	  )))
#                      	,char(bigint(sum(a.favoured_basecall_fee  )))
#                      	,char(bigint(sum(a.favoured_tollcall_fee  )))
#                      	,char(bigint(sum(a.favoured_call_fee	  )))                      	
#                      FROM
#                         bass1.int_210012916_$op_month
#                      where 
#                         op_time=$timestamp
#                         and svcitem_id not in ('1700','9901','9902','9903')
#                      GROUP BY
#                        COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(a.brand_id)),'2') 
#                      	,a.CALLMOMENT_ID "
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
#	return 0
####################################