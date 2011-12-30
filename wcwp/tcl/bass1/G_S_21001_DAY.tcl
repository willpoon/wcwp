######################################################################################################
#�ӿ����ƣ�GSM/TD��ͨ����ҵ����ʹ��
#�ӿڱ��룺21001
#�ӿ�˵������¼���ۺ������ҵ���շ���ʹ����Ϣ���������ͻ�����ʹ�ü�¼���⣩��
#          ���а����й��ƶ��ͻ�ͨ���ֻ������й��ƶ�IP17951��17950����ʹ�ü�¼��IP������������
#          ������������Ӫ��IPϵͳ�����ķ���ʹ�ü�¼������������ֵҵ�����ʹ�ü�¼���������������û�����¼����
#��������:  G_S_21001_DAY.tcl
#��������: ����2100������
#��������: ��
#Դ    ��1.bass1.int_210012916_yyyymm
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: ���� OPP_PROPERTY ������������ ,modi by zhanght on 20090501
#          liuzhilong 20090911 ȥ��where ����  and DRTYPE_ID  not in (1700,9901,9902,9903)
#          liuqf     20100401 ����һ�㴦���߼�����ֹ����Ψһ��У�������
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

    #ɾ���м������
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.temp_g_s_21001_day"
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

    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.temp_g_s_21001_day
                      (  BRAND_ID
                        ,SVC_TYPE_ID
                        ,TOLL_TYPE_ID
                        ,IP_TYPE_ID
                        ,ADVERSARY_ID
                        ,ROAM_TYPE_ID
                        ,CALL_TYPE_ID
                        ,CALL_COUNTS
                        ,BASE_BILL_DURATION
                        ,TOLL_BILL_DURATION
                        ,CALL_DURATION
                        ,BASE_CALL_FEE
                        ,TOLL_CALL_FEE
                        ,CALLFW_TOLL_FEE
                        ,CALL_FEE
                        ,FAVOURED_BASECALL_FEE
                        ,FAVOURED_TOLLCALL_FEE
                        ,FAVOURED_CALLFW_TOLLFEE
                        ,FAVOURED_CALL_FEE
                        ,FREE_DURATION
                        ,FAVOUR_DURATION
                        ,MNS_TYPE
                      )
                   select
		               COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
				      ,svc_type_id
				      ,toll_type_id
				      ,ip_type_id
				      ,adversary_id
				      ,roam_type_id
				      ,call_type_id
				      ,char(bigint(sum(call_counts		)))
				      ,char(bigint(sum(base_bill_duration	)))
				      ,char(bigint(sum(toll_bill_duration	)))
				      ,char(bigint(sum(call_duration		)))
				      ,char(bigint(sum(base_call_fee		)))
				      ,char(bigint(sum(toll_call_fee		)))
				      ,char(bigint(sum(callfw_toll_fee	)))
				      ,char(bigint(sum(call_fee		)))
				      ,char(bigint(sum(favoured_basecall_fee	)))
				      ,char(bigint(sum(favoured_tollcall_fee	)))
				      ,char(bigint(sum(favoured_callfw_tollfee)))
				      ,char(bigint(sum(favoured_call_fee	)))
				      ,char(bigint(sum(free_duration		)))
				      ,char(bigint(sum(favour_duration	)))
				      ,value(char(MNS_TYPE),'0')
                    FROM
                        (
                         select 
                           * 
                         from 
                           bass1.int_210012916_$op_month
                         where 
                           op_time=$timestamp
                      	 ) a
                    left join $RegDatFrmMis b on a.user_id=b.user_id
		    WHERE op_time=$timestamp
		    GROUP BY
		      COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
		      ,svc_type_id
		      ,toll_type_id
		      ,ip_type_id
		      ,adversary_id
		      ,roam_type_id
		      ,call_type_id
		      ,value(char(MNS_TYPE),'0')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#value(char(MNS_TYPE),'0')



  set handle [aidb_open $conn]
	set sql_buff "update bass1.temp_g_s_21001_day
set ip_type_id='9000'
where ip_type_id not in ('1000',     '2000',     '2100',     '2101',     '2102',     '2199',     '2200',  
   '2201',     '2202',     '2203',     '2204',     '2299',     '3000',    
    '3100',     '3101',     '3102',     '3103',     '3104',     '3105',    
     '3106',     '3107',     '3108',     '3109',     '3110',     '3111',   
       '3112',     '3113',     '3114',     '3199',     '3200',     '4000',   
         '4100',     '4101',     '4102',     '4103',     '4104',     '4105',    
          '4106',     '4107',     '4108',     '4109',     '4110',     '4111',    
           '4199',     '4200',     '6000',     '6100',     '6101',     '6102',    
            '6103',     '6104',     '6105',     '6106',     '6107',     '6199',    
             '6200',     '9000')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    # MODIFY �ϲ���֤����Ψһ�ԣ�����Ŀ���
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21001_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21001_day
                      (
                        TIME_ID
                        ,BILL_DATE
                        ,BRAND_ID
                        ,SVC_TYPE_ID
                        ,TOLL_TYPE_ID
                        ,IP_TYPE_ID
                        ,ADVERSARY_ID
                        ,ROAM_TYPE_ID
                        ,CALL_TYPE_ID
                        ,CALL_COUNTS
                        ,BASE_BILL_DURATION
                        ,TOLL_BILL_DURATION
                        ,CALL_DURATION
                        ,BASE_CALL_FEE
                        ,TOLL_CALL_FEE
                        ,CALLFW_TOLL_FEE
                        ,CALL_FEE
                        ,FAVOURED_BASECALL_FEE
                        ,FAVOURED_TOLLCALL_FEE
                        ,FAVOURED_CALLFW_TOLLFEE
                        ,FAVOURED_CALL_FEE
                        ,FREE_DURATION
                        ,FAVOUR_DURATION
                        ,MNS_TYPE
                      )
                    select
                        $timestamp
                        ,'$timestamp'
                        ,BRAND_ID
                        ,SVC_TYPE_ID
                        ,TOLL_TYPE_ID
                        ,IP_TYPE_ID
                        ,ADVERSARY_ID
                        ,ROAM_TYPE_ID
                        ,CALL_TYPE_ID
                        ,char(sum(bigint(CALL_COUNTS)))
                        ,char(sum(bigint(BASE_BILL_DURATION)))
                        ,char(sum(bigint(TOLL_BILL_DURATION)))
                        ,char(sum(bigint(CALL_DURATION)))
                        ,char(sum(bigint(BASE_CALL_FEE)))
                        ,char(sum(bigint(TOLL_CALL_FEE)))
                        ,char(sum(bigint(CALLFW_TOLL_FEE)))
                        ,char(sum(bigint(CALL_FEE)))
                        ,char(sum(bigint(FAVOURED_BASECALL_FEE)))
                        ,char(sum(bigint(FAVOURED_TOLLCALL_FEE)))
                        ,char(sum(bigint(FAVOURED_CALLFW_TOLLFEE)))
                        ,char(sum(bigint(FAVOURED_CALL_FEE)))
                        ,char(sum(bigint(FREE_DURATION)))
                        ,char(sum(bigint(FAVOUR_DURATION)))
                        ,MNS_TYPE
                    FROM bass1.temp_g_s_21001_day
                  GROUP BY BRAND_ID
                        ,SVC_TYPE_ID
                        ,TOLL_TYPE_ID
                        ,IP_TYPE_ID
                        ,ADVERSARY_ID
                        ,ROAM_TYPE_ID
                        ,CALL_TYPE_ID
                        ,MNS_TYPE
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
#####################################
#and svcitem_id not in ('1700','9901','9902','9903') 
#--����1700:�������ߡ����Ż���ҵ�� others:VPMN����--
####################################