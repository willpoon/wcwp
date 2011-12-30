######################################################################################################
#�ӿ����ƣ�����������KPI
#�ӿڱ��룺09901
#�ӿ�˵������¼����������KPI��Ϣ
#��������: G_S_09901_DAY.tcl
#��������: ����09901������
#��������: ��
#Դ    ��1.bass2.dw_acct_shoulditem_yyyymmdd--
#          2.bass2.dw_comp_all_dt--
#          3.bass2.Dw_comp_cust_dt--
#          4.bass2.dw_product_yyyymmdd --
#          5.bass2.dw_comp_all_yyyymmdd--
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2008-11-25
#�����¼��1.��ΪĿǰ���������������ϲ��ܴӶ��ų����������ݲ�׼ȷ��������Ŀǰ�޷������
#          2.�ýӿڳ�����Ϊ�õ���dt�����Բ���������ǰ�����ݡ�
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyy-mm-dd
        set optime $op_time
        #��������ڣ���ʽdd(��������20070411 ����11)
        set today_dd [string range $op_time 8 9]
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        #ɾ����������
	set sql_buff "delete from bass1.g_s_09901_day where time_id=$timestamp"
	puts $sql_buff
	exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_09901_day where time_id=88888888"
	puts $sql_buff
	exec_sql $sql_buff

	#����Ӫ�̾������������û���
  #Union_GSM_New       CHARACTER(10)    NOT NULL, --��ͨGSM�����ͻ���
  #Union_GSM_New156    CHARACTER(10)    NOT NULL, --��ͨGSM156�Ŷ������ͻ���
  #Union_Net_New       CHARACTER(10)    NOT NULL, --��ͨС��ͨ�����ͻ���
  #Union_Fix_New       CHARACTER(10)    NOT NULL, --��ͨ�̻������ͻ���
  #COM_CDMA_NEW        CHARACTER(10)    NOT NULL, --����CDMA�����ͻ���
  #COM_CDMA_NEW133     CHARACTER(10)    NOT NULL, --����CDMA133�Ŷ������ͻ���
  #COM_CDMA_NEW189     CHARACTER(10)    NOT NULL, --����CDMA189�Ŷ������ͻ���
  #COM_Net_NEW         CHARACTER(10)    NOT NULL, --����С��ͨ�����ͻ���
  #COM_Fix_NEW         CHARACTER(10)    NOT NULL, --���Ź̻������ͻ���

  set sql_buff "insert into g_s_09901_day (TIME_ID,StatDay,Union_GSM_New,Union_GSM_New156,Union_Net_New,Union_Fix_New,
                                           Union_GSM_arrive,Union_GSM_arrive156,Union_Net_arrive,Union_Fix_arrive,
                                           Union_GSM_Lost,Union_GSM_Lost156,Union_Net_Lost,Union_Fix_Lost,COM_CDMA_NEW,
                                           COM_CDMA_NEW133,COM_CDMA_NEW189,COM_Net_NEW,COM_Fix_NEW,COM_CDMA_Arrive,
                                           COM_CDMA_Arrive133,COM_CDMA_Arrive189,COM_Net_Arrive,COM_Fix_Arrive,
                                           COM_CDMA_Lost,COM_CDMA_Lost133,COM_CDMA_Lost189,COM_Net_Lost,COM_Fix_Lost)
                select $timestamp,'$timestamp',
                       char(sum(t.Union_GSM_New)),
                       char(sum(t.Union_GSM_New156)),
                       char(sum(t.Union_Net_New)),
                       char(sum(t.Union_Fix_New)),
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       char(sum(t.COM_CDMA_NEW)),
                       char(sum(t.COM_CDMA_NEW133)),
                       char(sum(t.COM_CDMA_NEW189)),
                       char(sum(t.COM_Net_NEW)),
                       char(sum(t.COM_Fix_NEW)),
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0'
                  from
                    (
	               select
	                 case
	                   when comp_brand_id in (3) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_New,
	                 case
	                   when comp_product_no like '156%' then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_New156,
	                 0 as Union_Net_New,
	                 case
	                   when comp_brand_id in (6) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_Fix_New,
	                 case
	                   when comp_brand_id in (4) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_NEW,
	                 case
	                   when comp_product_no like '133%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_NEW133,
	                 case
	                   when comp_product_no like '189%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_NEW189,
	                 case
	                   when comp_brand_id in (1) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Net_NEW,
	                 case
	                   when comp_brand_id in (2) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Fix_NEW
	               from 
	                 bass2.Dw_comp_cust_dt
	               where
	                 comp_day_new_mark=1
	               group by 
	                 comp_product_no,comp_brand_id 
	            )t"
	puts $sql_buff
	exec_sql $sql_buff
 

	#����Ӫ�̾��������û�������
  #Union_GSM_arrive    CHARACTER(10)    NOT NULL, --��ͨGSM�ͻ�������
  #Union_GSM_arrive156 CHARACTER(10)    NOT NULL, --��ͨGSM156�Ŷοͻ�������
  #Union_Net_arrive    CHARACTER(10)    NOT NULL, --��ͨС��ͨ�ͻ�������
  #Union_Fix_arrive    CHARACTER(10)    NOT NULL, --��ͨ�̻��ͻ�������
  #COM_CDMA_Arrive     CHARACTER(10)    NOT NULL, --����CDMA�ͻ�������
  #COM_CDMA_Arrive133  CHARACTER(10)    NOT NULL, --����CDMA133�Ŷοͻ�������
  #COM_CDMA_Arrive189  CHARACTER(10)    NOT NULL, --����CDMA189�Ŷοͻ�������
  #COM_Net_Arrive      CHARACTER(10)    NOT NULL, --����С��ͨ�ͻ�������
  #COM_Fix_Arrive      CHARACTER(10)    NOT NULL, --���Ź̻��ͻ�������
  set sql_buff "insert into g_s_09901_day (TIME_ID,StatDay,Union_GSM_New,Union_GSM_New156,Union_Net_New,Union_Fix_New,
                                           Union_GSM_arrive,Union_GSM_arrive156,Union_Net_arrive,Union_Fix_arrive,
                                           Union_GSM_Lost,Union_GSM_Lost156,Union_Net_Lost,Union_Fix_Lost,COM_CDMA_NEW,
                                           COM_CDMA_NEW133,COM_CDMA_NEW189,COM_Net_NEW,COM_Fix_NEW,COM_CDMA_Arrive,
                                           COM_CDMA_Arrive133,COM_CDMA_Arrive189,COM_Net_Arrive,COM_Fix_Arrive,
                                           COM_CDMA_Lost,COM_CDMA_Lost133,COM_CDMA_Lost189,COM_Net_Lost,COM_Fix_Lost)
                select $timestamp,'$timestamp',
                       '0',
                       '0',
                       '0',
                       '0',
                       char(sum(t.Union_GSM_Arrive)),
                       char(sum(t.Union_GSM_Arrive156)),
                       char(sum(t.Union_Net_Arrive)),
                       char(sum(t.Union_Fix_Arrive)),
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       char(sum(t.COM_CDMA_Arrive)),
                       char(sum(t.COM_CDMA_Arrive133)),
                       char(sum(t.COM_CDMA_Arrive189)),
                       char(sum(t.COM_Net_Arrive)),
                       char(sum(t.COM_Fix_Arrive)),
                       '0',
                       '0',
                       '0',
                       '0',
                       '0'
                  from
                    (
	               select
	                 case
	                   when comp_brand_id in (3) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_Arrive,
	                 case
	                   when comp_product_no like '156%' then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_Arrive156,
	                 0 as Union_Net_Arrive,
	                 case
	                   when comp_brand_id in (6) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_Fix_Arrive,
	                 case
	                   when comp_brand_id in (4) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Arrive,
	                 case
	                   when comp_product_no like '133%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Arrive133,
	                 case
	                   when comp_product_no like '189%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Arrive189,
	                 case
	                   when comp_brand_id in (1) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Net_Arrive,
	                 case
	                   when comp_brand_id in (2) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Fix_Arrive
	               from 
	                 bass2.Dw_comp_cust_dt
	               where
	                 replace(substr(char(comp_open_date),1,7),'-','')='$op_month'
	               group by 
	                 comp_product_no,comp_brand_id 
	            )t"
	puts $sql_buff
	exec_sql $sql_buff





	#����Ӫ�̾������������ͻ���
  #Union_GSM_Lost      CHARACTER(10)    NOT NULL, --��ͨGSM�����ͻ���
  #Union_GSM_Lost156   CHARACTER(10)    NOT NULL, --��ͨGSM156�Ŷ������ͻ���
  #Union_Net_Lost      CHARACTER(10)    NOT NULL, --��ͨС��ͨ�����ͻ���
  #Union_Fix_Lost      CHARACTER(10)    NOT NULL, --��ͨ�̻������ͻ���
  #COM_CDMA_Lost       CHARACTER(10)    NOT NULL, --����CDMA�����ͻ���
  #COM_CDMA_Lost133    CHARACTER(10)    NOT NULL, --����CDMA133�Ŷ������ͻ���
  #COM_CDMA_Lost189    CHARACTER(10)    NOT NULL, --����CDMA189�Ŷ������ͻ���
  #COM_Net_Lost        CHARACTER(10)    NOT NULL, --����С��ͨ�����ͻ���
  #COM_Fix_Lost        CHARACTER(10)    NOT NULL  --���Ź̻������ͻ���
  set sql_buff "insert into g_s_09901_day (TIME_ID,StatDay,Union_GSM_New,Union_GSM_New156,Union_Net_New,Union_Fix_New,
                                           Union_GSM_arrive,Union_GSM_arrive156,Union_Net_arrive,Union_Fix_arrive,
                                           Union_GSM_Lost,Union_GSM_Lost156,Union_Net_Lost,Union_Fix_Lost,COM_CDMA_NEW,
                                           COM_CDMA_NEW133,COM_CDMA_NEW189,COM_Net_NEW,COM_Fix_NEW,COM_CDMA_Arrive,
                                           COM_CDMA_Arrive133,COM_CDMA_Arrive189,COM_Net_Arrive,COM_Fix_Arrive,
                                           COM_CDMA_Lost,COM_CDMA_Lost133,COM_CDMA_Lost189,COM_Net_Lost,COM_Fix_Lost)
                select $timestamp,'$timestamp',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       value(char(sum(t.Union_GSM_Lost)),'0'),
                       value(char(sum(t.Union_GSM_Lost156)),'0'),
                       value(char(sum(t.Union_Net_Lost)),'0'),
                       value(char(sum(t.Union_Fix_Lost)),'0'),
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       '0',
                       value(char(sum(t.COM_CDMA_Lost)),'0'),
                       value(char(sum(t.COM_CDMA_Lost133)),'0'),
                       value(char(sum(t.COM_CDMA_Lost189)),'0'),
                       value(char(sum(t.COM_Net_Lost)),'0'),
                       value(char(sum(t.COM_Fix_Lost)),'0')
                  from
                    (
	               select
	                 case
	                   when comp_brand_id in (3) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_Lost,
	                 case
	                   when comp_product_no like '156%' then count(distinct comp_product_no)
	                   else 0
	                 end as Union_GSM_Lost156,
	                 0 as Union_Net_Lost,
	                 case
	                   when comp_brand_id in (6) then count(distinct comp_product_no)
	                   else 0
	                 end as Union_Fix_Lost,
	                 case
	                   when comp_brand_id in (4) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Lost,
	                 case
	                   when comp_product_no like '133%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Lost133,
	                 case
	                   when comp_product_no like '189%' then count(distinct comp_product_no)
	                   else 0
	                 end as COM_CDMA_Lost189,
	                 case
	                   when comp_brand_id in (1) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Net_Lost,
	                 case
	                   when comp_brand_id in (2) then count(distinct comp_product_no)
	                   else 0
	                 end as COM_Fix_Lost
	               from 
	                 bass2.Dw_comp_cust_dt
	               where
	                 comp_day_off_mark=1
	               group by 
	                 comp_product_no,comp_brand_id 
	            )t"
	puts $sql_buff
	exec_sql $sql_buff


	
  #���
	set sql_buff "delete from bass1.g_s_09901_day where time_id=88888888"
	puts $sql_buff
	exec_sql $sql_buff

	set sql_buff "update bass1.g_s_09901_day set time_id = 88888888 where time_id = $timestamp"
	puts $sql_buff
	exec_sql $sql_buff

	set sql_buff "insert into g_s_09901_day (TIME_ID,StatDay,Union_GSM_New,Union_GSM_New156,Union_Net_New,Union_Fix_New,
                                           Union_GSM_arrive,Union_GSM_arrive156,Union_Net_arrive,Union_Fix_arrive,
                                           Union_GSM_Lost,Union_GSM_Lost156,Union_Net_Lost,Union_Fix_Lost,COM_CDMA_NEW,
                                           COM_CDMA_NEW133,COM_CDMA_NEW189,COM_Net_NEW,COM_Fix_NEW,COM_CDMA_Arrive,
                                           COM_CDMA_Arrive133,COM_CDMA_Arrive189,COM_Net_Arrive,COM_Fix_Arrive,
                                           COM_CDMA_Lost,COM_CDMA_Lost133,COM_CDMA_Lost189,COM_Net_Lost,COM_Fix_Lost)
                           select $timestamp,'$timestamp',
                                  char(sum(bigint(Union_GSM_New))),
                                  char(sum(bigint(Union_GSM_New156))),
                                  char(sum(bigint(Union_Net_New))),
                                  char(sum(bigint(Union_Fix_New))),
                                  char(sum(bigint(Union_GSM_arrive))),
                                  char(sum(bigint(Union_GSM_arrive156))),
                                  char(sum(bigint(Union_Net_arrive))),
                                  char(sum(bigint(Union_Fix_arrive))),
                                  char(sum(bigint(Union_GSM_Lost))),
                                  char(sum(bigint(Union_GSM_Lost156))),
                                  char(sum(bigint(Union_Net_Lost))),
                                  char(sum(bigint(Union_Fix_Lost))),
                                  char(sum(bigint(COM_CDMA_NEW))),
                                  char(sum(bigint(COM_CDMA_NEW133))),
                                  char(sum(bigint(COM_CDMA_NEW189))),
                                  char(sum(bigint(COM_Net_NEW))),
                                  char(sum(bigint(COM_Fix_NEW))),
                                  char(sum(bigint(COM_CDMA_Arrive))),
                                  char(sum(bigint(COM_CDMA_Arrive133))),
                                  char(sum(bigint(COM_CDMA_Arrive189))),
                                  char(sum(bigint(COM_Net_Arrive))),
                                  char(sum(bigint(COM_Fix_Arrive))),
                                  char(sum(bigint(COM_CDMA_Lost))),
                                  char(sum(bigint(COM_CDMA_Lost133))),
                                  char(sum(bigint(COM_CDMA_Lost189))),
                                  char(sum(bigint(COM_Net_Lost))),
                                  char(sum(bigint(COM_Fix_Lost)))   
                             from g_s_09901_day
                            where time_id = 88888888
                            group by time_id"                        
	puts $sql_buff
	exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_09901_day where time_id=88888888"
	puts $sql_buff
	exec_sql $sql_buff


##############################################
	return 0      
}   



#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------



                  
##########################�ο�����
#        #01:----INCOME ���ɵ������� ,�ý��������-----#
#                     
#        set handle [aidb_open $conn]
#        if { $today_dd =="01" } {
#           set sql_buff "\
#	            select
#	              bigint(sum(fact_fee)*100)
#	            from 
#	              bass2.dw_acct_kpi_total_$timestamp "  	
#       } else {      
#           set sql_buff "\
#                    select bigint(a.fee-b.fee)
#                    from
#	            (select
#	              sum(fact_fee)*100 as fee
#	            from 
#	              bass2.dw_acct_kpi_total_$timestamp )a,
#	            (select
#	              sum(fact_fee)*100 as fee
#	            from 
#	              bass2.dw_acct_kpi_total_$last_day )b   "
#	}                  
#        puts $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set INCOME [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	puts $INCOME
#
#	#02:---D_COMM_USERS ����ͨ���û���----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "select
#                       count(product_no)
#                     from bass1.int_09901_mid1_ds 
#                     where op_time=$timestamp "
#        puts $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set D_COMM_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#
#	aidb_commit $conn
#	puts $D_COMM_USERS
#
#
#	#03:---M_COMM_USERS �����ۼ�ͨ���û���---#
#	set handle [aidb_open $conn]
#
#	 set sql_buff "select
#                       count( product_no)
#                     from bass1.INT_09901_MID1_DS
#                     where op_time<=$timestamp "
#                        
#        puts $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set M_COMM_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#
#	aidb_commit $conn
#	puts $M_COMM_USERS
#
#	#04:---D_VOICE_USERS ���������û���---#
#	set handle [aidb_open $conn]
#
#	 set sql_buff "select
#                        count(distinct product_no)
#                     from bass1.int_210012916_${op_month} 
#                     where op_time=$timestamp"
#        puts  $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set D_VOICE_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#
#	aidb_commit $conn
#	puts $D_VOICE_USERS
#
#	#05:---M_VOICE_USERS  �����վ������û���---#
#	set handle [aidb_open $conn]
#
#	 set sql_buff "select
#                        bigint(count(distinct product_no)/int($today_dd))
#                     from bass1.int_210012916_${op_month} 
#                     where op_time<=$timestamp"
#        puts  $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set M_VOICE_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	puts $M_VOICE_USERS
#
#
#	#---M_BILL_USERS �����շ��û���--#
#	set handle [aidb_open $conn]
#
#	 set sql_buff "select
#                        count(distinct product_no) 
#                     from bass2.dw_acct_shoulditem_${timestamp} 
#                     where fact_fee>0 "
#
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set M_BILL_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#
#	aidb_commit $conn
#	puts $M_BILL_USERS
#
#	#----���ɾ������ֵ�kpiָ��--------------------------#
#	#----D_UNION_USERS          ������ͨ�û���
#        #----D_TELE_FIX_USERS       ���յ���/��ͨ�̶��û���
#        #----D_TELE_NET_USERS       ���յ���/��ͨС��ͨ�û���
#        #----D_TELE_OTH_USERS       ����������Ӫ���û���
#        #----M_UNIO_USERS           ������ͨ�û���
#        #----M_TELE_FIX_USERS       ���µ���/��ͨ�̶��û���
#        #----M_TELE_NET_USERS       ���µ���/��ͨС��ͨ�û���
#        #----M_TELE_OTH_USERS       ����������Ӫ���û���
#        #----D_UNION_NUSERS         ������ͨ�����û���
#        #----D_TELE_FIX_NUSERS      ���յ���/��ͨ�̶������û���
#        #----D_TELE_NET_NUSERS      ���յ���/��ͨС��ͨ�����û���
#        #----D_OTH_NUSERS           ����������Ӫ�������û���
#        #----M_UNION_NUSERS         ������ͨ�����û���
#        #----M_TELE_FIX_NUSERS      ���µ���/��ͨ�̶������û���
#        #----M_TELE_NET_NUSERS      ���µ���/��ͨС��ͨ�����û���
#        #----M_OTH_NUSERS           ����������Ӫ�������û���
#
#        set handle [aidb_open $conn]
#
#        set sql_buff "\
#            select
#              sum(case when COMP_ACTIVE_MARK=1 and comp_brand_id in (3,4,7) then 1 else 0 end) as D_UNION_USERS
#	      ,sum(case when COMP_ACTIVE_MARK=1 and comp_brand_id in (2,6) then 1 else 0 end) as D_TELE_FIX_USERS
#	      ,sum(case when COMP_ACTIVE_MARK=1 and comp_brand_id in (1,8) then 1 else 0 end) as D_TELE_NET_USERS
#	      ,sum(case when COMP_ACTIVE_MARK=1 and comp_brand_id=5 then 1 else 0 end) as D_TELE_OTH_USERS
#	      ,sum(case when bigint(replace(char(COMP_LAST_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (3,4,7) then 1 else 0 end) as M_UNIO_USERS
#	      ,sum(case when bigint(replace(char(COMP_LAST_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (2,6) then 1 else 0 end) as M_TELE_FIX_USERS
#	      ,sum(case when bigint(replace(char(COMP_LAST_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (1,8) then 1 else 0 end) as M_TELE_NET_USERS
#	      ,sum(case when bigint(replace(char(COMP_LAST_DATE),'-','')) >=$this_month_first_day and comp_brand_id =5 then 1 else 0 end) as M_TELE_OTH_USERS
#	      ,sum(case when COMP_DAY_NEW_MARK=1 and comp_brand_id in (3,4,7) then 1 else 0 end) as D_UNION_NUSERS
#              ,sum(case when COMP_DAY_NEW_MARK=1 and comp_brand_id in (2,6) then 1 else 0 end) as D_TELE_FIX_NUSERS
#              ,sum(case when COMP_DAY_NEW_MARK=1 and comp_brand_id in (1,8) then 1 else 0 end) as D_TELE_NET_NUSERS
#              ,sum(case when COMP_DAY_NEW_MARK=1 and comp_brand_id=5 then 1 else 0 end) as D_OTH_NUSERS
#	      ,sum(case when bigint(replace(char(COMP_OPEN_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (3,4,7) then 1 else 0 end) as M_UNION_NUSERS
#              ,sum(case when bigint(replace(char(COMP_OPEN_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (2,6) then 1 else 0 end) as M_TELE_FIX_NUSERS
#              ,sum(case when bigint(replace(char(COMP_OPEN_DATE),'-','')) >=$this_month_first_day and comp_brand_id in (1,8) then 1 else 0 end) as M_TELE_NET_NUSERS
#              ,sum(case when bigint(replace(char(COMP_OPEN_DATE),'-','')) >=$this_month_first_day and comp_brand_id =5 then 1 else 0 end) as  M_OTH_NUSERS
#	    from bass2.Dw_comp_cust_dt"
#        puts  $sql_buff
#        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#        	WriteTrace $errmsg 1012
#        	return -1
#        }
#
#        while {[set p_success [aidb_fetch $handle]] != "" } {
#        	set D_UNION_USERS     	[lindex $p_success 0]
#        	set D_TELE_FIX_USERS  	[lindex $p_success 1]
#		set D_TELE_NET_USERS   	[lindex $p_success 2]
#		set D_TELE_OTH_USERS  	[lindex $p_success 3]
#		set M_UNIO_USERS      	[lindex $p_success 4]
#        	set M_TELE_FIX_USERS  	[lindex $p_success 5]
#		set M_TELE_NET_USERS   	[lindex $p_success 6]
#		set M_TELE_OTH_USERS  	[lindex $p_success 7]
#		set D_UNION_NUSERS    	[lindex $p_success 8]
#        	set D_TELE_FIX_NUSERS 	[lindex $p_success 9]
#		set D_TELE_NET_NUSERS  	[lindex $p_success 10]
#		set D_OTH_NUSERS      	[lindex $p_success 11]
#		set M_UNION_NUSERS    	[lindex $p_success 12]
#        	set M_TELE_FIX_NUSERS 	[lindex $p_success 13]
#		set M_TELE_NET_NUSERS  	[lindex $p_success 14]
#		set M_OTH_NUSERS      	[lindex $p_success 15]
#
#			}
#
#	aidb_commit $conn
#
#         puts "��������"
#          puts $D_UNION_USERS
#          puts $D_TELE_FIX_USERS
#          puts $D_TELE_NET_USERS
#          puts $D_TELE_OTH_USERS
#          puts $M_UNIO_USERS
#          puts $M_TELE_FIX_USERS
#          puts $M_TELE_NET_USERS
#          puts $M_TELE_OTH_USERS
#          puts $D_UNION_NUSERS
#          puts $D_TELE_FIX_NUSERS
#          puts $D_TELE_NET_NUSERS
#          puts $D_OTH_NUSERS
#          puts $M_UNION_NUSERS
#          puts $M_TELE_FIX_NUSERS
#          puts $M_TELE_NET_NUSERS
#          puts $M_OTH_NUSERS
#
#
#     #----W_COMM_NUMS ����ͨ�ſͻ���
#
#     set friday [ clock format [ clock scan $optime ] -format "%A" ]
#     if { ${friday} == "������" } {
#     	                           set handle [aidb_open $conn]
#	                           set sql_buff "select
#                                                   count(distinct product_no)
#                                                 from bass1.int_210012916_${op_month} 
#                                                 where op_time>= bigint(replace(char((date('$optime') -6 days )),'-',''))"   
#                                   puts  $sql_buff                             
#                                   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		                        WriteTrace $errmsg 1001
#		                        return -1
#	                            }
#	                           if [catch {set W_COMM_NUMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		                        WriteTrace $errmsg 1002
#		                        return -1
#	                            }
#	                            aidb_commit $conn
#	
#		                 } else {
#				           set W_COMM_NUMS "0"
#     }
#     puts $W_COMM_NUMS
#
#     #---�ý����ȥ���죬��Ϊbass2.DW_ACCT_SHOULDITEM_20070314��һ�����³��ۼƵ�����������---#
#     #---NEWBUSI_FEE     ������ҵ������
#     #---VOICEADD_FEE    ����������ֵҵ������
#     #---SMS_FEE         ���ն�����ҵ������
#     #---NONSMS_FEE      ���շǶ�����ҵ������
#     set handle [aidb_open $conn]
#        set sql_buff "\
#            select
#                 bigint(NEWBUSI_FEE)
#                ,bigint(VOICEADD_FEE)
#                ,bigint(SMS_FEE)
#                ,bigint(NEWBUSI_FEE - VOICEADD_FEE - SMS_FEE) as NONSMS_FEE
#                from
#                (
#                select
#                NEWBUSI_FEE1-NEWBUSI_FEE2 as NEWBUSI_FEE
#                ,VOICEADD_FEE1-VOICEADD_FEE2 as VOICEADD_FEE
#                ,SMS_FEE1-SMS_FEE2 as SMS_FEE
#                from
#                (
#                select
#
#                 sum(case when tt=1 then NEWBUSI_FEE else 0 end) as NEWBUSI_FEE1
#                 ,sum(case when tt=2 then NEWBUSI_FEE else 0 end) as NEWBUSI_FEE2
#                 ,sum(case when tt=1 then VOICEADD_FEE else 0 end) as VOICEADD_FEE1
#                 ,sum(case when tt=2 then VOICEADD_FEE else 0 end) as VOICEADD_FEE2
#                 ,sum(case when tt=1 then SMS_FEE else 0 end) as SMS_FEE1
#                 ,sum(case when tt=2 then SMS_FEE else 0 end) as SMS_FEE2
#                  from
#                  (
#                select
#                   1 as tt
#                   ,sum(case when item_id not in (82000002,82000001,82000006,82000015,82000016,82000018,82000019,82000020,82000021,82000022,82000023,82000024,82000029,82000030,82000034,82000101,82000104) then fact_fee else 0 end)*100 as NEWBUSI_FEE
#                   ,sum(case when item_id in (80000009,80000010,80000017,80000057) then fact_fee else 0 end)*100 as VOICEADD_FEE
#                   ,sum(case when item_id in (80000135) then fact_fee else 0 end)*100 as SMS_FEE
#                   from bass2.DW_ACCT_SHOULDITEM_${timestamp}
#                   union all
#                   select
#                   2 as tt
#                   ,sum(case when item_id not in (82000002,82000001,82000006,82000015,82000016,82000018,82000019,82000020,82000021,82000022,82000023,82000024,82000029,82000030,82000034,82000101,82000104) then fact_fee else 0 end)*100 as NEWBUSI_FEE
#                   ,sum(case when item_id in (80000009,80000010,80000017,80000057) then fact_fee else 0 end)*100 as VOICEADD_FEE
#                   ,sum(case when item_id in (80000135) then fact_fee else 0 end)*100 as SMS_FEE
#                   from bass2.DW_ACCT_SHOULDITEM_${timestamp}
#                   )a
#                   )b
#                   )c "
#        puts  $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#        while {[set  p_success [aidb_fetch $handle]] != "" } {
#        	set          NEWBUSI_FEE    	[lindex $p_success 0]
#        	set          VOICEADD_FEE   	[lindex $p_success 1]
#		set          SMS_FEE         	[lindex $p_success 2]
#		set          NONSMS_FEE     	[lindex $p_success 3]
#              }
#
#	aidb_commit  $conn
#
#	puts $NEWBUSI_FEE
#        puts $VOICEADD_FEE
#        puts $SMS_FEE
#        puts $NONSMS_FEE
#
#
#        #----��⣬�����ݲ��뵽����---#
#        set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_09901_DAY values($timestamp,'$timestamp','$INCOME','$D_COMM_USERS','$M_COMM_USERS','$D_VOICE_USERS','$M_VOICE_USERS','$M_BILL_USERS','$D_UNION_USERS','$D_TELE_FIX_USERS','$D_TELE_NET_USERS','$D_TELE_OTH_USERS','$M_UNIO_USERS','$M_TELE_FIX_USERS','$M_TELE_NET_USERS','$M_TELE_OTH_USERS','$D_UNION_NUSERS','$D_TELE_FIX_NUSERS','$D_TELE_NET_NUSERS','$D_OTH_NUSERS','$M_UNION_NUSERS','$M_TELE_FIX_NUSERS','$M_TELE_NET_NUSERS','$M_OTH_NUSERS','$W_COMM_NUMS','$NEWBUSI_FEE','$VOICEADD_FEE','$SMS_FEE','$NONSMS_FEE')
#                      "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#
#
#
#	aidb_close  $handle
#
#	return 0
#}
###############################