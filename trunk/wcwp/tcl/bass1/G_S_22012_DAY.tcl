######################################################################################################
#�ӿ����ƣ���KPI
#�ӿڱ��룺22012
#�ӿ�˵������¼��KPI��Ϣ
#��������: G_S_22012_DAY.tcl
#��������: ����22012������
#��������: ��
#Դ    ��1.bass2.dw_acct_shoulditem_yyyymmdd--
#          2.bass2.dw_comp_all_dt--
#          3.bass2.Dw_comp_cust_dt--
#          4.bass2.dw_product_yyyymmdd --
#          5.bass2.dw_comp_all_yyyymmdd--
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��ΪĿǰ���������������ϲ��ܴӶ��ų����������ݲ�׼ȷ��������Ŀǰ�޷������
#          2.�ýӿڳ�����Ϊ�õ���dt�����Բ���������ǰ�����ݡ�
#�޸���ʷ: 20090422 �޸��˵����վ������û���ͳ���㷨 �Ļ�ѧ  
#�޸���ʷ��20090708 �޸ĵ����շ��û���ͳ�ƿھ� ȥ�� usertype_id=8 �ļ��������û�
#          20090902 1.6.2�淶���������ֶ�(�����ͻ������ͻ���������)
#          20090928 �����շ��û����ϱ���ʱΪ0
#          20091022 1.6.3�淶�޸�(ȥ��'�����շ��û���'ָ���)
#          20091123 �޸��������ھ� a.apn_ni not in ('CMTDS') Ϊdrtype_id not in (8307)
#          20100120 �޸����ݿ�Ҳ�������ͻ������޳�����crm_brand_id2<>70 �޸Ŀͻ��������ݡ������ͻ����Ŀھ� userstatus_id in (1,2,3,6,8)
#          20111231 1.���żƷ�����������ҵ���ض��ţ� 2.��ҵ���ض��żƷ���
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
        #���� YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #�������һ��
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        puts $last_month_last_day        

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22012_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###
###	#02:�����շ��û���
###    set handle [aidb_open $conn]
###    set sql_buff "select
###            			count(distinct a.user_id)
###			            from  bass2.dw_acct_shoulditem_$timestamp a
###			              inner join 
###			              (
###		                 select user_id from bass2.dw_product_$timestamp where userstatus_id<>0 and usertype_id in (1,2,9)
###		                 except
###		                 select user_id from bass2.dw_product_${last_month_last_day} where userstatus_id in (0,4,5,7)
###		                 ) b
###			               on a.user_id=b.user_id
###			            with ur  
###             		"	               
###    puts $sql_buff
###    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###	if [catch {set M_BILL_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	puts "�����շ��û���:$M_BILL_USERS"
###

	#03:�����ͻ���
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
             		"	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_NEW_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "�����ͻ���Ϊ:$M_NEW_USERS"


	#04:�ͻ�������
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp
                   where usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
             		"	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_DAO_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "�ͻ�������Ϊ:$M_DAO_USERS"


	#05:�Ʒ�ʱ��
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.call_duration_m)) 
##                   from  bass2.dw_call_$timestamp a,bass2.dw_product_$timestamp b 
##                  where a.user_id =  b.user_id
##                    and b.usertype_id in (1,2,9)
##                    and b.test_mark <>1 and b.crm_brand_id2<>70
##										and b.userstatus_id not in (2,4,5,7,8)
##             		"	     

    set handle [aidb_open $conn]
    set sql_buff "select sum(bigint(a.call_duration_m)) 
                   from  bass2.dw_call_$timestamp a,bass2.dw_product_$timestamp b 
                  where a.user_id =  b.user_id
                    and b.usertype_id in (1,2,9)
                    and b.test_mark <>1 and b.userstatus_id not in (4,5,7,9)
             		"	 
             		          
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "�Ʒ�ʱ��Ϊ:$M_BILL_DURATION"


##	#06:�ƶ���������
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
##                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
##                   where a.user_id = b.user_id
##                     and b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##             		"	               
##    puts $sql_buff
##    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set M_DATA_FLOWS [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "�ƶ���������Ϊ:$M_DATA_FLOWS"


	#06:�ƶ���������(ȥ������������)
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
##                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
##                   where a.user_id = b.user_id
##                     and b.usertype_id in (1,2,9)
##					 and b.test_mark <>1 
##					 and b.crm_brand_id2<>70
##					 and b.userstatus_id not in (2,4,5,7,8)
##                     and a.drtype_id not in (8307)
##             		"
##
    set handle [aidb_open $conn]
    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
                   where a.user_id = b.user_id
                     and b.usertype_id in (1,2,9)
					 and b.test_mark <>1 
					 and b.userstatus_id not in (4,5,7,9)
                     and a.drtype_id not in (8307)
             		"

    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_DATA_FLOWS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "�ƶ���������Ϊ:$M_DATA_FLOWS"


	#07:���żƷ���
	#--��Ե����
	#--�ƶ�����(����)(12580)��ȫ����Զ����顢��ת���š����Ż���(������־����)(12590)����������(�����嵥)
	#--���ſͻ�����(�������л���)#--�������żƷ��������������Ż������żƷ�������
	
##    set handle [aidb_open $conn]
##    set sql_buff "
##		    select sum(cnts) from 
##		    (
##					 select value(sum(counts),0) cnts from bass2.dw_newbusi_sms_$timestamp  a,bass2.dw_product_$timestamp b 
##						where a.user_id=b.user_id
##						  and  b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##						  and  a.calltype_id=0
##						  and  a.final_state=1
##						union all
##						select value(sum(counts),0) cnts from  bass2.dw_newbusi_call_$timestamp a,bass2.dw_product_$timestamp b 
##						where  a.user_id=b.user_id
##						  and b.usertype_id in (1,2,9) and b.userstatus_id  in (1,2,3,6) 
##						  and a.calltype_id=0
##						  and a.svcitem_id in  (100003,100021,100025,100026,100027)
##						union  all
##						select value(sum(counts),0) cnts from bass2.dw_newbusi_ismg_$timestamp  a,bass2.dw_product_$timestamp b 
##						where a.user_id=b.user_id
##						  and  b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##						  and  a.calltype_id=0
##						  and  a.send_state='0'
##						  and  a.svcitem_id in (300009) 
##						union all
##						select value(sum(counts),0) cnts from  bass2.dw_newbusi_ismg_$timestamp a,bass2.dw_product_$timestamp b 
##							where a.user_id  = b.user_id
##							  and b.usertype_id in (1,2,9) and b.userstatus_id in  (1,2,3,6) 
##							  and a.record_type in (0,1,10,11)
##							  and a.send_state='0'
##					) as aa
##           "	               
##    puts $sql_buff
##    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set M_BILL_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "���żƷ���Ϊ:$M_BILL_SMS"
##


# ���ֺ�21007/04005/04014�ӿ�һ��
#1. meng wang  substr(a.sp_code,1,12) is not null
#2. ������־���Ż���   ('12590')       
#3.���Ŷ���
#4.�ƶ����顢ȫ�����ת���š���������
    set handle [aidb_open $conn]
    set sql_buff "
		    select sum(cnts) from 
		    (
						select value(sum(counts),0) cnts from bass2.dw_newbusi_ismg_$timestamp  a,bass2.dw_product_$timestamp b 
						where a.user_id=b.user_id
						  and b.usertype_id in (1,2,9)
						  and b.test_mark <>1 
						  and b.userstatus_id not in (4,5,7,9)
						  and a.calltype_id in (0,1,10,11)
						  and a.send_state='0'
						  and a.drtype_id<>61102 
						  and substr(a.sp_code,1,12) is not null
						  and a.svcitem_id in (300001,300002,300003,300004)
					union all		
					 select value(sum(counts),0) cnts from bass2.dw_newbusi_sms_$timestamp  a,bass2.dw_product_$timestamp b 
						where a.user_id=b.user_id
						  and b.usertype_id in (1,2,9)
						  and b.test_mark <>1
						  and b.userstatus_id not in (4,5,7,9)
						  and  a.calltype_id in (0)
						  and  a.final_state=1
					) as aa
           "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "���żƷ���Ϊ:$M_BILL_SMS"

#2011.12.31 ��ҵ���ض��żƷ���
   set handle [aidb_open $conn]
    set sql_buff "
					select value(count(0),0) cnts from    G_S_04016_DAY 
					where time_id = $timestamp 
					and RECORD_TYPE in ('00','01','10','11')
					and SEND_STATUS = '0'
           "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_HANGYE_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "��ҵ���ض��żƷ���Ϊ:$M_BILL_HANGYE_SMS"


	#08:�����ͻ���
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp  
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1
             		 "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_OFF_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "�����ͻ���Ϊ:$M_OFF_USERS"


	#09:���żƷ���
    set handle [aidb_open $conn]
    set sql_buff "
        select sum(cnts) from 
            (
             select sum(counts) cnts from bass2.dw_newbusi_mms_$timestamp a,bass2.dw_product_$timestamp b 
							where  a.user_id  = b.user_id
							  and b.usertype_id in (1,2,9)
							  and b.test_mark <>1 
							  and b.userstatus_id not in (4,5,7,9)
							  and a.mm_type=0
							  and a.app_type in (0)
							  and a.send_status in (0,1,2,3)
						union all
						select sum(counts) cnts from bass2.dw_newbusi_mms_$timestamp a,bass2.dw_product_$timestamp b 
							where  a.user_id  = b.user_id
							  and b.usertype_id in (1,2,9)
							  and b.test_mark <>1 
							  and b.userstatus_id not in (4,5,7,9)
							  and a.app_type in (1,2,3,4)
						) as aa
            "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_MMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "���żƷ���Ϊ:$M_BILL_MMS"


  #ָ��ȫ�����
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22012_day values
	             (
	              $timestamp
	              ,'$timestamp'
                ,'$M_NEW_USERS'
                ,'$M_DAO_USERS'
                ,'$M_BILL_DURATION'
                ,'$M_DATA_FLOWS'
                ,'$M_BILL_SMS'
                ,'$M_BILL_HANGYE_SMS'
                ,'$M_OFF_USERS'
                ,'$M_BILL_MMS'
	             ) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}             
##############################################
	return 0      
}                     
