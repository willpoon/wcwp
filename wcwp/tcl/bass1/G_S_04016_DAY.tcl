######################################################################################################
#�ӿ����ƣ���ҵ���ض��Ż���
#�ӿڱ��룺04016
#�ӿ�˵�������ſͻ�ҵ������ҵ�����ϲ����Ķ��Ż�������ʡ���ݱ�ʡ���ſͻ�����ҵ���ʵ�ʿ�չ�����
#          �ֱ���������Ż�����ýӿ��ϱ�
#��������: G_S_04016_DAY.tcl
#��������: ����04016������
#��������: ��
#Դ    ��1.
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼�� 20100729��forward_ismg_id ǰת�������ؼ���Ҫ��6λ����BOSS�Ǳ߳���10λ�ģ�ֱ��ץȡǰ6λ
#�޸���ʷ:  20100601 liuqf EC��ҵ���� ��sp_code ���壬������enterprise_id
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04016_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


#����ͨ��sp_code='931007'
#ũ��ͨ��sp_code in ('400089','422001')
#У��ͨ��sp_code in ('931047')
#С�����ţ�Ŀǰ��
#����ͨ��sp_code='931027'
#�ƶ�OA(ADC) : sp_code='931051'  and oper_code='AXZ3010501'
#�ƶ�CRM(ADC): sp_code='931051'  and oper_code='AXZ0010201'
#�ƶ�������(ADC): sp_code='931051'  and oper_code='AXZ0010101'
#����ͨ��ADC����
#SP_CODEoper_code
#400002137301
#400002137302
#400002137303
#901848-CSTDY
#901848-MJLCDY
#901848-ZXFXSDY
#901848-SCGJJRDY
#901848-SHQTJRDY
#901848-SXGMJZDY
#901848-SZHBJZDY
#901848-DHTDY
#901848-LCTDY
#901848-HYJSZDXDY
#90013911000014
#90013911000015
#90013911000016
#90013911000017
#90013911000018
#90013911000019
#90013911000021
#90013911000022
#90013911000023
#
#����ͨ��ADC����
#sp_code in ('600000','901870')  and oper_code like  'AJT%'
#MAS:
#SP_CODE IN ('931060',                                                                  
#'931061',                                                                  
#'931059',                                                                  
#'931062',                                                                  
#'931064',                                                                  
#'931063',                                                                  
#'931073',                                                                  
#'931066',                                                                  
#'931065')


#����ͨ��sp_code='931007'
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code='931007'"
        puts $sql_buff
        exec_sql $sql_buff
        
#ũ��ͨ��sp_code in ('400089','422001')
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code in ('400089','422001')"
        puts $sql_buff
        exec_sql $sql_buff
        
        
#У��ͨ��sp_code in ('931047')
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code in ('931047')"
        puts $sql_buff
        exec_sql $sql_buff
        
        
#����ͨ��sp_code='931027'
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code='931027'"
        puts $sql_buff
        exec_sql $sql_buff


#�ƶ�OA(ADC) : sp_code='931051'  and oper_code='AXZ3010501'
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code='931051'  and oper_code='AXZ3010501'"
        puts $sql_buff
        exec_sql $sql_buff

#�ƶ�CRM(ADC): sp_code='931051'  and oper_code='AXZ0010201'
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code='931051'  and oper_code='AXZ0010201'"
        puts $sql_buff
        exec_sql $sql_buff

#�ƶ�������(ADC): sp_code='931051'  and oper_code='AXZ0010101'
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code='931051'  and oper_code='AXZ0010101'"
        puts $sql_buff
        exec_sql $sql_buff



#����ͨ��ADC��
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and SP_CODE||oper_code in ('400002137301','400002137302','400002137303','901848-CSTDY','901848-MJLCDY','901848-ZXFXSDY','901848-SCGJJRDY',
                       '901848-SHQTJRDY','901848-SXGMJZDY','901848-SZHBJZDY','901848-DHTDY','901848-LCTDY','901848-HYJSZDXDY','90013911000014',
                       '90013911000015','90013911000016','90013911000017','90013911000018','90013911000019','90013911000021','90013911000022',
                       '90013911000023')"
        puts $sql_buff
        exec_sql $sql_buff



#����ͨ��ADC����
#
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and sp_code in ('600000','901870')  and oper_code like  'AJT%'"
        puts $sql_buff
        exec_sql $sql_buff
        
        
        
#MAS
	set sql_buff "insert into bass1.G_S_04016_DAY
             select
		                 $timestamp
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(a.record_type)),'')
                     ,a.product_no
                     ,a.sp_code
                     ,value(substr(ser_code,1,15),'0')
                     ,substr(ser_code,1,21)
                     ,value(substr(oper_code,1,10),'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then '0'
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
                     ,case when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,value(smsc_id,'0')
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp a,
                     bass2.dwd_enterprise_membersub_$timestamp b  
                   where 
                     drtype_id<>61102 and process_time is not null and substr(sp_code,1,12) is not null  and a.user_id = b.sub_id and 
                     a.SP_CODE IN ('931060','931061','931059','931062','931064','931063','931073','931066','931065')"
        puts $sql_buff
        exec_sql $sql_buff
              
              
      	set sql_buff "delete from bass1.G_S_04016_DAY where time_id = $timestamp and record_type not in ('00','01','10','11');"
        puts $sql_buff
        exec_sql $sql_buff
              
                
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

