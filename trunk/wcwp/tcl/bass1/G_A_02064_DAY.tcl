######################################################################################################
#�ӿ����ƣ��ƶ�400ҵ�񶩹����
#�ӿڱ��룺02064
#�ӿ�˵����
#��������: G_A_02064_DAY.tcl
#��������: ����02064������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-11-23
#�����¼��1.
#�޸���ʷ: 1.modify ����ҵ���߼� 20100423
#�޸���ʷ: 2011-06-30 12:23:04 �޸Ĺ���ģʽΪ3
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02064_DAY where TIME_ID=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#	set sql_buff "insert into BASS1.G_A_02064_DAY
#			(
#				TIME_ID,
#				ENTERPRISE_ID,
#				ENTERPRISE_BUSI_TYPE,
#				MANAGE_MODE,
#				CDR_NUM_CNT,
#				SMS_NUM_CNT,
#				NUM_ALL_CNT,
#				NUM_MOBILE_CNT,
#				NUM_OTHER_CNT,
#				ORDER_DATE,
#				STATUS_ID
#			)
#			select distinct
#				${timestamp},
#				A.enterprise_id,
#				'1520' as enterprise_busi_type,
#				case
#					when upper(B.config_value)='MAS' then '1'
#					when upper(B.config_value)='ADC' then '2'
#					else '3'
#				end as manage_mode,
#				char(value(c.cdr_num_cnt,0)) as cdr_num_cnt,
#				char(value(d.sms_num_cnt,0)) as sms_num_cnt,
#				char(value(e.num_all_cnt,0)) as num_all_cnt,
#				char(value(e.num_all_cnt,0)) as num_mobile_cnt,
#				'0',
#				replace(char(date(A.done_date)),'-','') as order_date,
#				case
#					when rec_status=1 then '1'
#					when rec_status=0 then '2'
#				else '2'
#				end as status_id
#			from (select * from bass2.dw_enterprise_sub_ds where service_id in ('931')) a
#			inner join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id=b.service_id
#			left join (select group_id,count(*) cdr_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803601002' and date(done_date)='${op_time}' group by group_id) C on a.enterprise_id=c.group_id
#			left join (select group_id,count(*) sms_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803606001' and date(done_date)='${op_time}' group by group_id) D on a.enterprise_id=D.group_id
#			left join (select group_id,sum((length(feature_value)-length(replace(feature_value,';','')))/2*length(';')) num_all_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803600014' and date(done_date)='${op_time}' group by group_id) e on a.enterprise_id=e.group_id
#		   where DATE(A.done_date)='${op_time}'
#			 and a.enterprise_id not in ('891910006274','891910006688','891910006714','891910006932')
#			"
#

	set sql_buff "insert into BASS1.G_A_02064_DAY
			(
				TIME_ID,
				ENTERPRISE_ID,
				ENTERPRISE_BUSI_TYPE,
				MANAGE_MODE,
				CDR_NUM_CNT,
				SMS_NUM_CNT,
				NUM_ALL_CNT,
				NUM_MOBILE_CNT,
				NUM_OTHER_CNT,
				ORDER_DATE,
				STATUS_ID
			)
			select distinct
				${timestamp},
				A.enterprise_id,
				'1520' as enterprise_busi_type,
				'3' manage_mode,
				char(value(c.cdr_num_cnt,0)) as cdr_num_cnt,
				char(value(d.sms_num_cnt,0)) as sms_num_cnt,
				char(value(e.num_all_cnt,0)) as num_all_cnt,
				char(value(e.num_all_cnt,0)) as num_mobile_cnt,
				'0',
				replace(char(date(A.done_date)),'-','') as order_date,
				case
					when rec_status=1 then '1'
					when rec_status=0 then '2'
				else '2'
				end as status_id
			from (select * from bass2.dw_enterprise_sub_ds where service_id in ('931')) a
			inner join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id=b.service_id
			left join (select group_id,count(*) cdr_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803601002' and date(done_date)='${op_time}' group by group_id) C on a.enterprise_id=c.group_id
			left join (select group_id,count(*) sms_num_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803606001' and date(done_date)='${op_time}' group by group_id) D on a.enterprise_id=D.group_id
			left join (select group_id,sum((length(feature_value)-length(replace(feature_value,';','')))/2*length(';')) num_all_cnt from bass2.dwd_group_order_featur_${timestamp} where feature_id='803600014' and date(done_date)='${op_time}' group by group_id) e on a.enterprise_id=e.group_id
		   where DATE(A.done_date)='${op_time}'
			 and a.enterprise_id not in ('891910006274','891910006688','891910006714','891910006932')
			"
			
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02064_DAY a where exists 
								 (
								select * from 
									(
								select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date,count(*)
								from bass1.G_A_02064_DAY
								group by time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date
								having count(*)>1
								) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.order_date = b.order_date
								  and a.time_id=$timestamp
								 )"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		

#2011-06-30 11:28:55
#һ�����޸�
##�ƶ�400
#
#       set sql_buff "
#       insert into G_A_02064_DAY select * from G_A_02064_DAY_FIX
#       "
#       exec_sql $sql_buff
#


# 2011-08-12 
#�޳� 1520 ���Լ���
#
#     set sql_buff "
#     insert into G_A_02064_DAY select * from G_A_02064_DAY_FIX20110812
#     "
#     exec_sql $sql_buff
#     
	return 0
}