######################################################################################################
#�ӿ����ƣ���ҵ����ҵ�񶩹����
#�ӿڱ��룺02056
#�ӿ�˵����
#��������: G_A_02056_DAY.tcl
#��������: ����02056������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02056_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	set sql_buff "insert into BASS1.G_A_02056_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,PAY_TYPE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				'1210' as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
                c.pay_type,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
                else '2'					
				end as STATUS_ID
			from
				(select * from bass2.DW_ENTERPRISE_SUB_DS where SERVICE_ID='925') a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select 
						order_id,
						case when feature_value = '92502001'  then '1' 
						     when feature_value <> '92502001' then '2'
						end as pay_type
						from bass2.DWD_GROUP_ORDER_FEATUR_${timestamp}
						where feature_id in ('92503002')) c
			where DATE(A.done_date)='${op_time}'
			  AND A.SERVICE_ID=B.SERVICE_ID
		    AND A.ORDER_ID=C.ORDER_ID"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


#2011-05-30 16:04:52 
#һ�����޸�
##��ҵ���伯�Ŷ����޸����޳����Լ���
#
#	set sql_buff "
#	insert into G_A_02056_DAY select * from G_A_02056_DAY_T
#	"
#	exec_sql $sql_buff
#	
  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02056_DAY a where exists
								(
								select * from 
								(
								select time_id,enterprise_id,enterprise_busi_type,manage_mode,pay_type,order_date,count(*)
								from bass1.G_A_02056_DAY
								where time_id = $timestamp
								group by time_id,enterprise_id,enterprise_busi_type,manage_mode,pay_type,order_date
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


	return 0
}