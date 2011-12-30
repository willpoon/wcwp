######################################################################################################
#�ӿ����ƣ���ҵ��վҵ�񶩹����
#�ӿڱ��룺02055
#�ӿ�˵����
#��������: G_A_02055_DAY.tcl
#��������: ����02055������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25
#�����¼��1.
#�޸���ʷ: 1. 2011-03-28 18:08:42 �޸�һ����ҵ��վ�������ݣ���89103000041929��ʧЧ,�뼯�ű���һ��!  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02055_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	set sql_buff "insert into BASS1.G_A_02055_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,PAY_TYPE
				,CREATE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				'1340' as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				case
					when C.PLAN100>0 then '1'
					when C.PLAN200>0 then '2'
					else '3'
				end as PAY_TYPE,
				'3' as CREATE_MODE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end as STATUS_ID
			from
				(select * from bass2.DW_ENTERPRISE_SUB_DS where SERVICE_ID='924') a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(
					select ORDER_ID,
					sum(case when FEATURE_ID ='92403003' and FEATURE_VALUE is not null then 1 else 0 end) as PLAN100,
					sum(case when FEATURE_ID ='92403004' and FEATURE_VALUE is not null then 1 else 0 end) as PLAN200,
					sum(case when FEATURE_ID  in ('92403002','92403005') and FEATURE_VALUE is not null then 1 else 0 end) as PLANELSE
					from bass2.DWD_GROUP_ORDER_FEATUR_${timestamp} where FEATURE_ID in ('92403002','92403003','92403004','92403005')
					group by ORDER_ID
				) c
			where DATE(A.done_date)='${op_time}'
			  AND A.SERVICE_ID=B.SERVICE_ID
				AND A.ORDER_ID=C.ORDER_ID
				and a.enterprise_id not in ('891910006274','891910006688','891910006714','891910006932')
			"


     puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

###2011-03-28 18:08:42 �޸�һ����ҵ��վ�������ݣ���89103000041929��ʧЧ,�뼯�ű���һ��!
###�˴���һ�����޸���	2011-03-29���ϳ������켴�Ѵ������ε�
##	set sql_buff "insert into G_A_02055_DAY
##									select 20110328
##										,a.ENTERPRISE_ID
##										,a.ENTERPRISE_BUSI_TYPE
##										,a.MANAGE_MODE
##										,a.PAY_TYPE
##										,a.CREATE_MODE
##										,'20110328' ORDER_DATE
##										,'2' STATUS_ID
##									from bass1.G_A_02055_DAY_down20110321 a
##									where  ENTERPRISE_ID = '89103000041929'
##							"
##     puts $sql_buff								
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	
	aidb_close $handle



  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02055_DAY a where exists 
								 (
								select * from 
									(
									select time_id,enterprise_id,enterprise_busi_type,manage_mode,pay_type,create_mode,order_date,count(*)
									from bass1.G_A_02055_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,enterprise_busi_type,manage_mode,pay_type,create_mode,order_date
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