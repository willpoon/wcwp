######################################################################################################
#�ӿ����ƣ�����100ҵ�񶩹����
#�ӿڱ��룺02058
#�ӿ�˵����
#��������: G_A_02058_DAY.tcl
#��������: ����02058������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25
#�����¼��1.
#�޸���ʷ: 1.ҵ���߼��ϱ�Ϊ bigint(e.porder_id)=bigint(a.order_id) ����
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02058_DAY  where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


#2011-05-31 19:28:05 һ�����޸�		
#set sql_buff "
#insert into G_A_02058_DAY select * from   G_A_02058_DAY_T
#"
#exec_sql $sql_buff
#

	#ͨ����Ʒ��������ϵ�е�pack_id�����ƶ���100ҵ���Լ���porder_id���ڲ�Ʒ����ID��������ץȡ����100�Ķ���ҵ��
	set sql_buff "insert into BASS1.G_A_02058_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,SUB_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				D.BASS1_VALUE as SUB_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
                else '2'					
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a
				inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
				inner join (select * from bass1.ALL_DIM_LKP_163 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
				inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') d on A.SERVICE_ID=D.XZBAS_VALUE
				inner join 
				(select porder_id,porder_name from bass2.DW_PACK_ORDER_INFO_$timestamp where pack_id in('10086','10087') and rec_status=1) e
				  on bigint(e.porder_id)=bigint(a.order_id)
			where DATE(A.done_date)='${op_time}'
			  and a.enterprise_id not in ('891910006274','891880005002')"
			
##		DATE(A.CREATE_DATE)='${op_time}'
##

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
	set sql_buff "delete  from bass1.G_A_02058_DAY a where exists
								(
								select * from 
								(
								select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date,count(*)
								from bass1.G_A_02058_DAY
								where time_id = $timestamp
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

	


	return 0
}