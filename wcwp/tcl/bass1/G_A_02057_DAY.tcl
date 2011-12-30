######################################################################################################
#�ӿ����ƣ�ר��ҵ�񶩹����
#�ӿڱ��룺02057
#�ӿ�˵����
#��������: G_A_02057_DAY.tcl
#��������: ����02057������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25
#�����¼��1.
#�޸���ʷ: 1.20090816 liuzhilong �޸ı�֤����Ψһ�Դ����ֶ�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02057_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#�˿���BOSSû�д����ԣ����԰�һ��ר��һ���˿�������ͳ��
	set sql_buff "insert into BASS1.G_A_02057_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,PORT_NUMS
				,BAND_WIDTH
				,NUMS
				,ORDER_DATE
				,STATUS_ID
			)
			select 
				${timestamp},
				A.ENTERPRISE_ID,
				case when a.prod_id='91201012' then '1180' else '1170' end as ENTERPRISE_BUSI_TYPE,
				char(sum(case when A.REC_STATUS=1 then 1 else 0 end)) as PORT_NUMS,
				char(sum(case when E.FEATURE_VALUE='1' then 1
                    	when E.FEATURE_VALUE='2' then 2
                    	when E.FEATURE_VALUE='3' then 4
                    	when E.FEATURE_VALUE='4' then 8
                    	when E.FEATURE_VALUE='5' then 34
                    	else 0 	end)) as BAND_WIDTH,
        char(sum(case when A.REC_STATUS=1 then 1 else 0 end))  as NUMS,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
				else '2'
				end as STATUS_ID
			from
				(select * from bass2.DW_ENTERPRISE_SUB_DS where service_id='912') a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				bass2.DWD_GROUP_ORDER_FEATUR_${timestamp} e
			where DATE(A.done_date)='${op_time}'
				and A.SERVICE_ID=B.SERVICE_ID
				and A.ORDER_ID=E.ORDER_ID
				and E.FEATURE_ID in ('805102001','805102002')
			group by A.ENTERPRISE_ID,
			case when a.prod_id='91201012' then '1180' else '1170' end,
			replace(char(date(A.done_date)),'-',''),
                case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
				else '2'
				end
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
##	set sql_buff "delete  from bass1.G_A_02057_DAY a where exists
##								(
##								select * from 
##									(
##									select time_id,enterprise_id,enterprise_busi_type,band_width,order_date,count(*)
##									from bass1.G_A_02057_DAY
##									where time_id = $timestamp
##									group by time_id,enterprise_id,enterprise_busi_type,band_width,order_date
##									having count(*)>1
##									) as b
##								where a.time_id = b.time_id
##								  and a.enterprise_id = b.enterprise_id
##								  and a.enterprise_busi_type = b.enterprise_busi_type
##								  and a.band_width = b.band_width
##								  and a.order_date = b.order_date
##								  and a.time_id=$timestamp
##								)"
# �޸�group by ���ֶ�
	set sql_buff "delete  from bass1.G_A_02057_DAY a where exists
								(
								select * from 
									(
									select time_id,enterprise_id,enterprise_busi_type,count(*)
									from bass1.G_A_02057_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,enterprise_busi_type
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
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
