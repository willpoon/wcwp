######################################################################################################
#�ӿ����ƣ�BlackBerry�����û��󶨹�ϵ
#�ӿڱ��룺02060
#�ӿ�˵����
#��������: G_A_02060_DAY.tcl
#��������: ����02060������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-07-06
#�����¼��1.
#�޸���ʷ: 1.liuzhilong 2010-9-29 ����ά�� 99001646 BlackBerry498Ԫ�¹��ܷ�
#          2.liuqf   2010-11-10 �޸Ŀھ��Լ�������ʷ����
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

  #���� yyyy-mm-dd
  set optime $op_time

  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02060_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#1230	BLACK BERRY

  #99001606	BlackBerry398Ԫ�¹��ܷ�
  #99001605	BlackBerry598Ԫ�¹��ܷ�
  #liuzhilong 2010-9-29 ����ά�� 99001646 BlackBerry498Ԫ�¹��ܷ� when e.sprom_id=99001646 then '2' 
	#�ʷ�����δȷ�Ͽھ���������
    #99001605	BlackBerry598Ԫ�¹��ܷ�
    #99001606	BlackBerry398Ԫ�¹��ܷ�
    #99001607	BlackBerry598Ԫ�¹��ܷ����һ���¹��ܷ�
    #99001644	BlackBerry198Ԫ�¹��ܷ�
    #99001645	BlackBerry298Ԫ�¹��ܷ�
    #99001646	BlackBerry498Ԫ�¹��ܷ�
    #99001660	BlackBerry266Ԫ�¹��ܷ�
    #99001661	BlackBerry366Ԫ�¹��ܷ�
    #99001662	BlackBerry466Ԫ�¹��ܷ�
    #99001663	BlackBerry566Ԫ�¹��ܷ�
    #99001664	BlackBerry666Ԫ�¹��ܷ�
##
##�ʷ����ͣ�
##1-398Ԫ�ײ�
##2-498Ԫ�ײ�
##3-598Ԫ�ײ�
##4-����

	set sql_buff "insert into BASS1.G_A_02060_DAY                      
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,EXPIRE_DATE
				,PAY_TYPE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				D.USER_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.EXPIRE_DATE)),'-','') as EXPIRE_DATE,
				case when e.sprom_id in (99001605,99001607) then '3'
						 when e.sprom_id=99001606 then '1'
						 when e.sprom_id=99001646 then '2' 
				else '4' end  as PAY_TYPE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
				else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a
				inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
				inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
        left join bass2.DW_ENTERPRISE_MEMBERSUB_DS d on A.ORDER_ID=D.ORDER_ID
        inner join bass2.dw_product_sprom_ds e on d.user_id =e.user_id
        inner join bass2.dw_product_bass1_${timestamp} f on d.user_id =f.user_id
			where DATE(A.done_date)='${op_time}' and
			  C.BASS1_VALUE='1230'
				and e.SPROM_ID in (99001605,99001606,99001607,99001644,99001645,99001646,99001660,99001661,99001662,99001663,99001664)
				and f.FREE_MARK=0
				and f.TEST_MARK=0
			"
#--- 

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#  #������ʷ����
#  set handle [aidb_open $conn]
#	set sql_buff "
#	insert into BASS1.G_A_02060_DAY 
#			select 
#			20101110
#			,enterprise_id
#			,user_id
#			,enterprise_busi_type
#			,manage_mode
#			,'20991231'
#			,pay_type
#			,'20101110'
#			,'2'
#			from BASS1.G_A_02060_DAY where time_id=20100928
#		"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#        aidb_close $handle

#2011-05-30 16:04:52 
#һ�����޸�
###blackberry ���ڲ��Լ��ŵĸ��˶����޸�
#	set sql_buff "
#	insert into G_A_02060_DAY select * from G_A_02060_DAY_T
#	"
#	exec_sql $sql_buff
	
  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02060_DAY a where exists
								(
								select * from 
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,expire_date,order_date,count(*)
									from bass1.G_A_02060_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,expire_date,order_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.expire_date = b.expire_date
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
