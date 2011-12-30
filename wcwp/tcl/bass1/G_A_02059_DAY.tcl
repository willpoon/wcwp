######################################################################################################
#�ӿ����ƣ�����ҵ������û��󶨹�ϵ
#�ӿڱ��룺02059
#�ӿ�˵����
#��������: G_A_02059_DAY.tcl
#��������: ����02059������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25 
#�����¼��1.
#�޸���ʷ: 1.20100415 1.6.5�淶�޸ļ��붯��100�ľ����Ӳ�Ʒ �����ڴ���ڡ�����100���е���ҵ�񣬳����ڡ�����100ҵ�񶩹�������ӿ����ϱ��⣨02058����Ҳ���ڴ˽ӿ��ϱ�
#          20100626 ҵ���߼��ϱ�Ϊ bigint(e.porder_id)=bigint(a.order_id) ����
#          20100818 ����V����Ա�İ󶨹�ϵ��ֻ�ϴ�����������ʷȫ����û���ϴ�
#          20101027 liuzhilong 1240 M2M����ҵӦ�ÿ��� �ĳ� 1241M2M������ͨ���� 1249 M2M��������
#          20110315 panzhiwei 1040�������񶩹���ϵ�޸�
#          20110321 panzhiwei 1340	��ҵ��վ(ADC)���� ������ҵ��վҵ��ֻҪ��ҵ�ͻ�������ҵ��վ�󣬼��Ÿ��˿ͻ�Ĭ�Ͽ���ʹ�øù��ܣ�������˿ͻ������������÷��񣬹�����ҵ��վ�������ҵ������˿ͻ��󶨹�ϵ���˴��޸��ǰ����ж����ļ���������Ա�ӽ�ȥ��Ϊ���˶������Է�ֹ��������֤������ҵ��վ��ADC��ʹ�ü��ſͻ������� > ʹ����ҵ��վ��ADC���ļ��Ÿ��˿ͻ������쳣��֮�쳣��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #set timestamp 20090712
        #���� yyyy-mm-dd
        set optime $op_time
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02059_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#1220	�ֻ�����
	#1230	BLACK BERRY
	#1240	M2M����ҵӦ�ÿ���
	#1320   ũ��ͨ
	#2011-03-15 10:55:09 1040��������绰	��������
	#����ͨ/ũ��ͨ��Ҫ���⴦��
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				A.USER_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_EXTSUB_RELA_DS a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.CREATE_DATE)='$op_time'
				and A.SERVICE_ID=B.SERVICE_ID
				and A.SERVICE_ID=C.XZBAS_VALUE
				and C.BASS1_VALUE not in ('1230','1241','1249','1220','1320','1040')
		"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
#####����ҵ��վADC����һ��ȫ�������޸����޸������δ˴���
##	set sql_buff "insert into BASS1.G_A_02059_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,USER_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				A.USER_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
##				case
##					when REC_STATUS=1 then '1'
##					when REC_STATUS=0 then '2'
##				else '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_EXTSUB_RELA_DS a,
##				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
##				(select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
##			where A.REC_STATUS=1
##				and A.SERVICE_ID=B.SERVICE_ID
##				and A.SERVICE_ID=C.XZBAS_VALUE
##				and C.BASS1_VALUE in ('1340')
##		"
##
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn

##1040	��������绰����
##�˴�����һ��,ȫ���޸���ʷ���ݣ��ڶ������δ���
##2011-03-16 8:53:34 ���δ���
##	set sql_buff "insert into BASS1.G_A_02059_DAY select * from G_A_02059_DAY_0315modify
##							 "
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
	
#�������񶩹���ϵΪ��������������,��02049���ų�Ա�ӿڿھ�һ��
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      bass2.dw_enterprise_member_mid_${timestamp} b,
								      bass2.dw_enterprise_msg_${timestamp} c,
								      bass2.dw_product_${timestamp} d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1040'
								  and a.time_id=${timestamp}
								  and a.status_id='1'
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#�������񶩹���ϵΪ�˶�����������
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      (select distinct enterprise_id,user_id 
								        from BASS1.G_A_02059_DAY 
								       where enterprise_busi_type='1040' 
								         and time_id<${timestamp}) b
								where a.enterprise_id=b.enterprise_id
								  and a.time_id=${timestamp}
								  and a.enterprise_busi_type='1040'
								  and a.status_id='2'
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

##2011-03-21 12:19:50 panzhiwei �˴�����һ��,ȫ���޸���ʷ���ݣ��ڶ������δ���
##1340	��ҵ��վ(ADC)���� ������ҵ��վҵ��ֻҪ��ҵ�ͻ�������ҵ��վ�󣬼��Ÿ��˿ͻ�Ĭ�Ͽ���ʹ�øù��ܣ�������˿ͻ������������÷��񣬹�����ҵ��վ�������ҵ������˿ͻ��󶨹�ϵ��
##�˴��޸��ǰ����ж����ļ���������Ա�ӽ�ȥ��Ϊ���˶������Է�ֹ��
##������֤������ҵ��վ��ADC��ʹ�ü��ſͻ������� > ʹ����ҵ��վ��ADC���ļ��Ÿ��˿ͻ������쳣��
##֮�쳣��

#2011-03-22 8:38:06ע�͵�һ�����޸�����
##	set sql_buff "insert into BASS1.G_A_02059_DAY select * from G_A_02059_DAY_20110321fix1340
##							 "
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
	
#��ҵ��վ(ADC)������ϵΪ��������������,��02049���ų�Ա�ӿڿھ�һ��

	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1340'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02055_day a,
								      bass2.dw_enterprise_member_mid_${timestamp} b,
								      bass2.dw_enterprise_msg_${timestamp} c,
								      bass2.dw_product_${timestamp} d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1340'
								  and a.time_id=${timestamp}
								  and a.status_id='1'
								  and d.test_mark = 0								  
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#��ҵ��վ(ADC)������ϵΪ�˶�����������
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1340'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02055_day a,
								      (select distinct enterprise_id,user_id 
								        from BASS1.G_A_02059_DAY 
								       where enterprise_busi_type='1340' 
								         and time_id<${timestamp}) b
								where a.enterprise_id=b.enterprise_id
								  and a.time_id=${timestamp}
								  and a.enterprise_busi_type='1340'
								  and a.status_id='2'
					"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	
	#����ͨ���˶�������	
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				'' as ENTERPRISE_ID,
				A.USER_ID,
				'1290' as ENTERPRISE_BUSI_TYPE,
				'2' as MANAGE_MODE,
				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
				case
					when STS=1 then '1'
					else '2'
				end as STATUS_ID
			from
				BASS2.DW_PRODUCT_REGSP_DS A
			where DATE(A.STS_DATE)='$op_time'
				and sp_code in ('600000','901870','801160','900141')"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#ũ��ͨ���˶�������(��һ����ȫ�����ڶ�������)
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				'' as ENTERPRISE_ID,
				A.USER_ID,
				'1320' as ENTERPRISE_BUSI_TYPE,
				'2' as MANAGE_MODE,
				replace(char(date(min(A.CREATE_DATE))),'-','') as ORDER_DATE,
				case
					when STS=1 then '1'
					else '2'
				end as STATUS_ID
			from
				BASS2.DW_PRODUCT_REGSP_DS A
			where DATE(A.STS_DATE)='$op_time'
				and a.busi_type='133'
		   group by A.USER_ID,
		         case
					when STS=1 then '1'
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



  #���붯��100�ľ����Ӳ�Ʒ������ϵ
    set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				A.USER_ID,
				D.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
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
			  and a.enterprise_id not in ('891910006274','891880005002')
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


###    ###1000	VPMN 
###    20100818 V����Ա�İ󶨹�ϵ��������ץȡ������Ϊ1000

    set handle [aidb_open $conn]
		set sql_buff "insert into BASS1.G_A_02059_DAY
				(
					TIME_ID
					,ENTERPRISE_ID
					,USER_ID
					,ENTERPRISE_BUSI_TYPE
					,MANAGE_MODE
					,ORDER_DATE
					,STATUS_ID
				)
				select distinct 
					${timestamp},
			    a.enterprise_id, 
			    b.user_id,
			    '1000' as enterprise_busi_type,
			    '3'    as manage_mode,
			    replace(char(date(min(b.create_date))),'-','') as order_date,
			    case 
			      when b.sts=0 then '1'
			      else '2'
			    end as status_id
			from bass2.dwd_enterprise_vpmn_rela_$timestamp a,
			     bass2.dw_vpmn_member_ds b
			where a.vpmn_id=b.vpmn_id
			  and DATE(b.sts_date)='${op_time}'
			group by a.enterprise_id,b.user_id,
			case 
			  when b.sts=0 then '1'
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

#2011-05-31 15:58:36
#�޸�02059������usertype_id = 8 ���û���ʧЧ����ͨ��R177У�顣
#һ�����޸���
#set sql_buff "
#insert into G_A_02059_DAY select * from   G_A_02059_DAY_T
#"
#exec_sql $sql_buff


  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02059_DAY a where exists
								(
									select * from
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
									from bass1.G_A_02059_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
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