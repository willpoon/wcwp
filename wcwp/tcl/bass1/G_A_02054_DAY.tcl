######################################################################################################
#�ӿ����ƣ����ſͻ�ҵ�񶩹���ϵ
#�ӿڱ��룺02054
#�ӿ�˵����
#1�����ӿ�Ϊ�������ӿڣ��״��ϱ�����״̬Ϊ������ȫ��������ϵ��
#2��ԭ���ϣ�û�м��Ŷ�����ϵ��ҵ�����ڱ��ӿ��ϱ�����Ŀǰ��ũ��ͨ��УѶͨ�����ס�ũ�ű��������׵ȣ�
#   �����ʡ��ʱ���ٸ�ҵ���ʵ�ʷ�չ�������ʵ�����жϺ��ϱ���
#3����ҵ��վ����ҵ���䡢ר��ҵ�񡢶���100���ѵ������ӿ��ϱ�������ϵ��ҵ��
#   �ڴ˽ӿڲ����ϱ��������ڿ��ܴ���ڡ�����100���е���ҵ������ֱ���뼯��ǩ������Э�飬�����ڴ˽ӿ��ϱ���
#��������: G_A_02054_DAY.tcl
#��������: ����02054������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-06-25  
#�����¼��1.
#�޸���ʷ: 1. 20100325 liuqf ����V�����Ŷ����嵥 20100415 1.6.5�淶�޸ļ��붯��100�ľ����Ӳ�Ʒ �����ڴ���ڡ�����100���е���ҵ�񣬳����ڡ�����100ҵ�񶩹�������ӿ����ϱ��⣨02058����Ҳ���ڴ˽ӿ��ϱ�
#             20110215 liuqf �޸���ݮ�Ĳ�������
#							20110317 panzw �޸�1220�ֻ������������
#							20110630 panzw �޸�R216��������1520 �ƶ�400����
#							20110805 panzw �޳����Լ���
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	
  #���� yyyy-mm-dd
  set optime $op_time

  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02054_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

  #��Ҫ�޳���ҵ��----------------------
	#1310	УѶͨ
	#1320	ũ��ͨ
	#1430	����
	#1440	ũ�ű�
	#1450	������
	#1340	��ҵ��վ
	#1210	��ҵ����
	#1170	������ר��
	#3000	����100

  #��飬�ִ���ҵ���ϵ��-----------
  #1030 ����PBX
  #1140 ���Ų���
  #1170 ������ר��==
  #1210 ��ҵ����==
  #1220 �ֻ�����             
  #1240 M2M����ҵӦ�ÿ���
  #1250 �ƶ�OA           
  #1260 �ƶ�������       
  #1280 �ƶ�CRM
  #1290 ����ͨ 
  #1310 УѶͨ==
  #1320 ũ��ͨ==
  #1330 ����ͨ
  #1340 ��ҵ��վ==
  #1360 ����ͨ
  
#2011-06-30 11:30:44  
##	set sql_buff "insert into BASS1.G_A_02054_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when C.BASS1_VALUE='1290' then '2'
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
##				case
##					when a.REC_STATUS=1 then '1'
##					when a.REC_STATUS=0 then '2'
##		        else '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_SUB_DS a,
##				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
##				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
##			where DATE(A.done_date)='$op_time'
##			  and A.SERVICE_ID = B.SERVICE_ID
##			  and a.SERVICE_ID = c.XZBAS_VALUE
##				and c.BASS1_VALUE not in ('1310','1320','1430','1440','1450','1340','1210','1170','3000','1230','1220')
##		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
##		 		case
##					when C.BASS1_VALUE='1290' then '2'
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end,
##				case
##					when REC_STATUS=1 then '1'
##					when REC_STATUS=0 then '2'
##				else '2'
##				end "
##
#2011-06-30 11:30:50
#����1520�Ĺ���ģʽΪ3
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when C.BASS1_VALUE='1290' then '2'
					when C.BASS1_VALUE='1520' then '3'					
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
		        else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a,
				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.done_date)='$op_time'
			  and A.SERVICE_ID = B.SERVICE_ID
			  and a.SERVICE_ID = c.XZBAS_VALUE
				and c.BASS1_VALUE not in ('1310','1320','1430','1440','1450','1340','1210','1170','3000','1230','1220')
		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
		 		case
					when C.BASS1_VALUE='1290' then '2'
					when C.BASS1_VALUE='1520' then '3'						
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end "


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


###2011-03-17 12:39:15 1220	�ֻ������޸�
###�˴�����һ��,ȫ���޸���ʷ���ݣ��ڶ������δ���
##  set handle [aidb_open $conn]
##	set sql_buff "insert into BASS1.G_A_02054_DAY 
##		select * from bass1.G_A_02054_DAY_0317_1220repair
##	"
##							 
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##  aidb_close $handle
	
#1230 BLACK BERRY  �������д����޳���������
  set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when C.BASS1_VALUE='1290' then '2'
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
		        else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a,
				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.done_date)='$op_time'
			  and A.SERVICE_ID = B.SERVICE_ID
			  and a.SERVICE_ID = c.XZBAS_VALUE
				and c.BASS1_VALUE ='1230'
				and a.enterprise_id not in ('89100000000683','89103000041929','89103000161144','89103000498290')
		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
		 		case
					when C.BASS1_VALUE='1290' then '2'
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end "


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



####1230 BLACK BERRY  �޸��͸����ŵĲ�������,20110215�������ݺ�������δ���
###  set handle [aidb_open $conn]
###	set sql_buff "insert into BASS1.G_A_02054_DAY select * from G_A_02054_DAY_black"
###
###
###  puts $sql_buff
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2006
###		puts $errmsg
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###



###��ʱ����20101011 liuqf ȫ������ICT�ͻ���������ݣ�ֻץ���ж�����ϵ��Ч�ģ�����ʱ�䲻��
###
###  set handle [aidb_open $conn]
###	set sql_buff "insert into BASS1.G_A_02054_DAY
###			(
###				TIME_ID
###				,ENTERPRISE_ID
###				,ENTERPRISE_BUSI_TYPE
###				,MANAGE_MODE
###				,ORDER_DATE
###				,STATUS_ID
###			)
###			select distinct
###				${timestamp},
###				A.ENTERPRISE_ID,
###				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
###				case
###					when C.BASS1_VALUE='1290' then '2'
###					when upper(B.CONFIG_VALUE)='MAS' then '1'
###					when upper(B.CONFIG_VALUE)='ADC' then '2'
###					else '3'
###				end as MANAGE_MODE,
###				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
###				case
###					when a.REC_STATUS=1 then '1'
###					when a.REC_STATUS=0 then '2'
###		        else '2'
###				end as STATUS_ID
###			from
###				bass2.DW_ENTERPRISE_SUB_DS a,
###				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
###				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
###			where A.SERVICE_ID = B.SERVICE_ID
###			  and a.SERVICE_ID = c.XZBAS_VALUE
###				and c.BASS1_VALUE in ('1550')
###		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
###		 		case
###					when C.BASS1_VALUE='1290' then '2'
###					when upper(B.CONFIG_VALUE)='MAS' then '1'
###					when upper(B.CONFIG_VALUE)='ADC' then '2'
###					else '3'
###				end,
###				case
###					when REC_STATUS=1 then '1'
###					when REC_STATUS=0 then '2'
###				else '2'
###				end "
###
###
###  puts $sql_buff
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2006
###		puts $errmsg
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###
###
###
###    ###1000	VPMN ��
###    ###��������Σ�����V�������嵥����һ��ȫ��Ϊ״̬�����ģ��ڶ����ϴ���������
    set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct 
				${timestamp},
				a.enterprise_id, 
				'1000' as enterprise_busi_type,
				'3'    as manage_mode,
				replace(char(date(min(b.create_date))),'-','') as order_date,
				case 
				  when b.sts=0 then '1'
				  else '2'
				end as status_id
			from bass2.dwd_enterprise_vpmn_rela_$timestamp a,
			     bass2.dwd_vpmn_enterprise_$timestamp b
			where a.vpmn_id=b.vpmn_id
			  and DATE(b.CREATE_DATE)='$op_time'
			group by a.enterprise_id,
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



###��ʱ����20101022 liuqf ȫ��������������绰�ͻ���������ݣ�ֻץ���ж�����ϵ��Ч�ģ�����ʱ�䲻��

  set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				cust_party_role_id as ENTERPRISE_ID,
				'1040' as ENTERPRISE_BUSI_TYPE,
				'3' as MANAGE_MODE,
				replace(char(date(done_date)),'-','') as ORDER_DATE,
				case
					when STATE in ('1','4','6','8','M') then '1'
		      else '2'
				end as STATUS_ID
			from bass2.ODS_PRODUCT_INS_PROD_${timestamp}
		 where offer_id = 111389150019 and user_type_id = 2
       and cust_party_role_id not in ('89103000498290')
       and DATE(done_date)='$op_time'
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
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
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


	

  #��֤����Ψһ�ԣ�ɾ��ͨһ�충�����˶�������
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.g_a_02054_day a where exists 
								 (
								select * from 
									(
									select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date ,count(*)
									from bass1.g_a_02054_day
									where time_id = $timestamp
									group by time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
									where a.enterprise_id = b.enterprise_id
							      and a.order_date = b.order_date
							      and a.time_id = b.time_id
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
	
#2011-05-31 18:09:04 һ�����޸� for R186		
#set sql_buff "
#insert into G_A_02054_DAY select * from   G_A_02054_DAY_T
#"
#exec_sql $sql_buff


#2011-06-30 11:28:55
#һ�����޸�
##�ƶ�400
#
#       set sql_buff "
#       insert into G_A_02054_DAY select * from G_A_02054_DAY_FIX1520
#       "
#       exec_sql $sql_buff
#
#
# 2011-08-12 
#�޳� 1520 ���Լ���
#
#     set sql_buff "
#     insert into G_A_02054_DAY select * from G_A_02054_DAY_FIX20110812
#     "
#     exec_sql $sql_buff
#

#2011-10-20 ����ͨ�����޸�
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Deal_fixqixintong $op_time $optime_month


  #1.���chkpkunique
set tabname "g_a_02054_day"
set pk "ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE"
chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}
