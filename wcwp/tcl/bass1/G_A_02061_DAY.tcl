######################################################################################################
#�ӿ����ƣ��ֻ���������û��󶨹�ϵ
#�ӿڱ��룺02061
#�ӿ�˵����
#��������: G_A_02061_DAY.tcl
#��������: ����02061������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-07-06
#�����¼��1.
#�޸���ʷ: 1.2011-03-17 15:50:30 �����ֻ�����ȫ�����ǲ������ݣ���ʵ�ʶ��������Ժ����ݲ��ϱ�,���ֹ������޸����ݣ�Ȼ�������ֻ���������û��󶨹�ϵ�ĳ�ȡ��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##        #���� yyyymmdd
##        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
##
##        #���� yyyy-mm-dd
##        set optime $op_time
##


##--##2011-03-17 15:55:08 �ֹ������޸�����
##--/**
##--		CREATE TABLE BASS1.G_A_02061_DAY_0317repair
##--		 (TIME_ID               INTEGER,
##--		  ENTERPRISE_ID         CHARACTER(20),
##--		  USER_ID               CHARACTER(20),
##--		  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
##--		  MANAGE_MODE           CHARACTER(1),
##--		  EXPIRE_DATE           CHARACTER(8),
##--		  PAY_TYPE              CHARACTER(1),
##--		  ORDER_DATE            CHARACTER(8),
##--		  STATUS_ID             CHARACTER(1)
##--		 )
##--		  DATA CAPTURE NONE
##--		 IN TBS_APP_BASS1
##--		 INDEX IN TBS_INDEX
##--		  PARTITIONING KEY
##--		   (USER_ID,
##--		    TIME_ID
##--		   ) USING HASHING;
##--		
##--			ALTER TABLE BASS1.G_A_02061_DAY_0317repair
##--			  LOCKSIZE ROW
##--			  APPEND OFF
##--			  NOT VOLATILE;
##--			delete from   bass1.G_A_02061_DAY_0317repair;
##--			insert into   bass1.G_A_02061_DAY_0317repair
##--			select 
##--			         20110317
##--			        ,ENTERPRISE_ID
##--			        ,USER_ID
##--			        ,ENTERPRISE_BUSI_TYPE
##--			        ,MANAGE_MODE
##--			        ,'20110317' EXPIRE_DATE
##--			        ,PAY_TYPE
##--			        ,ORDER_DATE
##--			        ,'2' STATUS_ID
##--			from 
##--			(
##--			select t.*,row_number()over(partition by user_id order by time_id ) rn 
##--			from 
##--			(
##--			select * from G_A_02061_DAY
##--			where ENTERPRISE_BUSI_TYPE = '1220'
##--			and  MANAGE_MODE = '2'
##--			and length(trim(user_id)) = 14
##--			) t
##--			) t2 
##--			where rn = 1 
##--			and STATUS_ID ='1'
##--			--
##--			--�ϴ��޸�����
##--			insert into BASS1.G_A_02061_DAY
##--			select * from bass1.G_A_02061_DAY_0317repair
##--			;
##--			
##--**/


##        #ɾ����������
##        set handle [aidb_open $conn]
##	set sql_buff "delete from BASS1.G_A_02061_DAY where TIME_ID=$timestamp"
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
##
##	#1220	�ֻ�����
##
##  #92602002	�ֻ����䣨���ţ�_3Ԫ/��
##  #92602003	�ֻ����䣨���ţ�_8Ԫ/��
##  #92602004	�ֻ����䣨WAP��_15Ԫ/��
##  #92602005	�ֻ����䣨�ͻ��ˣ�_28Ԫ/��
##  #
##
##	#�ʷ�����δȷ�Ͽھ���������
##	set sql_buff "insert into BASS1.G_A_02061_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,USER_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,EXPIRE_DATE
##				,PAY_TYPE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				D.USER_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(A.EXPIRE_DATE)),'-','') as EXPIRE_DATE,
##			  case when e.sprom_id=92602005 then '5'
##				else '5' end  as PAY_TYPE,
##				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
##				case
##					when a.REC_STATUS=1 then '1'
##					when a.REC_STATUS=0 then '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_SUB_DS a
##				inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
##				inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
##        left join bass2.DW_ENTERPRISE_MEMBERSUB_DS d on A.ORDER_ID=D.ORDER_ID
##        inner join bass2.dw_product_sprom_ds e on d.user_id =e.user_id
##			where DATE(A.done_date)='${op_time}'
##			  and C.BASS1_VALUE='1220'"
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
##	aidb_close $handle
##
##
##
##  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
##  set handle [aidb_open $conn]
##	set sql_buff "delete  from bass1.G_A_02061_DAY a where exists
##								(
##								select * from
##									(
##									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
##									from bass1.G_A_02061_DAY
##									where time_id = $timestamp
##									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
##									having count(*)>1
##									) as b
##								where a.time_id = b.time_id
##								  and a.enterprise_id = b.enterprise_id
##								  and a.user_id = b.user_id
##								  and a.enterprise_busi_type = b.enterprise_busi_type
##								  and a.manage_mode = b.manage_mode
##								  and a.order_date = b.order_date
##								  and a.time_id=$timestamp
##								 )"
##	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle		
##
##
	return 0
}