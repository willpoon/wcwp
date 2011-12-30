######################################################################################################
#�ӿ����ƣ�M2M�����û��󶨹�ϵ
#�ӿڱ��룺02062
#�ӿ�˵����
#��������: G_A_02062_DAY.tcl
#��������: ����02062������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhaolp2
#��дʱ�䣺2009-07-06
#�����¼��1.
#�޸���ʷ: 1. 20101027 liuzhilong 1240 M2M����ҵӦ�ÿ��� �ĳ� 1241M2M������ͨ���� 1249 M2M��������
#�޸���ʷ: 2. 20110317 panzhiwei ��ȡ�������޳������û� (and e.TEST_MARK = 0)
#�޸���ʷ: 3. 20110317 panzhiwei ���û���ʶ���ڴ��г��Ĳ����û��������û����˶����� 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02062_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#1240	M2M����ҵӦ�ÿ���



	#�ʷ�����δȷ�Ͽھ���������
	set sql_buff "insert into BASS1.G_A_02062_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,PRODUCT_NO
				,INDUSTRY_ID
				,GPRS_TYPE
				,DATA_SOURCE
				,CREATE_DATE
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
				D.PRODUCT_NO,
				case
					when A.SERVICE_ID='949' then '1'
					when A.SERVICE_ID in ('942','946') then '2'
					when A.SERVICE_ID='944' then '4'
					when A.SERVICE_ID='947' then '8'
					else '10'
				end  as INDUSTRY_ID,
				'1' as GPRS_TYPE,
				'cmm.hn' as DATA_SOURCE,
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
        inner join bass2.dw_enterprise_membersub_ds d on A.ORDER_ID=D.ORDER_ID
        inner join bass2.dw_product_$timestamp e on d.product_no=e.product_no
			where DATE(A.done_date)='${op_time}'
			  and c.bass1_value in ('1241','1249')
			  and e.usertype_id in (1,2,9)
			  and e.TEST_MARK = 0
			  and a.enterprise_id not in ('891880005002','891910006274')"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

# 20111013 �޸�����ͨ����




###2011-03-17 17:29:01 M2M�����û��󶨹�ϵ
###�˴�����һ��,�ڶ������δ���
##  set handle [aidb_open $conn]
##		set sql_buff "insert into BASS1.G_A_02062_DAY 
##			select * from bass1.G_A_02062_DAY_20110317repair
##		"
##							 
##	  puts $sql_buff
##		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##			WriteTrace $errmsg 2006
##			puts $errmsg
##			aidb_close $handle
##			return -1
##		}
##		aidb_commit $conn
##	 aidb_close $handle
  #��֤����Ψһ�ԣ�ɾ��ͬһ�충�����˶�������
	set sql_buff "delete  from bass1.G_A_02062_DAY a where exists
								(
								select * from 
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,product_no,industry_id,create_date,count(*)
									from bass1.G_A_02062_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,product_no,industry_id,create_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.product_no = b.product_no
								  and a.industry_id = b.industry_id
								  and a.create_date = b.create_date
								  and a.time_id=$timestamp
								)"
	exec_sql $sql_buff

# 2011-10-21 
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Deal_fixchewutong $op_time $optime_month



set sql_buff "ALTER TABLE G_A_02062_DAY_STAGE ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
set sql_buff "
insert into G_A_02062_DAY_STAGE
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
       from  (select i.* , row_number()over(partition by USER_ID order by TIME_ID desc ) rn 
		from G_A_02062_DAY i 
		)  t where t.rn = 1 and t.STATUS_ID = '1'
"
	exec_sql $sql_buff

 aidb_runstats bass1.G_A_02062_DAY_STAGE  3

	return 0
}
