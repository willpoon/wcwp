######################################################################################################
#�ӿڵ�Ԫ���ƣ��û�ѡ��ȫ��ͨר�������ʷ��ײ�
#�ӿڵ�Ԫ���룺02023
#�ӿڵ�Ԫ˵�������ϱ��û���ȫ��ͨר�����ݰ��Ķ�����¼��
#1.	ȫ��ͨ�ײͿ�ѡ�����ͻ�����ѡ��ֻ��ѡ��ȫ��ͨȫ��ͳһ�ʷ������ײ͡������ײ͡������ײ͵Ŀͻ�����ѡ��ר�����ݰ��������ѡ����
#2.	�û�����������
#3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ��ײͽ��мƷѵ���ʼ���ڡ���Ч����Ӧ�����ڻ���ڵ��졣
#��������: G_I_02023_DAY.tcl
#��������: ����02023������
#��������: ��
#Դ    ��
#1. bass2.Dw_product_ins_off_ins_prod_ds 
#2. BASS1.ALL_DIM_LKP 
#3. bass2.dw_product_$timestamp
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzhiwei
#��дʱ�䣺2011-04-26
#�����¼��1.
#�޸���ʷ: 1. 1.7.2 �淶
#�޸���ʷ: 2. ���ҵ��5��1�ղ����ߣ�5.1֮ǰû���������ߺ����5.2���������
#2011-05-18 11:51:24 panzhiwei ԭ����fn_get_all_dim��bug�����ܷ��ش���10���ֶ�ֵ����ʹ��fn_get_all_dim_ex()��������
#2011-05-18 11:51:24 panzhiwei ����02022У��user_id�ĺϷ��ԡ�
#2011-12-31 ����ͳһ�ʷѹ������
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
		global app_name
		set app_name "G_I_02023_DAY.tcl"        


  #ɾ����������
	set sql_buff "delete from bass1.g_i_02023_day where time_id=$timestamp"
	exec_sql $sql_buff


	#1.�󶩹��û�
	#һ���Խ���
	##drop table BASS1.G_I_02023_DAY_1;
	##CREATE TABLE BASS1.G_I_02023_DAY_1
	## (
	##		 USER_ID            	CHAR(20)            ----�û���ʶ ����   
	##		,ADD_PKG_ID         	CHAR(30)            ----�����ײͱ�ʶ ����
	##		,VALID_DT           	CHAR(8)             ----�ײ���Ч����      
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (USER_ID
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_I_02023_DAY_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	
	
	
	set sql_buff "ALTER TABLE BASS1.G_I_02023_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_I_02023_DAY_1
			( 
				 USER_ID
				,ADD_PKG_ID
				,VALID_DT
			)
			select 
			distinct 
				USER_ID
				,ADD_PKG_ID
				,VALID_DT
			FROM (
				SELECT
				 a.PRODUCT_INSTANCE_ID as USER_ID
				,bass1.fn_get_all_dim_ex('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
				,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
				,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
					order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
				from  bass2.Dw_product_ins_off_ins_prod_ds a 
				     ,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) b 
				where char(a.offer_id) = b.offer_id 
				and a.state=1
				and a.valid_type = 1 
				 and a.OP_TIME = '$op_time'
				 and date(a.VALID_DATE)<='$op_time'
				 and date(a.expire_date) >= '$op_time'
			    ) AS T where t.rn = 1 
			 with ur 
	"
	exec_sql $sql_buff

	  aidb_runstats bass1.G_I_02023_DAY_1 3


	set sql_buff "
	insert into bass1.g_i_02023_day
		  (
			 TIME_ID
			,USER_ID
			,ADD_PKG_ID
			,VALID_DT
		  )
	select 
			$timestamp as TIME_ID
			,a.USER_ID
			,c.new_pkg_id ADD_PKG_ID
			,a.VALID_DT
		from  bass1.G_I_02023_DAY_1 as a 
		      ,bass2.dw_product_$timestamp as b
		      ,bass1.DIM_QW_QQT_PKGID  c
		    where a.user_id = b.user_id
		    and a.add_pkg_id = c.old_pkg_id
		    and b.usertype_id in (1,2,9) 
		    and b.userstatus_id in (1,2,3,6,8)
		    and b.test_mark<>1
	with ur 
  "
	exec_sql $sql_buff
	
##~   �޳�һ��ֻ�е����ײ�û�����ײ͵��û�����
	set sql_buff "
		delete from (
						select * from    bass1.g_i_02023_day where time_id=$timestamp
						and user_id = '89160000950064'
						) t
		with ur
"
	exec_sql $sql_buff

# ���02023 ��user_id �Ƿ��� 02022 ��:���DEC_RESULT_VAL1 = 0 - ���� ; DEC_RESULT_VAL1 > 0 - ������


	set sql_buff "
	select count(0) from (
				select user_id from    bass1.g_i_02023_day where time_id=$timestamp
				except
				select user_id from    bass1.g_i_02022_day ) a 
	with ur
  "
	chkzero $sql_buff 1

#3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ��ײͽ��мƷѵ���ʼ���ڡ���Ч����Ӧ�����ڻ���ڵ��졣
  #���н�����ݼ��
  #���chkpkunique
  set tabname "g_i_02023_day"
	set pk 			"USER_ID||ADD_PKG_ID"
	chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}

