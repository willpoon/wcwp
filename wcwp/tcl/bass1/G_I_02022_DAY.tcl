######################################################################################################
#�ӿڵ�Ԫ���ƣ��û�ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ�
#�ӿڵ�Ԫ���룺02022
#�ӿڵ�Ԫ˵�������ϱ�ȫ��ͨ�û�ȫ��ͳһ�ʷ��ײ͵Ķ�����¼��ÿ�û����һ����¼��
#1.	�û�����������
#2.	ÿ���û�����ֻ��ѡ��һ������ײ͡�
#3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ������ײͽ��мƷѵ���ʼ���ڡ���Ч����Ӧ�����ڻ���ڵ��ա�
#��������: G_I_02022_DAY.tcl
#��������: ����02022������
#��������: ��
#Դ    ��
#1.bass2.ODS_PRODUCT_INS_PROD_$timestamp
#2.BASS1.ALL_DIM_LKP 
#3.bass2.dwd_product_test_phone_$timestamp
#4.bass2.dw_product_$timestamp
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzhiwei
#��дʱ�䣺2011-04-26
#�����¼��1.
#�޸���ʷ: 1. 1.7.2 �淶
# ���ҵ��5��1�ղ����ߣ�5.1֮ǰû���������ߺ����5.2���������
#	2011-05-18 11:51:24 panzhiwei 1.���룺					      and XZBAS_COLNAME not like '�ײͼ���%' ������
#	2011-05-18 11:51:24 panzhiwei 2.���ã�					     Dw_product_ins_off_ins_prod_ds ������
#	2011-05-18 19:45:56 panzhiwei ��ע�������������û�
#	2011-05-27 10:10:49 panzhiwei ����     and date(a.expire_date) > '$op_time'��rownumber()�������޳���Ч����
#	2011-12-31 ����ͳһ�ʷѹ������
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp

		set app_name "G_I_02022_DAY.tcl"        

  #ɾ����������
	set sql_buff "delete from bass1.g_i_02022_day where time_id=$timestamp"
	exec_sql $sql_buff


  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
	set sql_buff "
	insert into bass1.g_i_02022_day
		  (
			 TIME_ID
			,USER_ID
			,BASE_PKG_ID
			,VALID_DT
		  )
select 	TIME_ID,USER_ID,BASE_PKG_ID,VALID_DT
from (
	select 
		$timestamp TIME_ID
		,char(a.product_instance_id)  USER_ID
		,e.NEW_PKG_ID BASE_PKG_ID
		,replace(char(date(a.VALID_DATE)),'-','') VALID_DT
		,row_number()over(partition by a.product_instance_id order by expire_date desc ,VALID_DATE desc ) rn 
	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
					      and XZBAS_COLNAME not like '�ײͼ���%'
				      ) c
	,(select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1) d
	,bass1.DIM_QW_QQT_PKGID e
	where  char(a.offer_id) = c.offer_id 
	  and char(a.product_instance_id)  = d.user_id
	  and bass1.fn_get_all_dim_ex('BASS_STD1_0114',char(a.offer_id))  = e.OLD_PKG_ID	  
	  and a.state =1
	  and a.valid_type in (1,2)
	  and a.OP_TIME = '$op_time'	  
	  and date(a.VALID_DATE)<='$op_time'	
    and date(a.expire_date) >= '$op_time'
	  and not exists (	select 1 from bass2.dwd_product_test_phone_$timestamp b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
			 ) 
) t where t.rn = 1
	 with ur
  "
	exec_sql $sql_buff


  #���н�����ݼ��
  #���chkpkunique
  set tabname "g_i_02022_day"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}
  
  #��������û��Ƿ����û�����ͷ
	set sql_buff "select count(*) from 
	            (
		     select user_id from bass1.g_i_02022_day
		      where time_id =$timestamp
		       except
		  select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
	            ) as a
	            "
  set DEC_RESULT_VAL1 [get_single $sql_buff]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02022<�û�ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ�>���û������û�����ͷ"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }

	return 0
}

