
######################################################################################################		
#�ӿ�����: �ص�У԰��������ѧ���û�                                                               
#�ӿڱ��룺02035                                                                                          
#�ӿ�˵����"�ص�У԰��������ѧ���û��Ǹ������±���ѧ���û��뱾�ؾ��������û�����ҵ��Ͷ���ҵ����Ȧ������Ϣ�������γɾ��������ڸ���У԰�����뱾��ͨ��������ͨ��ʱ�������Ŵ����;������������б����û�ͨ��������ͨ��ʱ�������Ŵ��������������ֹ�������֮ͨ��������ͨ��ʱ�������Ŵ������ΪУ԰��Ϊ�������ֵĹ���У԰��"
#��������: G_I_02035_MONTH.tcl                                                                            
#��������: ����02035������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110807
#�����¼��
#�޸���ʷ: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]



  #������
  global app_name
  set app_name "G_I_02035_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02035_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02035_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,CMCC_BRANCH_ID
  )
select 
         $op_month TIME_ID
	,a.COMP_NO USER_ID
        ,b.SCHOOL_ID 
        ,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end COMP_BRAND_ID
from bass2.dw_xysc_school_comp_user_dt_$op_month a
left join bass2.Dim_xysc_maintenance_info b  on ( case when a.SCHOOL_NAME like '%���ش�ѧ%' then '���ش�ѧ' else a.SCHOOL_NAME end ) = b.SCHOOL_NAME
join bass2.dw_comp_cust_$op_month d on a.comp_no = d.COMP_PRODUCT_NO
where  d.COMP_BRAND_ID  in (1,2,9,10,11,3,4,5,7)
and b.school_id = '89189100000003'
and a.school_name not in (select distinct school_name from bass2.Dim_xysc_maintenance_info 
	where SCHOOL_NAME <> '���ش�ѧ'
	)
group by 
	a.COMP_NO
	,b.SCHOOL_ID 
	,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end 
with ur
"
  exec_sql $sql_buff
#	and SCHOOL_NAME <> '���ش�ѧũ��ѧԺ'

  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_02035_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}
