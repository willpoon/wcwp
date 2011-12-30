
######################################################################################################		
#�ӿ�����: У԰��������ѧ���û�                                                               
#�ӿڱ��룺02033                                                                                          
#�ӿ�˵����"���ݼ��Ź�˾�Ƽ��ľ�������ѧ���û�ʶ��ģ�ͻ�ʡ���еľ�������ѧ���û�ʶ�𷽰���ʶ����ľ�������ѧ���û���ϸ��Ҫ����ʵ��ӳ��ʡ����ʵ�������"
#��������: G_I_02033_MONTH.tcl                                                                            
#��������: ����02033������
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
  set app_name "G_I_02033_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02033_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02033_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,CMCC_BRANCH_ID
        ,MARK_TYPE
  )
select 
         $op_month TIME_ID
	,a.COMP_NO USER_ID
        ,b.SCHOOL_ID 
        ,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end COMP_BRAND_ID
        ,'1' MARK_TYPE
from bass2.dw_xysc_school_comp_user_dt_$op_month a
left join bass2.Dim_xysc_maintenance_info b on a.SCHOOL_NAME = b.SCHOOL_NAME
join bass2.dw_comp_cust_$op_month d on a.comp_no = d.COMP_PRODUCT_NO
where  d.COMP_BRAND_ID  in (1,2,9,10,11,3,4,5,7)
and a.PHONE_TYPE = 'S'
group by 
a.COMP_NO
,b.SCHOOL_ID 
,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end 
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_02033_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}
