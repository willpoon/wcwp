
######################################################################################################		
#�ӿ�����: �ص�У԰����ѧ���û�                                                               
#�ӿڱ��룺02034                                                                                          
#�ӿ�˵����"�ص�У԰����ѧ���û���ָ���ա��й��ƶ�ʡ����Ӫ����ϵͳУ԰�г�����Ӧ��ҵ����������v1.5.0������У԰�������û��Ļ���֮�ϣ�����ѧ���û����Ը�������ɸѡģ�ͺͻ����罻��������ɢ��ѧ���û�ʶ��ģ��ʶ�������ѧ���û�Ⱥ�塣"
#��������: G_I_02034_MONTH.tcl                                                                            
#��������: ����02034������
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
  set app_name "G_I_02034_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02034_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02034_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,MARK_TYPE
        ,IFSEED
  )
select distinct 
	 $op_month
        ,a.USER_ID
        ,b.school_id
        ,'1' MARK_TYPE
	,'0' IFSEED
from    bass2.DW_XYSC_SCHOOL_REAL_USER_DT_$op_month a
left join (select distinct  school_id,SCHOOL_NAME 
		from  bass2.Dim_xysc_maintenance_info ) b on ( case when a.SCHOOL_NAME like '%���ش�ѧ%' then '���ش�ѧ' else a.SCHOOL_NAME end ) = b.SCHOOL_NAME
where 	b.school_id = '89189100000003'
and a.school_name not in (select distinct school_name from bass2.Dim_xysc_maintenance_info where SCHOOL_NAME <> '���ش�ѧ')
with ur
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_02034_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}