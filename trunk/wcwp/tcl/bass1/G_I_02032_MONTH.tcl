
######################################################################################################		
#�ӿ�����: У԰����ѧ���û�                                                               
#�ӿڱ��룺02032                                                                                          
#�ӿ�˵����"���ݼ��Ź�˾�Ƽ���ѧ���û�ʶ��ģ�ͻ�ʡ���е�ѧ���û�ʶ�𷽰���ʶ�����ѧ���û���ϸ��Ҫ����ʵ��ӳ��ʡ����ʵ�������"
#��������: G_I_02032_MONTH.tcl                                                                            
#��������: ����02032������
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
  set app_name "G_I_02032_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02032_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02032_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,MARK_TYPE
  )
select distinct 
				$op_month
        ,a.USER_ID
        ,b.school_id
        ,'1' MARK_TYPE        
from    bass2.DW_XYSC_SCHOOL_REAL_USER_DT_$op_month a
 join (select distinct  school_id,SCHOOL_NAME 
		from  bass2.Dim_xysc_maintenance_info ) b on a.SCHOOL_NAME = b.SCHOOL_NAME
where  upper(a.PHONE_TYPE) = 'S'		
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_02032_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}
