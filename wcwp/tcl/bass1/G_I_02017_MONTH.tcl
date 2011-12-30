######################################################################################################
#�ӿ����ƣ��й��ƶ�У԰����ͻ�
#�ӿڱ��룺02017
#�ӿ�˵����
#��������: G_I_02017_MONTH.tcl
#��������:  
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-7-26
#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
#�޸���ʷ:
#2011-06-20 16:04:23 panzhiwei ���ݶ���������У԰�г�ר����ȡ.
#xyscv1.3  ��ɾ�� �ýӿ�
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #������
  global app_name
  set app_name "G_I_02017_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_02017_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02017_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,STUD_MARK
        ,MARK_TYPE
  )
select distinct 
				$op_month
        ,a.USER_ID
        ,b.school_id
        ,case when upper(a.PHONE_TYPE) = 'S' then '1' else '2' end STUD_MARK    
        ,'1' MARK_TYPE        
from    bass2.DW_XYSC_SCHOOL_REAL_USER_DT_$op_month a
join ( select distinct  school_id,SCHOOL_NAME 
	     from  bass2.Dim_xysc_maintenance_info ) b on a.SCHOOL_NAME = b.SCHOOL_NAME
with ur  
"
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_02017_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}