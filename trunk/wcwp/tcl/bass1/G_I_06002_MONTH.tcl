######################################################################################################
#�ӿ����ƣ�У԰λ����Ϣ
#�ӿڱ��룺06002
#�ӿ�˵����
#��������: G_I_06002_MONTH.tcl
#��������:  
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-7-26
#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
#�޸���ʷ:���ݶ���������У԰�г�ר����ȡ.
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #������
  global app_name
  set app_name "G_I_06002_MONTH.tcl"
  
##  #ɾ����������
	set sql_buff "
	delete from bass1.G_I_06002_MONTH where time_id=$op_month
	"
  exec_sql $sql_buff 
  
  set sql_buff "
  insert into G_I_06002_MONTH
  (
         TIME_ID
        ,SCHOOL_ID
        ,CELL_ID
        ,LAC_ID  
  )
  select distinct 
	$op_month
        ,SCHOOL_ID
        ,value(CELL_ID,'0') CELL_ID
        ,value(LAC_ID,'0') LAC_ID        
	from bass2.Dim_xysc_maintenance_info a
	where  a.SCHOOL_ID  in (
	select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
	)
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_06002_MONTH"
  set pk   "SCHOOL_ID||CELL_ID||LAC_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
  
	return 0
}

