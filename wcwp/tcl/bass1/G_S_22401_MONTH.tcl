
######################################################################################################		
#�ӿ�����: �û�����GPRSʹ��λ����Ϣ�»���                                                               
#�ӿڱ��룺22401                                                                                          
#�ӿ�˵����"���ڱ���GPRSҵ���굥�������������û�������ͨ������վС������ҵ�������ܡ�ע����������У԰��վ��"
#��������: G_S_22401_MONTH.tcl                                                                            
#��������: ����22401������
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
  set app_name "G_S_22401_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22401_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_S_22401_MONTH
  (
         TIME_ID
        ,USER_ID
        ,CELL_ID
        ,LAC_ID
        ,CALL_CNT
  )
select  
	$op_month TIME_ID
        ,a.USER_ID
	,value(a.CELL_ID,'0')
	,value(a.LAC_ID,'0')
        ,char(sum(CALL_CNT)) CALL_CNT
from   bass1.int_22401_$op_month a
where a.TIME_ID/100 = $op_month
	and a.user_id is not null 
group by 
	USER_ID
	,value(a.CELL_ID,'0')
	,value(a.LAC_ID,'0')
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22401_MONTH"
  set pk   "USER_ID||CELL_ID||LAC_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}