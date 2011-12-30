
######################################################################################################		
#�ӿ�����: У԰��Ϣ���������                                                               
#�ӿڱ��룺06003                                                                                          
#�ӿ�˵����"��Ҫ�����˽�ȫ����У԰��Ϣ��ҵ��ĸ��������Ϊ������Ϣ���Ŀ�ѧ�������ͳ������ռ���Ϣ"
#��������: G_I_06003_MONTH.tcl                                                                            
#��������: ����06003������
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
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #������
  global app_name
  set app_name "G_I_06003_MONTH.tcl"
  
##  #ɾ����������
	set sql_buff "
	delete from bass1.G_I_06003_MONTH where time_id=$op_month
	"
  exec_sql $sql_buff 
  
  set sql_buff "
  insert into G_I_06003_MONTH
  (
	 TIME_ID
        ,SCHOOL_ID
        ,ENTERPRISE_ID
        ,IF_CONTRACT
        ,IF_CHNL_COVER
        ,IF_LINE
        ,IF_WLAN
        ,IF_VPMN
        ,IF_MAS
        ,IF_GRP_SMMS
        ,IF_GRP_MAIL
        ,IF_CARDTONG
        ,IF_OTHCOVER
  )
  select 
  distinct 
 $op_month  TIME_ID
 ,a.SCHOOL_ID
 ,min(a.ENTERPRISE_ID ) ENTERPRISE_ID
 ,case  when a.ENTERPRISE_ID  like '8%' then '1' else '0' end IF_CONTRACT
 ,'0' IF_CHNL_COVER
 ,'0' IF_LINE
 ,'0' IF_WLAN
 ,'1' IF_VPMN
 ,'0' IF_MAS
 ,'0' IF_GRP_SMMS
 ,'0' IF_GRP_MAIL
 ,'0' IF_CARDTONG
 ,'1' IF_OTHCOVER
from bass2.Dim_xysc_maintenance_info a
where a.ENTERPRISE_ID  like '8%'
and a.SCHOOL_ID  in (
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
)
group by  
a.SCHOOL_ID
,case  when a.ENTERPRISE_ID  like '8%' then '1' else '0' end
with ur
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_06003_MONTH"
  set pk   "SCHOOL_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
  
	return 0
}

