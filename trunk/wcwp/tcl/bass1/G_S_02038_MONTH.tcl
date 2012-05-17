
######################################################################################################		
#�ӿ�����: �������û�                                                               
#�ӿڱ��룺02038                                                                                          
#�ӿ�˵�����ϱ������ж�Ϊ�������û����û���Ϣ
#��������: G_S_02038_MONTH.tcl                                                                            
#��������: ����02038������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120507
#�����¼��
#�޸���ʷ: 1. panzw 20120507	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.0) 
##~   �������ͻ�  	��������ָ�ͻ��Ѿ�ӵ���й��ƶ�ĳ�����зֹ�˾һ���ƶ����룬����ĳ��ԭ�����������й��ƶ��õ��зֹ�˾����һ���ƶ������������º�����ȫ�����߲������ԭ�к��룬�����������Ϊ�������������Ŀͻ���Ϊ�������ͻ������У��������ͻ�������һ��˫�ſͻ�
##~   ����������ͻ� 	�����ڷ�����������(������)�������Ŀͻ������������ͻ��Ӽ�


#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      
      set last_month [GetLastMonth [string range $op_month 0 5]]



  #ɾ����������
	set sql_buff "delete from bass1.G_S_02038_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "

insert into G_S_02038_MONTH(
         TIME_ID
        ,USER_ID
        ,MSISDN
        ,OLD_USER_ID
        ,OLD_MSISDN
)
select 
         $op_month TIME_ID
        ,a.RN_USER_ID
        ,a.RN_USER_NUMBER
        ,a.HIS_USER_ID
        ,a.HIS_USER_NUMBER
from 	bass2.dmrn_user_ms  a , bass2.dw_product_$op_month b
where substr(replace(char(RN_DATE),'-',''),1,6) =  '$last_month'
and a.RN_USER_ID = b.user_id 
and b.usertype_id in (1,2,9) 
and b.userstatus_id in (1,2,3,6,8)
with ur
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_02038_MONTH 3


  #1.���chkpkunique
	set tabname "G_S_02038_MONTH"
	set pk 			"USER_ID"
	chkpkunique ${tabname} ${pk} ${op_month}

	return 0
}

