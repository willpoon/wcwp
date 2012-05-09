
######################################################################################################		
#�ӿ�����: ���������û�                                                               
#�ӿڱ��룺02039                                                                                          
#�ӿ�˵������¼�������������û���Ϣ����ӿ�02039���������������������и��������������û���ƥ�䡣��һ���û����ڶ����������ԭ���������Ƴ̶���ߵ�˳������ϱ���
#��������: G_S_02039_MONTH.tcl                                                                            
#��������: ����02039������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120507
#�����¼��
#�޸���ʷ: 1. panzw 20120507	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.0) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      



  #ɾ����������
	set sql_buff "delete from bass1.G_S_02039_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "

insert into G_S_02039_MONTH(
         TIME_ID
        ,OP_TIME
        ,CHANNEL_ID
        ,USER_ID
        ,MSISDN
        ,YK_REASON
)
select 
         $op_month TIME_ID
        ,'$op_month' OP_TIME
        ,char(b.CHANNEL_ID) CHANNEL_ID
        ,a.USER_ID
        ,c.product_no MSISDN
        ,'01' YK_REASON
from 	BASS1.G_S_02039_MONTH_RPT0135B 	 a
		,bass2.stat_market_0135_b_final b 
		,bass2.dw_product_$op_month c
where a.CHANNEL_ID = b.CHANNEL_ID
and a.user_id = c.user_id 
and  b.WARN_LEVEL in (1,2,3)
and a.time_id = $op_month
and b.OP_TIME = $op_month
with ur
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_02039_MONTH 3


set sql_buff "
	select count(0)
	from  bass1.G_S_02039_MONTH where time_id=$op_month
	and channel_id not in ( select channel_id from G_I_06021_MONTH where  time_id=$op_month )
	with ur
"

chkzero2 $sql_buff "G_S_02039_MONTH has invalid channel_id! "
	
  #1.���chkpkunique
	set tabname "G_S_02039_MONTH"
	set pk 			"USER_ID"
	chkpkunique ${tabname} ${pk} ${op_month}

	return 0
}


##~   ���ֶν�ȡ���·��ࣺ
##~   01���źų�������
##~   02��������������
##~   03��SPҵ������
##~   04����������ҵ������
##~   05���������޷�����ʱ��д��

