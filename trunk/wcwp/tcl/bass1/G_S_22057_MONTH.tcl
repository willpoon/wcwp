
######################################################################################################		
#�ӿ�����: "����������������"                                                               
#�ӿڱ��룺22057                                                                                          
#�ӿ�˵����"��¼���������쳣��������ա�����������Ӫʡ����Ӫ����ϵͳ����֧�ŷ�����������Ԥ��ģ��"
#��������: G_S_22057_MONTH.tcl                                                                            
#��������: ����22057������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110727
#�����¼��
#�޸���ʷ: 1. panzw 20110727	1.7.4 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      



  #ɾ����������
	set sql_buff "delete from bass1.G_S_22057_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22057_MONTH
			( 
				 TIME_ID
				,OP_TIME
				,CHANNEL_ID
				,ALARM_TYPE
				,ALARM_LVL
				,YK_CNT
				,YK_AMT
			)
			select 
				$op_month TIME_ID
				,'$op_month' OP_TIME
				,char(a.CHANNEL_ID) CHANNEL_ID
				,'02' ALARM_TYPE
				,case 
					when WARN_LEVEL = 3 then '01'
					when WARN_LEVEL = 2 then '02'
					when WARN_LEVEL = 1 then '03'
				 end ALARM_LVL 
				,char(a.MONTH_NEW) YK_CNT
				,char(a.MONTH_NEW*30) YK_AMT			
			from bass2.stat_market_0135_b_final a
			where a.OP_TIME = $op_month
			and WARN_LEVEL in (1,2,3)
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22057_MONTH 3


set sql_buff "
	select count(0)
	from  bass1.G_S_22057_MONTH where time_id=$op_month
	and channel_id not in ( select channel_id from G_I_06021_MONTH where  time_id=$op_month )
	with ur
"

chkzero2 $sql_buff "G_S_22057_MONTH has invalid channel_id! "
	
  #1.���chkpkunique
	set tabname "G_S_22057_MONTH"
	set pk 			"OP_TIME||CHANNEL_ID||ALARM_TYPE||ALARM_LVL"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	return 0
}