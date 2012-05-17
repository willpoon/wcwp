######################################################################################################
#�ӿ����ƣ�ʵ�������ص���ֵҵ������ջ���
#�ӿڱ��룺22092
#�ӿ�˵������¼ʵ������29���ص���ֵҵ�����ջ�����Ϣ���漰��Ӫ����ί�о�Ӫ����24Сʱ����Ӫҵ��������������
#��������: G_S_22092_DAY.tcl
#��������: ����22092������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-06-08
#�����¼��
#�޸���ʷ: ref:22062
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set op_time 2011-06-07

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#��Ȼ��
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22092_DAY.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22092_DAY where time_id=$timestamp"
    exec_sql $sql_buff

		set sql_buff "
		insert into G_S_22092_DAY
		(
         TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
        ,IMP_VAL_TYPE
        ,IMP_VAL_OPEN_CNT		
		)
		select 
		     $timestamp
		    ,'$timestamp' DEAL_DATE
        ,CHANNEL_ID
        ,ACCEPT_TYPE
        ,IMP_ACCEPTTYPE
        ,char(CNT)
		from  BASS1.G_S_22091_DAY_P2 
		where time_id = $timestamp
		"
exec_sql $sql_buff

##~   ɾ�� a.channel_type = 90886 ������
  set sql_buff "
delete from 
(select * from bass1.g_s_22092_day where  TIME_ID = $timestamp and channel_id in (
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where  a.channel_type = 90886
	    )
) t
  "
exec_sql $sql_buff

  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22092_DAY"
  set pk   "DEAL_DATE||CHANNEL_ID||DEAL_TYPE||IMP_VAL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.$tabname 3
  
  set sql_buff "
  select count(0)
from bass1.g_s_22092_day where TIME_ID = $timestamp
and  channel_id not in 
			(
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
		 and a.channel_type <> 90886
	    )
  "
chkzero2 $sql_buff "invaid channel_id"

	return 0

}

