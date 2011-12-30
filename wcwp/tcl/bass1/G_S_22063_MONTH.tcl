######################################################################################################
#�ӿ����ƣ�ʵ��������𼰲�����Ϣ
#�ӿڱ��룺22063
#�ӿ�˵������¼ʵ��������𼰲�������Ϣ������ί�о�Ӫ����������������ʵ����������������Ӫ��
#��������: G_S_22063_MONTH.tcl
#��������: ����22063������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-11-9
#�����¼��
#�޸���ʷ:
#2011-04-15  ����06021��������Ӫ��ί�о�Ӫ�������������������ı�ʶ���������22063�С����� 06021 ��������Ӫ��ί�о�Ӫ���������������������� 22063 �С�����ҵ���������� ��Ϊ100��������0.����ʽ����뱾������
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

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

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

		set app_name "G_S_22063_MONTH.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22063_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff



    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_S_22063_MONTH
		(	 	 	TIME_ID
				 ,STATMONTH         	/* --�·�                 */
				 ,CHANNEL_ID        	/* --ʵ��������ʶ         */
				 ,FH_REWARD         	/* --�źų��             */
				 ,BASIC_REWARD      	/* --����ҵ���������� */
				 ,INCR_REWARD       	/* --��ֵҵ�������     */
				 ,INSPIRE_REWARD    	/* --�������             */
				 ,TERM_REWARD       	/* --�ն˳��             */
				 ,RENT_CHARGE       	/* --���ⲹ��           */
		)
		  SELECT
			   $op_month
			 	,'$op_month'
			 	,trim(char(a.CHANNEL_ID))
				,char(bigint( sum(case when t_index_id in (1,4,14) then result else 0 end )                 ))
				,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21) then result else 0 end )   ))
				,char(bigint( sum(case when t_index_id in (7) then result else 0 end )                      ))
				,'0'
				,'0'
				,'0'
			FROM BASS2.DW_CHANNEL_INFO_$op_month A
			inner join bass2.stat_channel_reward_0002 b on a.channel_id=b.channel_id
			WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102) 
						and b.op_time=$op_month
						AND B.result>0
			group by trim(char(a.CHANNEL_ID))
			"
    puts $sql_buff
    exec_sql $sql_buff

#11����06021��������Ӫ��ί�о�Ӫ�������������������ı�ʶ���������22063�У�
#�� 06021 ��������Ӫ��ί�о�Ӫ���������������������� 22063 �С�����ҵ���������� ��Ϊ100��������0.
    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_S_22063_MONTH
		(	 	 	TIME_ID
				 ,STATMONTH         	/* --�·�                 */
				 ,CHANNEL_ID        	/* --ʵ��������ʶ         */
				 ,FH_REWARD         	/* --�źų��             */
				 ,BASIC_REWARD      	/* --����ҵ���������� */
				 ,INCR_REWARD       	/* --��ֵҵ�������     */
				 ,INSPIRE_REWARD    	/* --�������             */
				 ,TERM_REWARD       	/* --�ն˳��             */
				 ,RENT_CHARGE       	/* --���ⲹ��           */
		)
		 select 
			   $op_month TIME_ID
			 	,'$op_month' STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'100' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
			from   bass1.g_i_06021_month a
			where a.channel_id not in
			(select distinct b.channel_id from bass1.g_s_22063_month b where b.time_id =$op_month)
			  and a.time_id =$op_month
			  and a.channel_type in ('2','3')
			  and a.channel_status='1'
			"
    puts $sql_buff
    exec_sql $sql_buff
    
    #У�飺9����22063�е�������ʶ���������06021�з���Ӫ��������ʶ�У�
        set sql_buff "
    select count(*) from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
  and time_id =$op_month
  			"
    set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "22063����channel_id����06021�ķ���Ӫ������"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }
  
    #У�飺10��22063�в��ܴ�����Ӫ����������ʶ��
  
    set sql_buff "
    select count(*) from bass1.g_s_22063_month
			where channel_id in
			(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type='1')
			  and time_id =$op_month
     "
      set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "22063�в��ܴ�����Ӫ����������ʶ"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }
  
  #11����06021��������Ӫ��ί�о�Ӫ�������������������ı�ʶ���������22063�У�
    set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type in ('2','3')
  and channel_status='1'
     "
      set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "06021��������Ӫ��ί�о�Ӫ�������������������ı�ʶ���������22063"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }
    
  
	return 0
}

