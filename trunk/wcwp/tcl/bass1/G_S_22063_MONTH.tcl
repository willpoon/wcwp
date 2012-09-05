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
# 20120.01.13 ��������������ֵҵ�����޸ķźš���������ֵҵ����ھ�
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
    #���� yyyy-mm-dd
    set optime $op_time
    #���� yyyymm
    #set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
	set last_month [GetLastMonth [string range $op_month 0 5]]
    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

		global app_name
		set app_name "G_S_22063_MONTH.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22063_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff



	  
    # 10	��ͨҵ����	            
    # 11	ǰ̨�ɷѳ��	            
    # 12	�����ն˽ɷѳ��	        
    # 13	Ӫ������û�Ԥ��ѳ��	
    # 17	���д��շѳ��	          
    # 18	��E�г��	                
    # 19	Ԥ�Ῠ���	              
    # 20	���г�ֵ���	            
    # 21	��ֵ�����	              
    # 22	TD�������г��	          
    # 23	�������г��   
	
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
			 	,trim(char(a.CHANNEL_ID)) CHANNEL_ID
				,char(bigint( sum(case when t_index_id in (1,2,3,4,5,6) then result else 0 end )                 )) FH_REWARD
				,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21,22,23) then result else 0 end )   )) BASIC_REWARD
				,char(bigint( sum(case when t_index_id in (7,8,9) then result else 0 end )                      )) INCR_REWARD
				,'0' INSPIRE_REWARD
				,'0' TERM_REWARD
				,'0' RENT_CHARGE
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
        ,'0' BASIC_REWARD
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
    


## ͨ�������Զ�����22063����Ӧ����06021��
   set sql_buff "
				delete from (select * from bass1.g_s_22063_month  where time_id =$op_month) t 
				where channel_id not in (select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
			"
	
    exec_sql $sql_buff
    

	
	
	## ������ֵҵ����
	
		set sql_buff "
	
					update   (select * from G_S_22063_MONTH where time_id = $op_month and INCR_REWARD <= '0') a 
						set a.INCR_REWARD = 
						(select char(b.VAL_BUSI_REC_CNT*5) from 
							(select channel_id,sum(bigint(VAL_BUSI_REC_CNT))VAL_BUSI_REC_CNT
							from g_s_22091_day
							where time_id / 100 = $op_month
							and channel_id in (select distinct channel_id from g_s_22063_month b   
												where time_id = $op_month  and  INCR_REWARD <= '0' )
							group by channel_id
							) b 
						where a.channel_id = b.channel_id )
	with ur
				"
		exec_sql $sql_buff

		
		set sql_buff "
	
					update   (select * from G_S_22063_MONTH where time_id = $op_month) a 
						set a.INCR_REWARD = '0'
						where a.INCR_REWARD is null 
	with ur
				"
		exec_sql $sql_buff
		


## ���¼��������

set sql_buff "

				update   (select * from G_S_22063_MONTH where time_id = $op_month) a 
					set a.INSPIRE_REWARD =  char(bigint(round(1000+rand(1)*800,0)))
					where a.channel_id in (select distinct b.channel_id from g_s_22063_month b  
											where b.time_id = $op_month  and  b.FH_REWARD > '0' and INSPIRE_REWARD <= '0')
with ur
			"
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

    set sql_buff1 "
	select sum(bigint(FH_REWARD)) 
	from G_S_22063_MONTH where time_id = $op_month
	with ur
	"
    set sql_buff2 "
	select sum(bigint(FH_REWARD)) 
	from G_S_22063_MONTH where time_id = $last_month
	with ur
	"

	ChnRatio $sql_buff1 $sql_buff2


			set grade 2
	        set alarmcontent "���������𱨱�002�����Ƿ�Ϊ�գ�"
	        WriteAlarm $app_name $op_month $grade ${alarmcontent}



##~   select OP_TIME 
##~   ,  count(distinct RESULT ) 
##~   from bass2.stat_channel_reward_0002 
##~   group by  OP_TIME 
##~   order by 1 


	return 0
}

