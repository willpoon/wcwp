
######################################################################################################		
#�ӿ�����: ���������𼰲�����Ϣ                                                               
#�ӿڱ��룺22062                                                                                          
#�ӿ�˵������¼��������˹�ǰ̨ҵ��������������ĳ����������Ĳ�����������Ϣ�������������������ί�о�Ӫ�����������������㡱��
#��������: G_S_22062_MONTH.tcl                                                                            
#��������: ����22062������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20111128
#�����¼��
#�޸���ʷ: 1. panzw 20111128	1.7.7 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
		##~   set optime_month 2012-05
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        #���� YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
	 set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
	puts $this_month_first_day
	set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
	puts $this_month_last_day
	global app_name
	set app_name "G_S_22062_MONTH.tcl"    

        #ɾ����������
	set sql_buff "delete from bass1.G_S_22062_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
	set sql_buff "
insert into 	 bass1.G_S_22062_MONTH 
select 
        $op_month TIME_ID
        ,'$op_month' OP_MONTH
        ,char(CHANNEL_ID) CHANNEL_ID
        --,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_ACT_REWARD
        --,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS like '%����%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%ǰ̨�ɷѳ��%' 
				or BUSI_NOTES  like '%���д��շѳ��%'  
				or BUSI_NOTES  like '%Ӫ������û�Ԥ��ѳ��%'  
				or BUSI_NOTES  like '%������˾���´��շѳ��%'  
				then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-�������־��ֲ�%' 
				or BUSI_NOTES  like '%��ҵ��-����%'
				then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-���Ź���%' 
				or BUSI_NOTES  like '%��ҵ��-139�ֻ�����%Ԫ��%'
				or BUSI_NOTES  like '%�Ų��ܼ�%'
				then RESULT else 0 end))) VAL_TYPE2_REWARD
       /** 
	   ,char(bigint(sum(case when BUSI_NOTES like '%����������%' 
				or BUSI_NOTES  like '%��������%'
				or BUSI_NOTES  like '%12580�����%'
				or BUSI_NOTES  like '%�����������ֲ�%'
				or BUSI_NOTES  like '%��Ϣ�ܼ�%'
				or BUSI_NOTES  like '%�ֻ��̽�%'
				or BUSI_NOTES  like '%�ֻ�ҽ��%'
				or BUSI_NOTES  like '%�ֻ���ͼ%'
				or BUSI_NOTES  like '%�ֻ�����%'
				or BUSI_NOTES  like '%��Ѷ%'
				or BUSI_NOTES  like '%�ֻ���%'
				or BUSI_NOTES  like '%�ֻ��Ķ�%'
				or BUSI_NOTES  like '%�ֻ���Ƶ%'
				or BUSI_NOTES  like '%�ֻ���Ϸ%'
				or BUSI_NOTES  like '%�ֻ�����%'
				then RESULT else 0 end))) VAL_TYPE3_REWARD
	  **/
	  ,char(bigint(sum(case when BUSI_NOTES like '��ҵ��%' 
				and not (
				BUSI_NOTES like '%��ҵ��-�������־��ֲ�%' 
				or BUSI_NOTES  like '%��ҵ��-����%'
				or BUSI_NOTES like '%��ҵ��-���Ź���%' 
				or BUSI_NOTES  like '%��ҵ��-139�ֻ�����%Ԫ��%'
				or BUSI_NOTES  like '%�Ų��ܼ�%'				
				)
				then RESULT else 0 end))) VAL_TYPE3_REWARD
	,char(bigint(sum(case when BUSI_NOTES like '%�ƶ�Ӧ���̳�%'  then RESULT else 0 end))) VAL_DIANBO
        ,'0' STORE_SUSIDY
        ,'0' SALE_ACTIVE_REWARD
        ,'0' ADD_REWARD
        ,'0' B_CLASS_REWARD
from bass2.stat_channel_reward_0019  a
where op_time = $op_month
and CHANNEL_TYPE in (90105,90102)
group by char(CHANNEL_ID)
with ur
"
  exec_sql $sql_buff

#ɾ���Ƿ�CHANNEL_ID
##~   4.1���ǰ�ᔵ����Ҳһ�ӣ��õı�Ҳ��ͬ

if { $op_month <= 201203 } {
	set TAB06035 "G_A_06035_DAY_OLD20120331"
} else {
	set TAB06035 "G_A_06035_DAY"
}

##~   set sql_buff "
##~   delete from G_S_22062_MONTH 
##~   where time_id = $op_month
##~   and channel_id  in (
		##~   select distinct channel_id from G_S_22062_MONTH where time_id = $op_month
		##~   except
		##~   select distinct channel_id
		##~   from
		##~   (
		##~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		##~   from ${TAB06035} a
		##~   where time_id / 100 <= $op_month
		##~   ) t where t.rn =1  and CHNL_STATE = '1'
		##~   ) 
##~   "
  ##~   exec_sql $sql_buff
  
##~   ����22063
   set sql_buff "
				delete from (select * from bass1.g_s_22062_month  where time_id =$op_month) t 
				where channel_id not in (select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
			"
	
    exec_sql $sql_buff
    
	
  aidb_runstats bass1.G_S_22062_MONTH 3

  #1.���chkpkunique
        set tabname "G_S_22062_MONTH"
        set pk                  "OP_MONTH||CHANNEL_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        
	
# У�飺
set sql_buff "
	select count(0) from (
		select distinct channel_id from G_S_22062_MONTH where time_id = $op_month
		except
		select distinct channel_id
		from
		(
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from ${TAB06035} a
		where time_id / 100 <= $op_month
		) t where t.rn =1  and CHNL_STATE = '1'
	) o
	with ur
"
chkzero2 $sql_buff "22062�зǷ�channel_id! "

##~   ����22063 ������������

##~   set sql_buff "
	##~   select count(0)
		##~   from (
		##~   select TIME_ID ,channel_id
		##~   ,sum(bigint(FH_REWARD)) FH_REWARD
		##~   from G_S_22063_MONTH 
		##~   where time_id = $op_month
		##~   group by  TIME_ID ,channel_id
		##~   ) a ,
		##~   (
		##~   select 			
				 ##~   a.TIME_ID	
				##~   ,a.channel_id         
				##~   ,sum(bigint(NUM_ACT_REWARD))	NUM_ACT_REWARD
				##~   ,sum(bigint(NUM_DELAY_ACT_REWARD))			
				##~   ,sum(bigint(NUM_DELAY_ACT_REWARD)+bigint(NUM_ACT_REWARD))			
		##~   from ( select * from G_S_22062_MONTH where time_id = $op_month ) a, ( select * from G_I_06021_MONTH where time_id = $op_month )  b 		
		##~   where a.time_id = $op_month			
		##~   and a.channel_id = b.channel_id
		##~   and b.CHANNEL_STATUS = '1'
		##~   and CHANNEL_TYPE in ('2','3')
		##~   group by  a.time_id ,a.channel_id		
		##~   ) b 
		##~   where a.CHANNEL_ID = b.CHANNEL_ID
		##~   and  FH_REWARD <> NUM_ACT_REWARD
	##~   with ur
##~   "
##~   chkzero2 $sql_buff "22062 �� 22063 �źų��һ��! "

##~   update (select * from G_S_22063_MONTH where time_id = $op_month ) a
##~   set NUM_ACT_REWARD = ''


			set grade 2
	        set alarmcontent "���������𱨱�0019�����Ƿ�Ϊ�ջ�������"
	        WriteAlarm $app_name $op_month $grade ${alarmcontent}



return 0

}

#	
#	���Ա���	��������	��������	��������	��ע
#	01		�·�	��ʽ��YYYYMM	CHAR(6)	��������
#	02		���������ʶ	�μ���ʵ������������Ϣ�������������ӿ��еġ�ʵ��������ʶ�����ԡ�	CHAR(40)	��������
#	03		��������ʵ�����	��λ��Ԫ	NUMBER(10)	
#	04		��������Ӧ�����	��λ��Ԫ	NUMBER(10)	
#	05		���������׸����	��λ��Ԫ	NUMBER(10)	
#	06		�������۵���ʵ�����	��λ��Ԫ	NUMBER(10)	
#	07		�������۵���Ӧ�����	��λ��Ԫ	NUMBER(10)	
#	08		������ջ��ѳ��	��λ��Ԫ	NUMBER(10)	
#	x09		��Լ�ƻ��ն����۳��	��λ��Ԫ	NUMBER(10)	
#	x10		�ն�������۳��	��λ��Ԫ	NUMBER(10)	
#	x11		�����ն����۳��	��λ��Ԫ	NUMBER(10)	
#	x12		   ���ж����ֻ����۳��	��λ��Ԫ	NUMBER(10)	
#	x13		�����ն����۳��	��λ��Ԫ	NUMBER(10)	
#	14		��ֵҵ���������һ���	��ֵҵ���������һҵ�����ҹ�˾������Ӫ��ҵ�񣬰����������־��ֲ�����ý����塣��λ��Ԫ	NUMBER(10)	
#	15		��ֵҵ��������Ͷ����	��ֵҵ��������Ͷ�ҵ�����ҹ�˾���������˾������Ӫ���ҹ�˾���û���ģ����Ӫ��˾֧������ѵ�ҵ�񣬰������Ż�Ա��139�����շѰ桢�Ų��ܼҡ���λ��Ԫ	NUMBER(10)	
#	16		��ֵҵ��������������	��ֵҵ�����������ҵ�����ҹ�˾�������ṩ�̺�����Ӫ���ҹ�˾�������ṩ�̽���ҵ������ֳɵ�ҵ�񣬰����������������������ء�12580������������������ֲ�����Ϣ�ܼҡ��ֻ��̽硢�ֻ�ҽ�ơ��ֻ���ͼ���ֻ���������Ѷ���ֻ������ֻ��Ķ����ֻ���Ƶ���ֻ���Ϸ���ֻ����ӡ���λ��Ԫ	NUMBER(10)	
#	17		��ֵҵ��㲥����	��ֵҵ��㲥��ҵ�����ҹ�˾�������ṩ�̺�����Ӫ���ҹ�˾�������ṩ�̽���ҵ������ֳɵ�ҵ�񣬰����ƶ�Ӧ���̳ǡ���λ��Ԫ	NUMBER(10)	
#	x18		�ŵ겹��	��λ��Ԫ	NUMBER(10)	
#	x19		���ۼ���	��λ��Ԫ	NUMBER(10)	
#	x20		�������	��λ��Ԫ	NUMBER(10)	
#	x21		B��Ŀ¼���	��λ��Ԫ	NUMBER(10)	
#	


##~   select OP_TIME , count(0) 
##~   ,  count(distinct COUNTS ) 
##~   ,  count(distinct RESULT ) 
##~   from bass2.stat_channel_reward_0019 
##~   group by  OP_TIME 
##~   order by 1 


