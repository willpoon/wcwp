######################################################################################################
#�������ƣ�	INT_CHECK_R304_DAY.tcl
#У��ӿڣ�	
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-06-09 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_R304_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R304'					
					)
			"

		exec_sql $sql_buff





##~   --R304	��	02_���ſͻ�	����ͨʹ�ü��Ÿ��˿ͻ�������	02059 ����ҵ������û��󶨹�ϵ	����ͨʹ�ü��Ÿ��˿ͻ����������Ⱦ���ֵС�ڵ���50%
   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
cc.curr_cnts
,dd.bef_cnts
,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,2)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $curr_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$curr_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $last_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$last_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R304',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R304 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




	return 0
}
