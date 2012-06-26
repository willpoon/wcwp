######################################################################################################
#�������ƣ�	INT_CHECK_R287_DAY.tcl
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
        set app_name "INT_CHECK_R287_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R287'
					)
			"

		exec_sql $sql_buff




##~   R287	��	02_���ſͻ�	B�༯�ſͻ�������	01004 ���ſͻ�	B�༯�ſͻ������������±䶯�� �� 30%	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

	set sql_buff "
select
 a.curr_cnts
,b.bef_cnts
,case when bef_cnts=0 then 1
	  else decimal((curr_cnts-bef_cnts)*1.0/bef_cnts,10,2)
 end
from table(
select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type,count(0) curr_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end
) a,
table(select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type
			 ,count(0) bef_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $last_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end) b
where a.type = b.type
"

   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R287',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.3 } {
                set grade 2
                set alarmcontent " R287 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



	return 0
}
