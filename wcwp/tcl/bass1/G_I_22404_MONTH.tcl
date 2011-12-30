
######################################################################################################		
#�ӿ�����: У԰�������û����Ž���Ȧ                                                               
#�ӿڱ��룺22404                                                                                          
#�ӿ�˵����У԰�������û����͵�Ե���ŵ����жԶ˺��뼯�ϡ�
#��������: G_I_22404_MONTH.tcl                                                                            
#��������: ����22404������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time


  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
puts $this_month_first_day
set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
puts $this_month_last_day

  #������
  global app_name
  set app_name "G_I_22404_MONTH.tcl"
        
  #ɾ����������
	set sql_buff "delete from bass1.G_I_22404_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
# ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
	set i 0
# ���������������� , $i<= n   ,  n Խ��Խ��Զ
	while { $i<=62 } {
	        set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        set i_time [get_single $sql_buff]
	
	if { $i_time >= ${this_month_first_day} && $i_time <= ${this_month_last_day} } {
	puts $i_time
	        set sql_buff "select replace(char(current date - ( 1+$i ) days),'-','') from bass2.dual"
	        set i_timestamp [get_single $sql_buff]
		Deal_22404_cdr $i_timestamp $optime_month
	}
	incr i
	}



set sql_buff "ALTER TABLE G_I_22404_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
exec_sql $sql_buff


set sql_buff "
insert into G_I_22404_MONTH_2
select 
	$op_month
	,user_id
	,product_no
	,opposite_no
	,sum(sms_counts)
from G_I_22404_MONTH_1 a
where OP_TIME/100 = $op_month
group by 
	 user_id
	,product_no
	,opposite_no
with ur
"
exec_sql $sql_buff

  set sql_buff "
  insert into G_I_22404_MONTH
  (
         TIME_ID
        ,USER_ID
        ,OPP_NBR
        ,CALL_CNT
  )
select  
	$op_month TIME_ID
        ,a.USER_ID
	,opposite_no
	,char(SMS_COUNTS) CALL_CNT	
from   G_I_22404_MONTH_2 a
where op_time = $op_month
and a.user_id is not null 
and a.opposite_no is not null
with ur  
  "
  exec_sql $sql_buff
  
  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_22404_MONTH"
  set pk   "USER_ID||OPP_NBR"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
	return 0
}



proc Deal_22404_cdr { op_time optime_month } {

  #set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  set timestamp $op_time
  #���� yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
  
set sql_buff "delete from G_I_22404_MONTH_1 where op_time = $timestamp"
exec_sql $sql_buff

set sql_buff "
insert into G_I_22404_MONTH_1
		    	  select
				 $timestamp,
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 a.opp_number,
		    	  	 count(1)
		    	  from bass2.cdr_sms_dtl_$timestamp a inner join bass2.dw_xysc_school_real_user_dt_$op_month b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_number
with ur
"
exec_sql $sql_buff

return 0

}
