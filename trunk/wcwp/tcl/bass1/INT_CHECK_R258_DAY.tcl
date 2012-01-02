######################################################################################################
#�������ƣ�	INT_CHECK_R258_DAY.tcl
#У��ӿڣ�	02054
#��������: DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  2011.11.14 ���ݼ��ſ����޸�R258�ھ�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
      #��Ȼ�� ���� 01 ��
      set last_month_first_day ${op_month}01
      
        #������
        set app_name "INT_CHECK_R258_DAY.tcl"



########################################################################################################3


##	#R258	
##	��	10_�ʷѶ���	ͳһ�ʷѰ���������Ч����ϵ	"02022 �û�ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ�
##	02024 ȫ��ͨ�����ʷ��ײ��û��ɹ�������"	02022�����û����гɹ����������û�ռ�ȡ�98%	0.05	"��һ����ȡ02022�ӿ�����Ч���ڵ���ͳ���գ�ʧЧ���ڴ���ͳ���յ��û���ʶ�ͻ����ײͱ�ʶ��
##	�ڶ�����ȡ02024�������û���ʶ�ͻ����ײͱ�ʶ��
##	����������һ���Ľ��ͨ���û���ʶ�ͻ����ײͱ�ʶ������ڶ����Ľ�����ܹ����ϵļ�Ϊͳ�����гɹ��������������û���
##	���Ĳ������������û������Ե�һ�����û��������ݱ�ֵ�����жϡ�"
##	
  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R258') "        


	  exec_sql $sqlbuf


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
	select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
	from 
	(
	select USER_ID,BASE_PKG_ID from   
	G_I_02022_DAY  a
	where time_id = $timestamp and VALID_DT = '$timestamp'
	) a
	left join (
		select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
		from (
			select  USER_ID,BASE_PKG_ID from 
			G_S_02024_DAY a
			where time_id >= $last_month_first_day and time_id <= $timestamp 
		     ) a 
		left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
	) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
	with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]
        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R258',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
                
        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] < 0.98 } {
                set grade 2
                set alarmcontent " R258 У��1��ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

## 2011.11.14 ���ݼ��ſھ��޸ģ�����ԭУ�飬������ΪR258_2
  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R258_2') "        


	  exec_sql $sqlbuf


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
	select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
	from 
	(
	select USER_ID,BASE_PKG_ID from   
	G_I_02022_DAY  a
	where time_id = $timestamp and VALID_DT = '$timestamp'
	) a
	left join (
			select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
		from (
			select  USER_ID,BASE_PKG_ID from 
			G_S_02024_DAY a
			where time_id <= $timestamp 
		     ) a 
		left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
		) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
	with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]
        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R258_2',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
                
        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] < 0.98 } {
                set grade 2
                set alarmcontent " R258_2 У��2��ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }


	return 0
}

