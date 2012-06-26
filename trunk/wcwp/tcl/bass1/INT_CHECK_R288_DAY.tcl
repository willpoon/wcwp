######################################################################################################
#�������ƣ�	INT_CHECK_R288_DAY.tcl
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
        set app_name "INT_CHECK_R288_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R288'					
					)
			"

		exec_sql $sql_buff




##~   R288	��	02_���ſͻ�	�嵥ID�뼯�ſͻ���ʶ��Ӧ��ϵ֮��Ķ�Զ��¼	01007 ���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ	01007�ӿ����嵥�ͻ�ID�뼯�ſͻ���ʶ��Ӧ��ϵ�����ڶ�Զ��¼����	0.05


##~   ͳ��01007�����ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ���ӿڡ���Ӧ��ϵ״̬��Ϊ�����ļ�¼�У�Ŀ���г��嵥ID
##~   ����
##~   �嵥ID�뼯�ſͻ���ʶ��һ�Զ��ϵ��
##~   ����
##~   �嵥ID�뼯�ſͻ���ʶ�Ķ��һ��ϵ��
##~   �ļ�¼���������������0����Υ������	

	set sql_buff "
select count(0) from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
and ENTERPRISE_ID in (
select ENTERPRISE_ID from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by ENTERPRISE_ID having count(0) > 1 
) 
and CUST_ID in (
select CUST_ID  from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by CUST_ID having count(0) > 1 
)


"

chkzero2 $sql_buff "R288 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R288',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
	return 0
}
