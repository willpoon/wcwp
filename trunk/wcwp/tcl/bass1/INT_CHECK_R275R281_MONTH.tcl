######################################################################################################
#�������ƣ�	INT_CHECK_R275R281_MONTH.tcl
#У��ӿڣ�	������ؽ��
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-04-19
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #������
        global app_name
        set app_name "INT_CHECK_R275R281_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R275' ,'R276' ,'R277' ,'R278' ,'R279' ,'R280' ,'R281')
 	  "        
	  exec_sql $sqlbuf

##~   R276	��	09_������Ӫ	��Ӫ����ǰ̨Ӫҵ���	06023 ʵ��������Դ������Ϣ	��Ӫ����ǰ̨Ӫҵ�������Ϊ0����ǰ̨Ӫҵ���/ʹ����� �� 60%	0.05	
##~   R277	��	09_������Ӫ	��Ӫ����̨ϯ������Ӫҵ��Ա����	06023 ʵ��������Դ������Ϣ	��Ӫ����̨ϯ������0����Ӫҵ��Ա������0����1.5��Ӫҵ��Ա����/̨ϯ������2.5	0.05	
##~   R278	��	09_������Ӫ	ʵ������������Ϣ���¹�ϵ	"06021 ʵ������������Ϣ
##~   06035 ʵ������������Ϣ����������"	06021�е�ʵ����������Ӧ����06035��ĩ�����е�ʵ����������	0.05	
##~   R279	��	09_������Ӫ	ʵ������������������Ϣ���¹�ϵ	"06022 ʵ������������������Ϣ
##~   06036 ʵ������������������Ϣ����������"	06022�е�ʵ����������Ӧ����06036��ĩ�����е�ʵ����������	0.05	
##~   R280	��	09_������Ӫ	ʵ��������Դ������Ϣ���¹�ϵ	"06023 ʵ��������Դ������Ϣ
##~   06037 ʵ��������Դ������Ϣ����������"	06023�е�ʵ����������Ӧ����06037��ĩ�����е�ʵ����������	0.05	
##~   R281	��	09_������Ӫ	����Ӫҵ���ķź����Ͷ����ն�������	22066 ���������ؼ������ջ���	����Ӫҵ���ķź���=0���Ҷ����ն�������=0	0.05	


   ##~   set RESULT_VAL1 0
   ##~   set RESULT_VAL2 0
   ##~   set RESULT_VAL3 0

##~   R275	��	09_������Ӫ	��Ӫ���ľ�γ��	06021 ʵ������������Ϣ	��ͬ��Ӫ���ľ�γ�Ȳ�����ȫ��ͬ	0.05	

set sql_buff "
			select count(0) cnt
			from (
				select LONGITUDE,LATITUDE,count(0)
				from bass1.g_i_06021_month  
				where time_id =$op_month and channel_type='1'	
				group by LONGITUDE,LATITUDE
				having count(0) > 1	
			) t
			with ur
"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
   ##~   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R275',$RESULT_VAL1,0,0,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL3 < 0.05 } {
		set grade 2
	  set alarmcontent "R275 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
