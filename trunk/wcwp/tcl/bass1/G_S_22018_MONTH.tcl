######################################################################################################
#�ӿ����ƣ�����ͳ���»���2
#�ӿڱ��룺22018
#�ӿ�˵������¼����ͳ�Ƶ������Ϣ������"03  �������ͱ���"������άֵ�����ṩ������άֵ���ϱ��򱨣���
#��������: G_S_22018_MONTH.tcl
#��������: ����22018������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#���� yyyy-mm
	set opmonth $optime_month	
	#���� yyyy-mm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #��������
        set this_month_days [GetThisMonthDays ${op_month}01]
        #�������� 
        set this_year_days [GetThisYearDays ${op_month}01]
        #��������
        set last_month_days [GetThisMonthDays ${last_month}01]
        #�������һ�� yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]   
        
        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $this_month_last_day 2 2]
        puts $DEC_CHECK_VALUE_7
        
        set DEC_CHECK_VALUE_6 [exec get_kpi.sh $last_month 7 2];
        puts $DEC_CHECK_VALUE_6
	return 0
}	