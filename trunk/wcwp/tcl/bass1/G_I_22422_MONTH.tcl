
######################################################################################################		
#�ӿ�����: ����ϵͳʶ��߷����û�����                                                               
#�ӿڱ��룺22422                                                                                          
#�ӿ�˵������Ӫ����ϵͳ�����ɵĸ߷����û���������CRMϵͳ�����������ż��ƽ̨�������������ƺ������ͻ��������ȼ�����
#��������: G_I_22422_MONTH.tcl                                                                            
#��������: ����22422������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20111129
#�����¼��
#�޸���ʷ: 1. panzw 20111129	��������1.2 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
	##~   set i 1
##~   # ���������������� , $i<= n   ,  n Խ��Խ��Զ
	##~   while { $i<=30 } {
	        ##~   set sql_buff "select substr(char(current date - (20 - $i ) months ),1,7) from bass2.dual"
	        ##~   set optime_month [get_single $sql_buff]
	
	##~   if { $optime_month <= "2012-07" } {
	##~   puts $optime_month
	##~   p22422 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
#set op_time 2011-11-13
##~   p22421 $op_time $optime_month




return 0

}

##~   ����ϵͳʶ��߷����û����� (201204-201207)
##~   bass2.sub_refuse_sms_danger_yyyymm �����´��ڼ������ݡ�
 
 
proc p22422 { op_time optime_month } {
#set op_time 2011-06-07
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
		
    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
	set last_month [GetLastMonth [string range $op_month 0 5]]
    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#��Ȼ��
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_I_22422_MONTH.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_I_22422_MONTH where time_id=$timestamp"
    exec_sql $sql_buff




    ##~   set sql_buff "
	##~   insert into G_I_22422_MONTH
	##~   select 
         ##~   $op_month TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,USER_ID
        ##~   ,'$op_month' ADD_MONTH
	##~   from bass2.sub_refuse_sms_danger_$op_month a
	##~   union all
	##~   select
	     ##~   $op_month TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,USER_ID
        ##~   ,ADD_MONTH
	##~   from G_I_22422_MONTH
	##~   where time_id = $last_month
	##~   with ur	
	##~   "
    ##~   exec_sql $sql_buff



    set sql_buff "
	insert into G_I_22422_MONTH
	select 
         $op_month TIME_ID
        ,PRODUCT_NO
        ,USER_ID
        ,'$op_month' ADD_MONTH
	from bass2.sub_refuse_sms_danger_$op_month a
	with ur	
	"
    exec_sql $sql_buff
	
	
	return 0
}


