
######################################################################################################		
#�ӿ�����: �������ź�����                                                               
#�ӿڱ��룺22420                                                                                          
#�ӿ�˵�����ýӿ�ΪCRMϵͳ��ȷ�ϡ�ά���ĺ�������������Ӫ����ϵͳ�����к������ͻ��ķ��������١�
#��������: G_I_22420_DAY.tcl                                                                            
#��������: ����22420������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110811
#�����¼��
#�޸���ʷ: 1. panzw 20110811	1.7.4 newly added
#######################################################################################################   
#���������ź����������еġ��û���ʶ����Ӧ���ڡ��û������д���
#22420 22421 Ϊ�в�������һ������ԭ�ھ��ϱ������գ� Ϊ����9��ǰ�����������γ����ϱ����� 9��ǰ�����в����9������

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
	##~   set i 1
##~   # ���������������� , $i<= n   ,  n Խ��Խ��Զ
	##~   while { $i<=300 } {
	        ##~   set sql_buff "select char(current date - ( 30+31+30+31+15+7+1 - $i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time <= "2012-08-22" } {
	##~   puts $op_time
	##~   p22420 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
##~   #set op_time 2011-11-13
p22420 $op_time $optime_month




return 0

}

proc p22420 { op_time optime_month } {
#set op_time 2011-06-07
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
		
    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#��Ȼ��
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
	
	set last_day [GetLastDay [string range $timestamp 0 7]]
	 
    puts $this_month_last_day
		global app_name
		set app_name "G_I_22420_DAY.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_I_22420_DAY where time_id=$timestamp"
    exec_sql $sql_buff

##~   alter table bass1.G_I_22420_DAY activate not logged initially with empty table
 


    ##~   set sql_buff "
	##~   insert into G_I_22420_DAY
	##~   select distinct 
		 ##~   $timestamp TIME_ID
        ##~   ,COMPLAINED_NO BLACK_NBR
        ##~   ,USER_ID USER_ID
        ##~   ,replace(char(date(ACCEPT_TIME)),'-','') ADD_DT
        ##~   ,replace(char(time(ACCEPT_TIME)),'.','') ADD_TIME
	##~   from bass2.Sub_refuse_sms_junk_$timestamp
	##~   union all
	##~   select distinct
        ##~   $timestamp TIME_ID
        ##~   ,BLACK_NBR
        ##~   ,USER_ID
        ##~   ,ADD_DT
        ##~   ,ADD_TIME
	##~   from G_I_22420_DAY  where time_id = $last_day
	##~   with ur
	
	##~   "
    ##~   exec_sql $sql_buff


    set sql_buff "
	insert into G_I_22420_DAY
	select distinct 
		 $timestamp TIME_ID
        ,COMPLAINED_NO BLACK_NBR
        ,USER_ID USER_ID
        ,replace(char(date(ACCEPT_TIME)),'-','') ADD_DT
        ,replace(char(time(ACCEPT_TIME)),'.','') ADD_TIME
	from (
			select i.*,row_number()over(partition by user_id order by ACCEPT_TIME asc) rn 
			from bass2.Sub_refuse_sms_junk_$timestamp i
	) o where o.rn = 1
	with ur
	
	"
    exec_sql $sql_buff
	
	return 0
}

