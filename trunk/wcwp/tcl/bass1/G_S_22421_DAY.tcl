
######################################################################################################		
#�ӿ�����: �����������ƺ�����������                                                               
#�ӿڱ��룺22421                                                                                          
#�ӿ�˵�����ýӿ�ΪCRMϵͳ�����ƺ��������˹�ȷ�ϵĴ�����־��������Ӫ����ϵͳ��
#��������: G_S_22421_DAY.tcl                                                                            
#��������: ����22421������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110811
#�����¼��
#�޸���ʷ: 1. panzw 20110811	1.7.4 newly added
#######################################################################################################   
#�������������ƺ����������������еġ��û���ʶ����Ӧ���ڡ��û������д���


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
	##~   set i 1
# ���������������� , $i<= n   ,  n Խ��Խ��Զ
	##~   while { $i<=200 } {
	        ##~   set sql_buff "select char(current date - ( 30+31+30+31+15+7+1 - $i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time <= "2012-08-22" } {
	##~   puts $op_time
	##~   p22421 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
#set op_time 2011-11-13
p22421 $op_time $optime_month




return 0

}

proc p22421 { op_time optime_month } {
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
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22421_DAY.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22421_DAY where time_id=$timestamp"
    exec_sql $sql_buff


 ##~   �����������ƺ����������� ��20120401-20120815��
##~   Sub_refuse_sms_deal_yyyymmdd 
##~   G_S_22421_DAY

##~   �����ʼʱ�䡢��ؽ���ʱ�䡢���к��롢���к��롢Υ��ԭ�򡢳�����Ŀ��MSGID���������͡�����״̬��������������ʱ�䡣

         ##~   ACCEPT_TIME
        ##~   ,RESULT_TIME
        ##~   ,USER_ID
        ##~   ,TARGETNO
        ##~   ,REASON
        ##~   ,SUM
        ##~   ,PROC_STATUS
        ##~   ,DEAL_RESULT
        ##~   ,SUBMIT_TIME
		
##~   ��¼�к�
##~   �����ʼ����
##~   ��ؽ�������
##~   �û���ʶ
##~   ���ƺ���������
##~   Υ��ԭ��
##~   ������Ŀ
##~   ����״̬
##~   ������


    set sql_buff "
	insert into G_S_22421_DAY
	select 
		 $timestamp TIME_ID
        ,substr(ACCEPT_TIME,1,8) MON_BEGIN_DT
        ,'$timestamp' MON_END_DT
        ,USER_ID USER_ID
        ,TARGETNO PRODUCT_NO
        ,case when REASON is null or REASON = '' or REASON = ' ' then '5' else REASON end ILLEGAL_REASON
        ,case when SUM is null or SUM = '' or SUM = ' ' then '0' else SUM end OVER_CNT
        ,'1' DEAL_STS
        ,DEAL_RESULT DEAL_RESULT
        ,substr(SUBMIT_TIME,1,8) DEAL_DT
	from bass2.Sub_refuse_sms_deal_$timestamp
	where substr(SUBMIT_TIME,1,8) = '$timestamp'
	with ur
	"
    exec_sql $sql_buff
	
	return 0
}


