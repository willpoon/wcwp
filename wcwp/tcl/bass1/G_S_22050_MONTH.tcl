######################################################################################################

#�ӿ����ƣ�ȫ������������������

#�ӿڱ��룺22050

#�ӿ�˵����ͳ������ȫ���Ժ���������������������������Ϣ���

#��������: G_S_22050_MONTH.tcl

#��������: ����22050������

#��������: ��

#Դ    ��

#�������: 

#�������: ����ֵ:0 �ɹ�;-1 ʧ��

#�� д �ˣ�liuqf

#��дʱ�䣺2011-01-17

#�����¼��

#�޸���ʷ: 
# 2011.11.29  panzw 1.7.7  1.�������ԡ�����������ơ���	2.�޸����ԡ�������顱�����ӷ��ࡰ4�����򡱡���5������������������������ʡ��˾�����ĺ�����飩����

#######################################################################################################





proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



	#���� yyyymmdd

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd

        set optime $op_time

        

        #���� yyyymm

        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          

        puts $op_month



				#����  yyyymm

				set last_month [GetLastMonth [string range $op_month 0 5]] 

				puts $last_month          



        #���µ�һ�� yyyy-mm-dd

        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01

        puts $this_month_first_day



        #�������һ�� yyyy-mm-dd

        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]

        puts $this_month_last_day

        

        #ɾ����������

	set sql_buff "delete from bass1.g_s_22050_month where time_id=$op_month"

	puts $sql_buff

  exec_sql $sql_buff

       



  #���� 
#2011.11.29 ��bossŷ��ȷ�ϣ� ���������������ĺ�����飬���Զ���0

#1������
#2������
#3������ͨ
#4������
#5������

	set sql_buff "insert into bass1.g_s_22050_month

               select 
                  $op_month,
                  '$op_month',
                  COMATE,
		  case 
		  when COMATE = '1' then '����'
		  when COMATE = '2' then '����'
		  when COMATE = '3' then '����ͨ'
		  when COMATE = '4' then '����'
		  else '����' end comate_name,
		  CHANNELCOUNT,
                  NEWCUSTCOUNT,
                  PAYCOUNT,
                  BUSICOUNT,
                  COMINALCOUNT,
                  OTHERCOUNT,
                  QQTOPERATORCOUNT,
                  SZXOPERATORCOUNT,
                  DGDDOPERATORCOUNT,
                  PAYMONEY 
                from bass1.g_s_22050_month
               where time_id=$last_month

         "





  puts $sql_buff

  exec_sql $sql_buff

  

 

	return 0

}








