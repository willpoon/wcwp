
######################################################################################################		
#�ӿ�����: ���������ص���ֵҵ������ջ���                                                               
#�ӿڱ��룺22096                                                                                          
#�ӿ�˵������¼��������29���ص���ֵҵ�����ջ�����Ϣ��
#��������: G_S_22096_DAY.tcl                                                                            
#��������: ����22096������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120529
#�����¼��
#�޸���ʷ: 1. panzw 20120529	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #�ϸ��� yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #������
        global app_name
        set app_name "G_S_22096_DAY.tcl"
	
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22096_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	return 0
}

���ֶν�ȡ���·��ࣺ
01:�Ż���վ
02:10086�绰Ӫҵ��
03: ����Ӫҵ��
04:WAP��վ
05:�����նˣ��������е������նˣ�������ʵ��������24СʱӪҵ���ڲ��ŵ������նˣ��������̳��ȳ��������ڷŵ������նˡ���



select 

��վ��

					 select '01' ECHNL_TYPE
							,
							,count(0)
                       from bass2.dw_product_ord_cust_dm_$curr_month a
                       , bass2.dw_product_$timestamp b
					   , bass2.dw_product_ord_offer_dm_201204 c
                    where a.product_instance_id = b.user_id 
					and a.ORDER_STATE = 11
					and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
					and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
					
					
					


01:��������
02:��������
03:�Ų��ܼ�
04:���Ż�ִ
05:��Ϣ�ܼ�
06:���Ż�Ա
07:139�����շѰ�
08:�ֻ�֤ȯ
09:�ֻ��̽�
10:blackberry
11:�������־��ֲ�
12:�ֻ�����
13:����
14:��ý�����
15:����������
16:��������
17:�ֻ�����(���ֹ㲥��ʽ)
18:�ֻ�ҽ��
19:�ֻ���ͼ
20:�ֻ�����
21:��Ѷ
22:�ֻ��Ķ�
23:�ֻ���
24:�����������ֲ�
25:�ֻ���Ƶ
26:�ֻ���Ϸ
27:����
28:�ƶ�Ӧ���̳�
29:12580�����


