######################################################################################################
#�ӿ����ƣ����ſͻ���Ŀ���г��嵥��Ӧ��ϵ
#�ӿڱ��룺01005
#�ӿ�˵����1�����ӿ������ϴ�Ŀ���г��嵥�ͻ�ID�뵱��ʡ��˾�������ſͻ���ʶ��Ķ�Ӧ��ϵ����ظ��CRM����Ҫ����μ�
#��������ȷ��Ҫ���ſͻ���Ϣ����ʡ��ҵ��֧��ϵͳ����Ҫ���֪ͨ����ҵͨ[2010]76�ţ���
#          2��Ŀ���г��嵥�ͻ�ID���ϱ���Χ�Լ��Ź�˾3�·��·�����Ҫ���ſͻ�Ŀ���г��嵥����Ϊ׼���������ϱ����պ���֮���ID��
#          3���������ſͻ���ʶ�����ϱ����¼��ſͻ�Ŀ���г��嵥�������Ѿ����ڵļ��ſͻ���ʶ��
#
#��������: G_A_01005_MONTH.tcl
#��������: ����01005������
#��������: ��
#Դ    ����һ�����������ڼ����·�������
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��  
#�� д �ˣ�liuqf
#��дʱ�䣺2010-06-24
#�����¼��
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
	set sql_buff "DELETE FROM bass1.G_A_01005_MONTH where time_id=$op_month"
    exec_sql $sql_buff




#2011-06-03 17:50:21
#һ���ԣ�
#set sql_buff "
#insert into  BASS1.G_A_01005_MONTH
#select distinct 
#         201105 TIME_ID
#        ,ID
#        ,ENTERPRISE_ID
#from     BASS1.G_I_77780_DAY_DOWN20110429 a
#"
#exec_sql $sql_buff

#������
#set sql_buff "
#insert into  BASS1.G_A_01005_MONTH
#select distinct 
#         $op_month TIME_ID
#        ,ID
#        ,ENTERPRISE_ID
#from     BASS1.boss_interface a
#"
#exec_sql $sql_buff
#
  #���chkpkunique
  set tabname "G_A_01005_MONTH"
  set pk    "ID||ENTERPRISE_ID"
  chkpkunique ${tabname} ${pk} $op_month


	return 0
}
