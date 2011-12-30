######################################################################################################
#�ӿ����ƣ����ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ
#�ӿڱ��룺01007
#�ӿ�˵���� 
#��������: G_A_01007_DAY.tcl
#��������: ����01007������
#��������: ��
#Դ    �� 
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��  
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-11-9 16:05:01
#�����¼��
#�޸���ʷ: 2011.11.30 boss����Դ�������ƣ������ϱ��˽ӿ�����
#�ӿڵ�Ԫ���ƣ����ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ
#�ӿڵ�Ԫ���룺01007
#�ӿڵ�Ԫ˵����1�����ӿ��ϱ�����Ŀ���г��嵥���ſͻ�ID��֧��ϵͳ�ڼ��ſͻ���ʶ֮��Ķ�Ӧ��ϵ��Ϣ����01006�ӿ���01004�ӿڼ��ż�Ķ�Ӧ��ϵ��
#              2�����ӿ�����������ʽ�����ϴ����״��ϱ�ȫ����Ӧ��ϵ��
#              3��ʡ��˾�ϱ����嵥����ID�뼯�ſͻ���ʶ��Ӧ��ϵ��ֻ���ϱ�һ��һ��һ�Զ���߶��һ��Ӧ��ϵ�������ϱ���Զ�Ķ�Ӧ��ϵ��
#
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    set sql_buff "DELETE FROM bass1.G_A_01007_DAY where time_id=$timestamp"
    exec_sql $sql_buff

#01006	Ŀ���г��嵥�ͻ�	ÿ������	ÿ��11��ǰ	2
#01007	���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ	ÿ������	ÿ��11��ǰ	2
# �����½�������
set sql_buff "
	insert into G_A_01007_DAY
	select  distinct 
	$timestamp TIME_ID
	, ENTERPRISE_ID
	, CUST_ID
	,'1' RELA_STATE
	from (
		select 
		qd_group_id ENTERPRISE_ID
		,zw_group_id CUST_ID
		from bass2.dw_ent_group_relation_$timestamp  a
		except
		select ENTERPRISE_ID,CUST_ID
		from (
		select ENTERPRISE_ID,CUST_ID ,row_number()over(partition by ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
		from G_A_01007_DAY
		)  t where rn = 1
	    ) o
	with ur
"

#exec_sql $sql_buff

# ��������ϵ����
# bass2.dw_ent_group_relation_$timestamp ��û�е���Ϊ�ѽ��

set sql_buff "
	insert into G_A_01007_DAY
	select  distinct 
	$timestamp TIME_ID
	, ENTERPRISE_ID
	, CUST_ID
	,'2' RELA_STATE
	from (

		select ENTERPRISE_ID,CUST_ID
		from (
		select ENTERPRISE_ID,CUST_ID ,RELA_STATE,row_number()over(partition by ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
		from G_A_01007_DAY
		)  t where rn = 1 and RELA_STATE = '1'
	except
	select 
		qd_group_id ENTERPRISE_ID
		,zw_group_id CUST_ID
		from bass2.dw_ent_group_relation_$timestamp  a
	    ) o
	with ur
"

#exec_sql $sql_buff



  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_A_01007_DAY"
  set pk   "ENTERPRISE_ID||CUST_ID"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.$tabname 3
  
           
##	set sql_buff "DELETE FROM bass1.G_A_01007_DAY where time_id=$op_month"
##	puts $sql_buff
##    exec_sql $sql_buff
##
##
##
##	set sql_buff "insert into bass1.G_A_01007_DAY
##
##	              "
##
##  puts $sql_buff
##  exec_sql $sql_buff
##
#2011-05-31 19:45:00 һ���� for R194

#set sql_buff "
#insert into G_A_01007_DAY select * from   G_A_01007_DAY_T1
#"
#exec_sql $sql_buff
#

	return 0
}
