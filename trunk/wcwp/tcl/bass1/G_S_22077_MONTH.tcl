
######################################################################################################		
#�ӿ�����: �Ʒ�����KPI                                                               
#�ӿڱ��룺22077                                                                                          
#�ӿ�˵������¼�Ʒ�������KPI��Ϣ��
#��������: G_S_22077_MONTH.tcl                                                                            
#��������: ����22077������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-08
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      


  #ɾ����������
	set sql_buff "delete from bass1.G_S_22077_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
		set sql_buff "
			INSERT INTO  BASS1.G_S_22077_MONTH
		select $op_month
		,'$op_month'
		,(select  char(bigint(sum(BASECALL_FEE+TOLL_FEE+INFO_FEE))) 
			from 
			bass2.DW_CALL_$op_month a
			where roamtype_id in (1,4,6,8)	
			and TOLLTYPE_ID in ( 0,1,2,101,102)
			and CALLTYPE_ID = 0
				)
	,(
			select  char(bigint(sum(BASECALL_FEE+TOLL_FEE+INFO_FEE))) 
		from 
		bass2.DW_CALL_$op_month a
		where roamtype_id in (1,4,6,8)	
		and CALLTYPE_ID = 1
			)
	,'0'
	,'0'
	,
	(
	select char(bigint(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE))) from  bass2.dw_newbusi_gprs_$op_month 
	where ROAMTYPE_ID in ( 0,1 )
	)
	,(
		select  char(bigint(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE))) from  bass2.dw_newbusi_gprs_$op_month 
		where ROAMTYPE_ID in ( 4,8 )
		)
	,case when 
	(
	select char(bigint(1.0*sum(CHARGE1+CHARGE2+CHARGE3+CHARGE4)/100)) from  bass2.dw_gprs_l_dm_$op_month 
	where ROAM_TYPE in ( 5,9 )
	) is null then '0' else 
(
	select char(bigint(1.0*sum(CHARGE1+CHARGE2+CHARGE3+CHARGE4)/100)) from  bass2.dw_gprs_l_dm_$op_month 
	where ROAM_TYPE in ( 5,9 )
	) end 
from bass2.dual
with ur
	"
	exec_sql $sql_buff


  #1.���chkpkunique
	set tabname "G_S_22077_MONTH"
	set pk 			"OP_MONTH"
	chkpkunique ${tabname} ${pk} ${op_month}

  aidb_runstats bass1.G_S_22077_MONTH 3

	
	return 0
}

#	
#	
#	���Ա���	��������	��������	��������	��ע
#	00	��¼�к�	Ψһ��ʶ��¼�ڽӿ������ļ��е��кš�	number(8)	
#	01	�·�	��ʽ��YYYYMM	char(6)	����
#	02	�������γ��ù������мƷ�����	ָͳ��������ʡ�����γ��ú�ʡ�����γ���״̬�¹��ڣ����������䣩���в����ļƷ����롣	number(15)	��λ��Ԫ
#	03	�������γ��ñ��мƷ�����	ָͳ��������ʡ�����γ��ú�ʡ�����γ���״̬�����У�������������ʣ����в����ļƷ����롣	number(15)	��λ��Ԫ
#	04	IDC-�����йܼƷ�����	ͳ��������IDC-�����й�ҵ������ļƷ�����,IDC-�����й���ָIDCΪ�ͻ��ṩһ���ġ��ռ䡱�͡����������С��ռ䡱�ǲ��ջ��ܷ������Ĺ��ѡȡ�̶���1U/2U/4U��λ�ռ䣬�ͻ����Լ��������豸���������й������õĿռ��ڡ��ͻ�ӵ�ж��й��豸������Ȩ����ȫ����Ȩ�ޣ��ͻ����а�װ���ϵͳ������ά����	number(15)	��λ��Ԫ
#	05	IDC-���������Ʒ�����	ͳ��������IDC-��������ҵ������ļƷ�����,IDC-��������Ϊ�ͻ�����WEB��վ�ṩ�����������Դ�����������ӡ����������ǰ�һ̨�����ڻ������ϵķ�������Դ��ϵͳ��Դ����������洢�ռ�ȣ�����һ���ı������ֳ�����̨�����⡱�ġ�С��������ÿһ���������������ж�����������ͬһ̨�������ϵĲ�ͬ���������Ǳ˴˶����ģ������ɿͻ����й���	number(15)	��λ��Ԫ
#	06	GPRS����-ʡ�ڼƷ�����	ͳ��������GPRS����-ʡ��ҵ������ļƷ����롣	number(15)	��λ��Ԫ
#	07	GPRS����-ʡ�����γ��üƷ�����	ͳ��������GPRS����-ʡ�����γ���ҵ������ļƷ����롣	number(15)	��λ��Ԫ
#	08	GPRS����-�������γ��üƷ�����	ͳ��������GPRS����-�������γ���ҵ������ļƷ����롣	number(15)	��λ��Ԫ
#	
#	
#02	�������γ��ù������мƷ�����
##	
##	select (BASE_FEE+TOLL_FEE+INFO_FEE)
##	from 
##	bass2.DW_CALL_$op_month a
##	where roamtype_id in (1,4,6,8)	
##	and TOLLTYPE_ID in ( 0,1,2,101,102)
##	and CALLTYPE_ID = 0
##	
##	
##	
##	03	�������γ��ñ��мƷ�����
##	
##	select char((BASE_FEE+TOLL_FEE+INFO_FEE)
##	from 
##	bass2.DW_CALL_$op_month a
##	where roamtype_id in (1,4,6,8)	
##	and CALLTYPE_ID = 1
##	
##	
##	#	04	IDC-�����йܼƷ�����	ͳ��������IDC-�����й�ҵ������ļƷ�����,IDC-�����й���ָIDCΪ�ͻ��ṩһ���ġ��ռ䡱�͡����������С��ռ䡱�ǲ��ջ��ܷ������Ĺ��ѡȡ�̶���1U/2U/4U��λ�ռ䣬�ͻ����Լ��������豸���������й������õĿռ��ڡ��ͻ�ӵ�ж��й��豸������Ȩ����ȫ����Ȩ�ޣ��ͻ����а�װ���ϵͳ������ά����	number(15)	��λ��Ԫ
##	0
##	#	05	IDC-���������Ʒ�����	ͳ��������IDC-��������ҵ������ļƷ�����,IDC-��������Ϊ�ͻ�����WEB��վ�ṩ�����������Դ�����������ӡ����������ǰ�һ̨�����ڻ������ϵķ�������Դ��ϵͳ��Դ����������洢�ռ�ȣ�����һ���ı������ֳ�����̨�����⡱�ġ�С��������ÿһ���������������ж�����������ͬһ̨�������ϵĲ�ͬ���������Ǳ˴˶����ģ������ɿͻ����й���	number(15)	��λ��Ԫ
##	0
##	
##	
##	
##	#	06	GPRS����-ʡ�ڼƷ�����	ͳ��������GPRS����-ʡ��ҵ������ļƷ����롣	number(15)	��λ��Ԫ
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 0,1 )
##	#	07	GPRS����-ʡ�����γ��üƷ�����	ͳ��������GPRS����-ʡ�����γ���ҵ������ļƷ����롣	number(15)	��λ��Ԫ
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 4,8 )
##	
##	#	08	GPRS����-�������γ��üƷ�����	ͳ��������GPRS����-�������γ���ҵ������ļƷ����롣	number(15)	��λ��Ԫ
##	
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 100 )
##	
##	
##	
##	select cast( sum(value(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE,0)) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 100 )
##	
##	select  cast( sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 4,8 )
##	
##	
##	select cast( sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 0,1 )
##	
##	
##	
##	
##	0	����
##	1	ʡ�ڷ�IP��;
##	2	ʡ�ʷ�IP��;
##	101	17951ʡ��IP��;
##	102	17951ʡ��IP��;
##	
##	