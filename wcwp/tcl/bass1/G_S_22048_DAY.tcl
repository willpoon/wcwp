
######################################################################################################		
#�ӿ�����: "���г�ֵ��ҵ���ջ���"                                                               
#�ӿڱ��룺22048                                                                                          
#�ӿ�˵����"ͳ�����о߱����г�ֵ���ܵ��������"
#��������: G_S_22048_DAY.tcl                                                                            
#��������: ����22048������
#��������: DAY
#Դ    ��1.BASS2_Dw_agent_acc_info_ds.tcl	���г�ֵ�ʺ���Ϣ
#Դ    ��2.BASS2_Dwd_channel_dept_ds.tcl	ODS-DWD ������֯��Ϣ[I04103]
#Դ    ��3.$dw_acct_payment_dm_yyyymm c
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110727
#�����¼��
#�޸���ʷ: 1. panzw 20110727	1.7.4 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set curr_month [string range $op_time 0 3][string range $op_time 5 6]
      
    #�ϸ��� yyyymm
            global app_name

		set app_name "G_S_22048_DAY.tcl"        

  #ɾ����������
	set sql_buff "delete from bass1.G_S_22048_DAY where time_id=$timestamp"
	exec_sql $sql_buff

#       TIME_ID                 INTEGER(4)          
#       OP_TIME                 CHARACTER(8)        
#       CHRG_NBR                CHARACTER(11)       
#       CMCC_BRANCH_ID          CHARACTER(5)        
#       CHANNEL_ID              CHARACTER(40)       
#       CHRG_WAY_TYPE           CHARACTER(1)        
#       CHRG_CNT                CHARACTER(8)        
#       CHRG_AMT                CHARACTER(14)       
#       CZ_CNT                  CHARACTER(8)        
#       CZ_AMT                  CHARACTER(14)    
#

#GJFK	�����̿��г�ֵ�ۼ�	1
#GTFK	�����̿��г�ֵ�ۼ�����	������
#GTFG	���г�ֵ����	1

	set sql_buff "
insert into G_S_22048_DAY
select 
	$timestamp TIME_ID
	,replace(char(OP_TIME),'-','') OP_TIME
	,a.mobile_id CHRG_NBR
	,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(b.CHANNEL_CITY)),'13101')   CMCC_BRANCH_ID
	,char(a.channel_id) CHANNEL_ID
	,case when b.channel_type=90102 
		 and b.dept_type_dtl in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
	else '1' end CHRG_WAY_TYPE
	,char(int(sum(case when c.opt_code = 'GJFK' then 1 else 0 end ))) CHRG_CNT
	,char(int(sum(case when c.opt_code = 'GJFK' then c.balance+c.amount else 0 end ))) CHRG_AMT
	,char(int(sum(case when d.opt_code = 'GTFG' then 1 else 0 end ))) CZ_CNT
	,char(int(sum(case when d.opt_code = 'GTFG' then d.amount else 0 end ))) CZ_AMT
from bass2.dw_agent_acc_info_$timestamp a
join bass2.dwd_channel_dept_$timestamp  b on a.channel_id = b.organize_id
join bass2.dw_acct_payment_dm_$curr_month c on  a.mobile_id=c.key_num
left join (select PAYMENT_ID,OPT_CODE,AMOUNT,BALANCE 
		from bass2.dw_acct_payment_dm_$curr_month
		where  opt_code = 'GTFG' 
	  ) d  on c.PAYMENT_ID=d.PAYMENT_ID 
where c.opt_code in ('GJFK','GTFK')
and c.op_time = '$op_time'
group by replace(char(OP_TIME),'-','')
	,a.mobile_id 
	,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(b.CHANNEL_CITY)),'13101')   
	,char(a.channel_id) 
	,case when b.channel_type=90102 
	and b.dept_type_dtl in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
	else '1'
	end       
  "
	exec_sql $sql_buff

	return 0
}
