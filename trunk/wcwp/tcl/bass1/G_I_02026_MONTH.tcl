
######################################################################################################		
#�ӿ�����: �ʷ��ײͻ�����Ϣ���ʷ�ͳһ���룩                                                               
#�ӿڱ��룺02026                                                                                          
#�ӿ�˵�������ӿڰ���֧��ϵͳ���ߣ�ͣ�ۡ����ۡ����ۣ����ʷ�ȫ����Ϣ������ͣ�۵��ʷѼȰ������û���Ҳ�������û��ġ�
#��������: G_I_02026_MONTH.tcl                                                                            
#��������: ����02026������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120112
#�����¼��
#�޸���ʷ: 1. panzw 20120112	ͳһ�ʷѹ���v1.1 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      
 set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
        global app_name
        set app_name "G_I_02026_MONTH.tcl"    

  #ɾ����������
	set sql_buff "delete from bass1.G_I_02026_MONTH where time_id=$op_month"
	exec_sql $sql_buff


 set sql_buff "ALTER TABLE BASS1.G_I_02026_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
 	exec_sql $sql_buff

								
	set sql_buff "
select count(0)*2 - count(distinct OLD_PKG_ID)-count(distinct NEW_PKG_ID)
from G_I_02026_MONTH_LOAD
with ur
"


chkzero2 $sql_buff "02026 src data pk check not pass!"


#	
#	Ŀǰ����״̬��
#	1-���ۣ��ѻ�������δͶ���г�
#	2-���ۣ���Ͷ���г���չ�û�
#	3-ͣ�ۣ���ֹ�г��ƹ㲻�ٷ�չ���û�
#	
#	
#	select * from   BASS1.ALL_DIM_LKP 
#	where bass1_tbid = 'BASS_STD1_0114'
#	


	set sql_buff "
	insert into bass1.G_I_02026_MONTH_1
		  (
			 TIME_ID
			,PKG_ID
			,BASS2_PKG_ID
			,PKG_NAME
			,PKG_DESC
			,PKG_STS
			,STOP_DT
		  )
	select 
	  $op_month TIME_ID
	  ,value(d.NEW_PKG_ID,a.NEW_PKG_ID) PKG_ID
	  ,a.OLD_PKG_ID
	  ,case when length(b.NAME) > 100 then substr(b.NAME,100) else b.NAME end PKG_NAME
	  ,repeat(' ',600) PKG_DESC
	  ,case when exp_date > '$this_month_last_day' and del_flag = '1' then '2' else '3' end PKG_STS
	  ,exp_date
	from BASS1.G_I_02026_MONTH_LOAD a
	left join (select xzbas_value BASS2_ID, bass1_value QW_QQT_ID from bass1.ALL_DIM_LKP where bass1_tbid in ('BASS_STD1_0114','BASS_STD1_0115') ) c on a.old_pkg_id = c.bass2_id
	left join BASS1.DIM_QW_QQT_PKGID d on c.qw_qqt_id = d.OLD_PKG_ID
	left join (select char(product_item_id) product_item_id,del_flag,NAME
			,replace(char(date(eff_date)),'-','') eff_date
			,replace(char(date(exp_date)),'-','') exp_date  from   bass2.dim_prod_up_product_item 
			where item_type = 'OFFER_PLAN'
		) b on a.old_pkg_id = b.product_item_id
	where b.product_item_id is not null 
	and (substr(a.new_pkg_id,1,4) between '3101' and '3107' or substr(a.new_pkg_id,1,4) = '9999')
	and substr(a.new_pkg_id,5,1) between '1' and '3'
	and substr(a.new_pkg_id,6,1) between '1' and '6'
	and substr(a.new_pkg_id,7,1) between '1' and '7'
	and substr(a.new_pkg_id,8,1) between '1' and '2'
	and substr(a.new_pkg_id,9,3) between '001' and '147'
	and substr(a.new_pkg_id,12,4) between '0000' and '9999'
	and substr(a.new_pkg_id,16,3) between '001' and '999'
	  with ur 
  "     
  exec_sql $sql_buff



	set sql_buff "
	insert into bass1.G_I_02026_MONTH
		  (
			 TIME_ID
			,PKG_ID
			,PKG_NAME
			,PKG_DESC
			,PKG_STS
			,STOP_DT
		  )
	select 
			TIME_ID
			,PKG_ID
			,PKG_NAME
			,PKG_DESC
			,PKG_STS
			,STOP_DT
		from bass1.G_I_02026_MONTH_1
	  with ur
  "     
  exec_sql $sql_buff


  #1.���chkpkunique
	set tabname "G_I_02026_MONTH"
	set pk 			"PKG_ID"
	chkpkunique ${tabname} ${pk} ${op_month}



## ֱ�Ӹ澯��ʾ����װ�أ�
                set grade 2
                set alarmcontent "���鱾��02026ͳһ�ʷѱ���������"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}



	return 0
}

