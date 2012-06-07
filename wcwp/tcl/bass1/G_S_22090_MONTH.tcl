
######################################################################################################		
#�ӿ�����: ������ֵҵ�����˷Ѻ��֤�»���                                                               
#�ӿڱ��룺22090                                                                                          
#�ӿ�˵�����ɼ�������ֵҵ�����˷ѣ����֤�������±����ݣ����ְ���ҵ��͵㲥ҵ��
#��������: G_S_22090_MONTH.tcl                                                                            
#��������: ����22090������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120507
#�����¼��
#�޸���ʷ: 1. panzw 20120507	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.0) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

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
	set sql_buff "delete from bass1.G_S_22090_MONTH where time_id=$op_month"
	exec_sql $sql_buff
		set sql_buff "
			INSERT INTO  BASS1.G_S_22090_MONTH
			( 
				 TIME_ID
				,OP_TIME
				,BUSI_CODE
				,BUSI_NAME
				,BILLING_TYPE
				,SP_CODE
				,SP_NAME
				,ONLINE_CNT
				,TUIFEI_CNT
				,TUIFEI
			)
			 
select 
				   $op_month TIME_ID
				,	'$op_month' OP_TIME        
				,	EXTEND_ID2 BUSI_CODE
				,	b.name		BUSI_NAME
				,	'10' BILLING_TYPE
				,	' ' SP_CODE
				,	' ' SP_NAME
				,	char(count(distinct a.product_instance_id)) ONLINE_CNT
				,	'0' TUIFEI_CNT
				,	'0' TUIFEI
from bass2.Dw_product_ins_off_ins_prod_$op_month a 
,bass2.dim_prod_up_product_item  b
, bass2.DIM_DATA_OFFER c
where  a.state =1     
    and  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6)  <= '$op_month'
    and  substr(replace(char(date(a.expire_date)),'-',''),1,6) >= '$op_month' 
    and a.offer_id = b.PRODUCT_ITEM_ID
    and b.PRODUCT_ITEM_ID = c.OFFER_ID
    and SUPPLIER_ID is not null
    and a.offer_id  in (113110166392,113110146314,113090000006)
	group by  EXTEND_ID2,b.name,SUPPLIER_ID			
with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22090_MONTH 3

  #1.���chkpkunique
	set tabname "G_S_22090_MONTH"
	set pk 			"OP_TIME||BUSI_CODE||SP_CODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	
	return 0
}
