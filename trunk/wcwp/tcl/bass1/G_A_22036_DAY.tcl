#=====================================================================*
#  G_A_22036_DAY.tcl                              *
#���ſͻ��˿���Դʹ�ÿ�չ���
#  bass2.dim_bs_enterprise_customer_code                               *
#  bass2.DW_ENTERPRISE_SUB_yyyymm                                      *
#	 bass2.dw_newbusi_ismg_yyyymm                                        *
#	 BASS2.DIM_NEWBUSI_SPINFO                                            *
#*�������ͣ���                                                          *
#*����������22036�ӿڳ���                                               *
#������������¼��ʡ�ĺ�����Դ��Ϣ                                        *
#�������̣�int -s G_A_22036_DAY.tcl                                  *
#�������ڣ�2007-08-23                                                  *
#�� �� �ߣ�xiahuaxue                                                   *
#�޸���ʷ��1.5.3�汾��22036��Ϊ������ҵӦ��ҵ�����ӿ� 2008-03-27         *
#	20111027 �޸Ľӿ�22036�����ſͻ��˿���Դʹ�ÿ�չ�����������Ϊ�����ſͻ��˿���Դʹ���������
#�����ֶ�"����ҵ������"������ͨ���ڡ�����״̬�����ӿڴ��䷽ʽ��Ϊ�������ӿڡ�
#�ӿڵ�Ԫ˵�������ӿ�������ͳ��֧�Ÿ�ʡ��˾���ſͻ���SIռ�û�ʹ���ҹ�˾�˿ں�����Դ��GPRSר��APN�����úͷ����������ʡ��˾�ϱ��ġ���ҵӦ�ô���ȫ�롱�Լ���APN�����ʶ����Ӧ��������Ӧ�����еķ�������APN�����ʶ����һ�¡�
#======================================================================*

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
	    global env
      global handle

       

	     #���� yyyymmdd

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   

        

        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

			 #����  yyyymm

			 set last_month [GetLastMonth [string range $op_month 0 5]]

			 #������ yyyymm

			 set last_last_month [GetLastMonth [string range $last_month 0 5]]




set curr_month [string range $op_time 0 3][string range $op_time 5 6]

	set sql_buff "delete from bass1.G_A_22036_DAY where TIME_ID < 20111101"
  exec_sql $sql_buff
  
  #ɾ����������
	set sql_buff "delete from bass1.G_A_22036_DAY where TIME_ID= $timestamp"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  


  
  set sql_buff "insert into BASS1.G_A_22036_DAY
                select a.TIME_ID, a.BILL_MONTH, a.CUST_TYPE, a.EC_CODE, a.SINAME, 
                       a.OPERATE_TYPE, a.APP_LENCODE, a.APNCODE ,service_name ,open_date,sts
                from
                (
select distinct 
 $timestamp   			TIME_ID,
'$curr_month'         	 BILL_MONTH,
'0'                  	CUST_TYPE,
cust_id            		EC_CODE,
''                   	SINAME,        
case 
when ATTR_3 like 'QXZ%' then '1'
when ATTR_3 like 'M%' then '1'
else '2'
end as 					OPERATE_TYPE,
ATTR_2      			APP_LENCODE,
'' 						APNCODE
,name 					SERVICE_NAME
,replace(char(date(VALID_DATE)),'-','') open_date
,case when replace(char(date(EXPIRE_DATE)),'-','') > '$timestamp' then '1' 
	else '2' end  		sts
from 
(select a.*,b.name,c.cust_id
,row_number()over(partition by a.ATTR_2 order by a.EXPIRE_DATE desc , a.VALID_DATE desc ) rn 
from bass2.Dw_product_ins_srv_ds  a
, (
select product_item_id ,name from   bass2.dim_prod_up_product_item
where platform_id = 2
and item_type = 'SERVICE'
) b
,bass2.dw_product_$timestamp c
where  a.attr_2 like '106%'
and a.VALID_DATE <='$op_time'
and a.EXPIRE_DATE >='$op_time'
and a.service_id = b.product_item_id
and a.product_instance_id = c.user_id 
) t where t.rn =1 
) a with ur
"  
    exec_sql $sql_buff      


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_A_22036_DAY"
        set pk                  "APP_LENCODE||APNCODE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  
  
  
return 0

    
}

