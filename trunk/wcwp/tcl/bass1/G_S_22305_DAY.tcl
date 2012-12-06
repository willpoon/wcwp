
######################################################################################################		
#�ӿ�����: �ƶ�400ҵ�����ջ���                                                               
#�ӿڱ��룺22305                                                                                          
#�ӿ�˵�������ӿ��ϱ��ƶ�400ҵ���ҵ������ʹ�������
#��������: G_S_22305_DAY.tcl                                                                            
#��������: ����22305������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120801
#�����¼��
#�޸���ʷ: 1. panzw 20120801	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.2) 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
	##~   set i 1
##~   # ���������������� , $i<= n   ,  n Խ��Խ��Զ
	##~   while { $i<=100 } {
	        ##~   set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time >= "2012-07-01" } {
	##~   puts $op_time
	##~   Deal_22305_day $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
	Deal_22305_day $op_time $optime_month


return 0
}



proc Deal_22305_day { op_time optime_month } {

	      #���� yyyymmdd

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        #���� yyyy-mm-dd

        set optime $op_time

        #��������ڣ���ʽdd(��������20070411 ����11)

        set today_dd [string range $op_time 8 9]

        #���� yyyymm

        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        #ɾ����������

	set sql_buff "delete from bass1.G_S_22305_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
	set sql_buff "
insert into G_S_22305_DAY
select 
        $timestamp TIME_ID
        ,a.cust_id ENTERPRISE_ID
        ,a.product_no NUMBER400
        ,char(value(UPMESSAGE,0))
        ,char(value(DOWNMESSAGE,0))
        ,char(LOCAL_IN_COUNTS)
        ,char(CHANG_IN_COUNTS)
        ,char(LOCAL_OUT_COUNTS)
        ,char(CHANG_OUT_COUNTS)
        ,char(LOCAL_IN_DUR)
        ,char(CHANG_IN_DUR)
        ,char(LOCAL_OUT_DUR)
        ,char(CHANG_OUT_DUR)
		from bass2.dw_product_$timestamp  a
		 join table(
		select 
			PRODUCT_NO
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID  = 0 then CALL_COUNTS else 0 end) LOCAL_IN_COUNTS
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID <> 0 then CALL_COUNTS else 0 end) CHANG_IN_COUNTS
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID  = 0 then CALL_COUNTS else 0 end) LOCAL_OUT_COUNTS
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID <> 0 then CALL_COUNTS else 0 end) CHANG_OUT_COUNTS
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID  = 0 then CALL_DURATION_M else 0 end) LOCAL_IN_DUR
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID <> 0 then CALL_DURATION_M else 0 end) CHANG_IN_DUR
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID  = 0 then CALL_DURATION_M else 0 end) LOCAL_OUT_DUR
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID <> 0 then CALL_DURATION_M else 0 end) CHANG_OUT_DUR
			from bass2.dw_call_$timestamp			
			where product_no like '4001%'
			group by PRODUCT_NO
		) b on a.product_no = b.PRODUCT_NO
		left join table(		
			select PRODUCT_NO
					,sum(case when  CALLTYPE_ID = 0 then  COUNTS else 0 end) UPMESSAGE
					,sum(case when  CALLTYPE_ID = 0 then  COUNTS else 0 end) DOWNMESSAGE
			from bass2.dw_newbusi_sms_$timestamp
			where product_no like '4001%'
			group by PRODUCT_NO
		) c on a.product_no = c.PRODUCT_NO
		where USERTYPE_ID = 8
		and USERSTATUS_ID in (1,2,3,6,8)
		and a.test_mark = 0
		with ur
		
"		
	exec_sql $sql_buff


set sql_buff "
select count(0)
FROM (
select ENTERPRISE_ID
from G_S_22305_DAY WHERE TIME_ID = $timestamp
EXCEPT
select ENTERPRISE_ID FROM G_A_01004_DAY 
) T

"

chkzero2 $sql_buff "invalid ENTERPRISE_ID !"
	return 0      


}

		




##~   select PRODUCT_NO
        ##~   ,sum(case when  CALLTYPE_ID = 0  COUNTS else 0 end) UPMESSAGE
        ##~   ,sum(case when  CALLTYPE_ID = 0  COUNTS else 0 end) DOWNMESSAGE
##~   from bass2.dw_newbusi_sms_20120801
##~   where product_no like '4001%'
##~   group by PRODUCT_NO

