######################################################################################################
#�ӿ����ƣ�ʹ��TD����Ŀͻ��ջ���
#�ӿڱ��룺22201
#�ӿ�˵������¼ÿ��ʹ��TD����Ŀͻ�������Ϣ���Լ�TDר�úŶεĿͻ�������Ϣ
#��������: G_S_22201_DAY.tcl
#��������: ����22201������
#��������: ��
#Դ    ��1.bass2.dw_product_td_yyyymmdd
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-04-28
#�����¼��MTL_TD_USAGE_MARK char(10), MTL_TD_DATACARD_MARK char(10)
#�޸���ʷ: liuzhilong  20090629 ����1.6.0�޸�"ʹ��TD������ֻ��ͻ���","ʹ��TD��������ݿ��ͻ���","�����ںϵ����ݿ��ͻ���"�ֶγ����߼� ȥ���������ͻ�
#          ���� 6���ֶ� "ʹ��TD������������ͻ���","ʹ��TD�������Ϣ���ͻ���","�����ۼ�ʹ��TD�������Ϣ���ͻ���","������������Ϣ���ͻ���","�����ۼ���������Ϣ���ͻ���","��Ϣ���ͻ�������"
#           liuzhilong 20090702 ȥ����������ֶ�
#          2009-09-18 �޸ġ�ʹ��TD������ֻ��ͻ�����ҵ��ھ�������TD��У��
#          2009-12-22 �޸ģ�TDר�úŶοͻ���-ͳ��������ר�úŶε������ͻ���Ŀǰר�úŶ�Ϊ188�ŶΡ������ʼ�֪ͨ��
#          2010-01-13 �����ۼ�ʹ��TD������ֻ��ͻ��� �ھ��޸ģ�ͨ���˲鼯�ŵĵ��鷢�ִ�����,�ۼ�ָ��͵���ָ��ھ�һ��
#          2010-01-20 �޸Ŀͻ��������ݡ������ͻ����Ŀھ� userstatus_id in (1,2,3,6,8)
#          1.6.5�淶 �޳� ��ģ�ն˵�ר�úŶ�147345��147349����
#          2011.12.27 ����  ȡ 188 û���ų� 2G���ݿ�������R111��bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)��ͨ�����������ݿ��� LINE��88   and td_2gcard_mark=0
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22201_day where time_id=$timestamp"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� �����ͻ���  
	set sql_buff "insert into BASS1.G_S_22201_DAY values
                 ($timestamp,           
                 '$timestamp',          
                (
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  
),
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)
  
),  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_video_mark =1
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and td_3gbook_mark=0  
  
 ),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and product_no like '188%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0) 
  
 ) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and product_no like '157%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)  
  
) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8) 
  and usertype_id in (1,2,9)   
  and td_3gcard_mark =0
  and td_2gcard_mark=0
  and product_no not like '157%'
  and product_no not like '188%'
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)  
  
) ,  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (((td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and td_3gcard_mark=1)
  or (td_gprs_mark=1 and td_2gcard_mark=1)) 
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=0
  
),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=1
  and product_no like '157%'
  and test_mark=0
  and td_3gbook_mark=0  
  
),  
(  
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where (td_call_mark =1
  or td_gprs_mark =1
  or td_addon_mark=1
  )
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=1
  and product_no like '147%'  
  and test_mark=0
  and td_3gbook_mark=0  

),  
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where td_gprs_mark =1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=1
  and test_mark=0
  and td_3gbook_mark = 0

),  
(select value(char(count(distinct user_id)),'0') 
from bass2.dw_product_td_$timestamp
where td_gprs_mark =1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=1
  and bigint(product_no) not between 14734500000 and 14734999999
),
'0',
( 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '188%'
  and test_mark=0
  
 ),  
(  

select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '188%'
  and test_mark=0
  
),  
(   
 
select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and product_no like '147%'  
  and test_mark=0
  
  ),
 (select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where mtl_td_usage_mark=1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and td_3gcard_mark=0
  and td_2gcard_mark=0
  and test_mark=0
  and not ((td_3gbook_mark = 1 and td_gprs_mark=1) and td_call_mark = 0 and td_addon_mark = 0)
  ) ,
  (select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where mtl_td_datacard_mark=1
  and userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and td_3gbook_mark=0
  ) ,
  '0',
(select value(char(count(distinct user_id)),'0') from bass2.dw_product_td_$timestamp
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and mtl_td_3gbook_mark=1
  and bigint(product_no) not between 14734500000 and 14734999999
)
  
)
with ur
;"
                        
  puts $sql_buff
  exec_sql $sql_buff
  
	#	#R111��  �Զ���R111У��
	#	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)
	#	from BASS1.G_S_22201_DAY
	#	where time_id=$timestamp
	#	with ur"
	#	
	#	#	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	#	set DEC_RESULT_VAL1 [get_single $sql_buff]
	#	puts $DEC_RESULT_VAL1
	#	
	#	
	#	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
	#	set sql_buff "
				#update BASS1.G_S_22201_DAY
				#set TD_188_CNT =char( bigint(TD_188_CNT)+(${DEC_RESULT_VAL1} ))
				#where time_id=$timestamp
				#"
	#	exec_sql $sql_buff
	#	}
	#	
	
	return 0
}


#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------



