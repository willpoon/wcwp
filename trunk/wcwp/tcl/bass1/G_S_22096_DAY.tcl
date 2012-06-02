
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
	set curr_month [string range $op_time 0 3][string range $op_time 5 6]
        #������
        global app_name
        set app_name "G_S_22096_DAY.tcl"
	
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22096_DAY where time_id=$timestamp"
	exec_sql $sql_buff


 set sql_buff "alter table bass1.G_S_22096_DAY_1 activate not logged initially with empty table"
	exec_sql $sql_buff

#########################################################################################################

##~   ��վ��
	set sql_buff "
			insert into G_S_22096_DAY_1
			select 
			'01' ECHNL_TYPE
			,case 
			when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			when INDEX_ID1 = 23 then '02'
			when INDEX_ID1 = 10 then '03'
			when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			when INDEX_ID1 = 22 then '08'
			when INDEX_ID1 = 24 then '09'
			--when INDEX_ID1 = 24 then '10' �޸�ҵ��
			when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			when INDEX_ID1 = 25 then '12'
			--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
			--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
			when INDEX_ID1 = 25 then '15'
			when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			when INDEX_ID1 = 27 then '17'
			when INDEX_ID1 = 13 then '18'
			when INDEX_ID1 = 27 then '19'
			when INDEX_ID1 = 17 then '20'
			when INDEX_ID1 = 8  then '21'
			when INDEX_ID1 = 9  then '22'
			when INDEX_ID1 = 7  then '23'
			when INDEX_ID1 = 28 then '24'
			when INDEX_ID1 = 11 then '25'
			when INDEX_ID1 = 19 then '26'
			--when INDEX_ID1 = 24 then '27'  �޸�ҵ��
			--when INDEX_ID1 = 24 then '28'  �޸�ҵ��
			when INDEX_ID1 = 21 then '29'
			else '99' end IMP_VAL_TYPE
			,char(count(0)) OPEN_CNT
			from bass2.dw_product_ord_cust_dm_$curr_month a
			, bass2.dw_product_$timestamp b
			, bass2.dw_product_ord_offer_dm_$curr_month c
			, bass2.DIM_DATA_OFFER d
			where a.product_instance_id = b.user_id 
			and a.ORDER_STATE = 11
			and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
			and c.OFFER_ID = d.OFFER_ID
			and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
			group by case 
			when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			when INDEX_ID1 = 23 then '02'
			when INDEX_ID1 = 10 then '03'
			when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			when INDEX_ID1 = 22 then '08'
			when INDEX_ID1 = 24 then '09'
			--when INDEX_ID1 = 24 then '10' �޸�ҵ��
			when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			when INDEX_ID1 = 25 then '12'
			--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
			--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
			when INDEX_ID1 = 25 then '15'
			when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			when INDEX_ID1 = 27 then '17'
			when INDEX_ID1 = 13 then '18'
			when INDEX_ID1 = 27 then '19'
			when INDEX_ID1 = 17 then '20'
			when INDEX_ID1 = 8  then '21'
			when INDEX_ID1 = 9  then '22'
			when INDEX_ID1 = 7  then '23'
			when INDEX_ID1 = 28 then '24'
			when INDEX_ID1 = 11 then '25'
			when INDEX_ID1 = 19 then '26'
			when INDEX_ID1 = 21 then '29'
			else '99' end
			"
	exec_sql $sql_buff


	set sql_buff "
			insert into G_S_22096_DAY_1
		select 
		'01' ECHNL_TYPE
		,case 
		when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		else '99' end IMP_VAL_TYPE
		,char(count(0)) OPEN_CNT
		from bass2.dw_product_ord_cust_dm_$curr_month a
		, bass2.dw_product_$timestamp b
		, bass2.dw_product_ord_offer_dm_$curr_month c
		, bass2.DIM_DATA_SRVPKG d
		where a.product_instance_id = b.user_id 
		and a.ORDER_STATE = 11
		and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
		and c.OFFER_ID = bigint('11'||substr(char(d.SRVPKG_ID),3))
		and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
		group by case 
		when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		else '99' end
			"
	exec_sql $sql_buff

#########################################################################################################
##~   10086
	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'02' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  �޸�ҵ��
--when INDEX_ID1 = 24 then '28'  �޸�ҵ��
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where  ( a.op_id =  91000038 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%�ͷ�%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'02' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where    ( a.op_id =  91000038 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%�ͷ�%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff


#########################################################################################################

#########################################################################################################
##~  ����Ӫҵ��
	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'03' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  �޸�ҵ��
--when INDEX_ID1 = 24 then '28'  �޸�ҵ��
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where   ( a.op_id =  10000047 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'03' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where    ( a.op_id =  10000047 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff
#########################################################################################################
##~   wap����

#########################################################################################################
##~   05:�����ն�
	set sql_buff "
insert into G_S_22096_DAY_1
select 
'05' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  �޸�ҵ��
--when INDEX_ID1 = 24 then '28'  �޸�ҵ��
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where ( a.op_id =  10100101 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' �޸�ҵ��
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
insert into G_S_22096_DAY_1
select 
'05' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%�ͷ�%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where   ( a.op_id =  10100101 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff



##~  �����նˣ�
	##~   set sql_buff "
			##~   insert into G_S_22096_DAY_1
			##~   select 
			##~   '05' ECHNL_TYPE
			##~   ,case 
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			##~   when INDEX_ID1 = 23 then '02'
			##~   when INDEX_ID1 = 10 then '03'
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			##~   when INDEX_ID1 = 22 then '08'
			##~   when INDEX_ID1 = 24 then '09'
			##~   --when INDEX_ID1 = 24 then '10' �޸�ҵ��
			##~   when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			##~   when INDEX_ID1 = 25 then '12'
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
			##~   when INDEX_ID1 = 25 then '15'
			##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			##~   when INDEX_ID1 = 27 then '17'
			##~   when INDEX_ID1 = 13 then '18'
			##~   when INDEX_ID1 = 27 then '19'
			##~   when INDEX_ID1 = 17 then '20'
			##~   when INDEX_ID1 = 8  then '21'
			##~   when INDEX_ID1 = 9  then '22'
			##~   when INDEX_ID1 = 7  then '23'
			##~   when INDEX_ID1 = 28 then '24'
			##~   when INDEX_ID1 = 11 then '25'
			##~   when INDEX_ID1 = 19 then '26'
			##~   --when INDEX_ID1 = 24 then '27'  �޸�ҵ��
			##~   --when INDEX_ID1 = 24 then '28'  �޸�ҵ��
			##~   when INDEX_ID1 = 21 then '29'
			##~   else '99' end IMP_VAL_TYPE
			##~   ,char(count(0)) OPEN_CNT
			##~   from bass2.dw_product_ord_cust_dm_$curr_month a
			##~   , bass2.dw_product_$timestamp b
			##~   , bass2.dw_product_ord_offer_dm_$curr_month c
			##~   , bass2.DIM_DATA_OFFER d
			##~   where a.product_instance_id = b.user_id 
			##~   and a.ORDER_STATE = 11
			##~   and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
			##~   and c.OFFER_ID = d.OFFER_ID
			##~   and upper(a.channel_type) = '9' and    a.op_time = '$op_time' 
			##~   group by case 
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			##~   when INDEX_ID1 = 23 then '02'
			##~   when INDEX_ID1 = 10 then '03'
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			##~   when INDEX_ID1 = 22 then '08'
			##~   when INDEX_ID1 = 24 then '09'
			##~   --when INDEX_ID1 = 24 then '10' �޸�ҵ��
			##~   when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			##~   when INDEX_ID1 = 25 then '12'
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
			##~   when INDEX_ID1 = 25 then '15'
			##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			##~   when INDEX_ID1 = 27 then '17'
			##~   when INDEX_ID1 = 13 then '18'
			##~   when INDEX_ID1 = 27 then '19'
			##~   when INDEX_ID1 = 17 then '20'
			##~   when INDEX_ID1 = 8  then '21'
			##~   when INDEX_ID1 = 9  then '22'
			##~   when INDEX_ID1 = 7  then '23'
			##~   when INDEX_ID1 = 28 then '24'
			##~   when INDEX_ID1 = 11 then '25'
			##~   when INDEX_ID1 = 19 then '26'
			##~   when INDEX_ID1 = 21 then '29'
			##~   else '99' end
			##~   "
	##~   exec_sql $sql_buff


	##~   set sql_buff "
			##~   insert into G_S_22096_DAY_1
		##~   select 
		##~   '05' ECHNL_TYPE
		##~   ,case 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		##~   else '99' end IMP_VAL_TYPE
		##~   ,char(count(0)) OPEN_CNT
		##~   from bass2.dw_product_ord_cust_dm_$curr_month a
		##~   , bass2.dw_product_$timestamp b
		##~   , bass2.dw_product_ord_offer_dm_$curr_month c
		##~   , bass2.DIM_DATA_SRVPKG d
		##~   where a.product_instance_id = b.user_id 
		##~   and a.ORDER_STATE = 11
		##~   and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
		##~   and c.OFFER_ID = bigint('11'||substr(char(d.SRVPKG_ID),3))
		##~   and upper(a.channel_type) = '9' and    a.op_time = '$op_time' 
		##~   group by case 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		##~   else '99' end
			##~   "
	##~   exec_sql $sql_buff


####################################################################################################


 	set sql_buff "
		insert into G_S_22096_DAY
		(
				 TIME_ID
				,OP_TIME
				,ECHNL_TYPE
				,IMP_VAL_TYPE
				,OPEN_CNT
		)
		select 
				$timestamp TIME_ID
				,'$timestamp' OP_TIME
				,ECHNL_TYPE
				,IMP_VAL_TYPE
				,OPEN_CNT
		from G_S_22096_DAY_1
		where  IMP_VAL_TYPE <> '99'
		with ur
"

	exec_sql $sql_buff


  #1.���chkpkunique
  set tabname "G_S_22096_DAY"
  set pk   "OP_TIME||ECHNL_TYPE||IMP_VAL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.G_S_22066_DAY 3


	return 0
}

##~   ���ֶν�ȡ���·��ࣺ
##~   01:�Ż���վ
##~   02:10086�绰Ӫҵ��
##~   03: ����Ӫҵ��
##~   04:WAP��վ
##~   05:�����նˣ��������е������նˣ�������ʵ��������24СʱӪҵ���ڲ��ŵ������նˣ��������̳��ȳ��������ڷŵ������նˡ���



					
##~   TIME_ID                        SYSIBM    INTEGER                   4     0 Yes   
##~   OP_TIME                        SYSIBM    CHARACTER                 8     0 Yes   
##~   ECHNL_TYPE                     SYSIBM    CHARACTER                 2     0 Yes   
##~   IMP_VAL_TYPE                   SYSIBM    CHARACTER                 2     0 Yes   
##~   OPEN_CNT                       SYSIBM    CHARACTER                10     0 Yes   

					
					
					 
##~   case 
##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
##~   when INDEX_ID1 = 23 then '02'
##~   when INDEX_ID1 = 10 then '03'
##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
##~   when INDEX_ID1 = 22 then '08'
##~   when INDEX_ID1 = 24 then '09'
##~   --when INDEX_ID1 = 24 then '10' �޸�ҵ��
##~   when INDEX_ID1 = 24 then '11'
##~   when INDEX_ID1 = 25 then '12'
##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' �������
##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' �������
##~   when INDEX_ID1 = 25 then '15'
##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
##~   when INDEX_ID1 = 27 then '17'
##~   when INDEX_ID1 = 13 then '18'
##~   when INDEX_ID1 = 27 then '19'
##~   when INDEX_ID1 = 17 then '20'
##~   when INDEX_ID1 = 8  then '21'
##~   when INDEX_ID1 = 9  then '22'
##~   when INDEX_ID1 = 7  then '23'
##~   when INDEX_ID1 = 28 then '24'
##~   when INDEX_ID1 = 11 then '25'
##~   when INDEX_ID1 = 19 then '26'
##~   --when INDEX_ID1 = 24 then '27'  �޸�ҵ��
##~   --when INDEX_ID1 = 24 then '28'  �޸�ҵ��
##~   when INDEX_ID1 = 21 then '29'
##~   else '99' end 


##~   01:��������  INDEX_ID1 = 15 and INDEX_ID2 = 71
##~   02:��������  INDEX_ID1 = 23
##~   03:�Ų��ܼ�  INDEX_ID1 = 10  
##~   04:���Ż�ִ  INDEX_ID1 = 15 and INDEX_ID2 = 75
##~   05:��Ϣ�ܼ�  -
##~   06:���Ż�Ա  INDEX_ID1 = 20 and INDEX_ID2 = 88
##~   07:139�����շѰ� INDEX_ID1 = 12 and INDEX_ID2 in (58,93)
##~   08:�ֻ�֤ȯ  INDEX_ID1 = 22 
##~   09:�ֻ��̽�  INDEX_ID1 = 24
##~   10:blackberry -
##~   11:�������־��ֲ� INDEX_ID1 = 6 and INDEX_ID2 = 26
##~   12:�ֻ�����  INDEX_ID1 = 25
##~   --13:����		- bass2.DIM_DATA_SRVPKG  INDEX_ID1 = 4 and INDEX_ID2 = 19
##~   --14:��ý����� - bass2.DIM_DATA_SRVPKG  INDEX_ID1 = 4 and INDEX_ID2 = 20
##~   15:���������� INDEX_ID1 = 25
##~   16:��������   INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29)
##~   17:�ֻ�����(���ֹ㲥��ʽ)    INDEX_ID1 = 15 and INDEX_ID2 = 27
##~   18:�ֻ�ҽ��    INDEX_ID1 = 13
##~   19:�ֻ���ͼ	   INDEX_ID1 = 27
##~   20:�ֻ�����		INDEX_ID1 = 17
##~   21:��Ѷ			INDEX_ID1 = 8
##~   22:�ֻ��Ķ�		INDEX_ID1 = 9
##~   23:�ֻ���		INDEX_ID1 = 7
##~   24:�����������ֲ� INDEX_ID1 = 28
##~   25:�ֻ���Ƶ		INDEX_ID1 = 11
##~   26:�ֻ���Ϸ		INDEX_ID1 = 19
##~   27:����		-
##~   28:�ƶ�Ӧ���̳�  -
##~   29:12580�����  INDEX_ID1 = 21





##~   CREATE TABLE "BASS1   "."G_S_22096_DAY_1"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "OP_TIME" CHAR(8) , 
                  ##~   "ECHNL_TYPE" CHAR(2) , 
                  ##~   "IMP_VAL_TYPE" CHAR(2) , 
                  ##~   "OPEN_CNT" CHAR(10) )   
                 ##~   DISTRIBUTE BY HASH("OP_TIME",  
                 ##~   "ECHNL_TYPE",  
                 ##~   "IMP_VAL_TYPE")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

