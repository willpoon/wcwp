######################################################################################################
#�ӿ����ƣ�TD�ͻ��������»���
#�ӿڱ��룺22207
#�ӿ�˵������¼TD�ͻ�����������Ϣ
#��������: G_S_22207_MONTH.tcl
#��������: ����22207������
#��������: ��
#Դ    ��1.bass2.dw_product_td_yyyymm 2.bass2.dw_acct_shoulditem_yyyymmdd
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xiahuaxue
#��дʱ�䣺2009-03-09
#�����¼��
#�޸���ʷ: liuzhilong 1.6.0�淶�������һ���ֶ�. Ŀǰ�޴�ҵ��.��0
#          2010-01-24 �޸Ŀͻ��������ݡ������ͻ����Ŀھ� userstatus_id in (1,2,3,6,8)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        puts $op_month
              

        #���µ�һ�� yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22207_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� �����ͻ���  
	set sql_buff "insert into bass1.g_s_22207_month
                values( $op_month,'$op_month'  ,
                
                (
select
value(char(bigint(sum(a.fact_fee*100))),'0') a02
 from  bass2.dw_acct_shoulditem_$op_month a
 inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id
),
(

select
value(char(bigint(sum(a.fact_fee*100))),'0') a03
 from  bass2.dw_acct_shoulditem_$op_month a
 inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id

  ),
(        

select
value(char(bigint(sum(fact_fee*100))),'0') a04
 from  
   bass2.dw_acct_shoulditem_$op_month
 where 
 (
   (item_id not in (80000027,80000032,80000033,80000101)
   and substr(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(item_id)),'0901'),1,2) in ('05','06','07')    
   )
   or  item_id in (80000479	 ,80000480 ,80000465 ,80000477 ,80000476 ,80000478 ,80000454 ,80000455 ,80000483,
80000484,80000485)
 )
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)
 
),
(


select
value(char(bigint(sum(fact_fee*100))),'0') a05
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id in
 (
select distinct int(xzbas_value) from bass1.all_dim_lkp where bass1_tbid='BASS_STD1_0074' 
and (bass1_value like '01%'
or
bass1_value like '02%'
or bass1_value like '03%')
) 
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)
 
),
(


select
value(char(bigint(sum(fact_fee*100))),'0') a06
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id in
 (
select distinct int(xzbas_value) from bass1.all_dim_lkp where bass1_tbid='BASS_STD1_0074' 
and bass1_value ='0101'

) 
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)

),
(

select
value(char(bigint(sum(fact_fee*100))),'0') a07
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id in
 (
select distinct int(xzbas_value) from bass1.all_dim_lkp where bass1_tbid='BASS_STD1_0074' 
and bass1_value ='0305'

) 
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)
 
),
(

select
value(char(bigint(sum(fact_fee*100))),'0') a08
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id in
 (
select distinct int(xzbas_value) from bass1.all_dim_lkp where bass1_tbid='BASS_STD1_0074' 
and bass1_value ='0319'

) 
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)
 
 ),
(

select
value(char(bigint(sum(fact_fee*100))),'0') a09
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id in
 (
select distinct int(xzbas_value) from bass1.all_dim_lkp where bass1_tbid='BASS_STD1_0074' 
and bass1_value like '02%'

) 
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)
 
),
(
   
select
value(char(bigint(sum(fact_fee*100))),'0') a10
 from  
   bass2.dw_acct_shoulditem_$op_month
 where item_id =80000204
 and user_id in(select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1)

),
(

select
value(char(bigint(sum(a.fact_fee*100))),'0') a11
 from  bass2.dw_acct_shoulditem_$op_month a
 inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1 and (td_3gcard_mark=1 or td_2gcard_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id
 )   
 ,'0'            
                       )
               "        
                     
                        
  puts $sql_buff
  exec_sql $sql_buff
  
 
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



