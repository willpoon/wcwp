######################################################################################################
#�ӿ����ƣ�TD�ͻ������������»���
#�ӿڱ��룺22206
#�ӿ�˵������¼TD�ͻ���ÿ����������������Ϣ
#��������: G_S_22206_MONTH.tcl
#��������: ����22206������
#��������: ��
#Դ    ��1.bass2.dw_product_td_yyyymm 2.BASS2.DW_NEWBUSI_GPRS_YYYYMM
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xiahuaxue
#��дʱ�䣺2009-03-09
#�����¼��
#�޸���ʷ: liuzhilong 1.6.0�淶������������ֶ�. Ŀǰ�޴�ҵ��.��0
#          liuqf �޸��ֻ��ͻ����Ŀھ�
#          20091123 �޸��������ھ� a.apn_ni not in ('CMTDS') Ϊdrtype_id not in (8307)
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
	set sql_buff "delete from bass1.g_s_22206_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� �����ͻ���  
	set sql_buff "insert into bass1.g_s_22206_month
                values( $op_month,'$op_month',
                
            
(
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id
),
(    

select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
  where drtype_id not in (8307)
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month 
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id    
    
),
(    
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
  where drtype_id not in (8307)
  and( MNS_TYPE=0 or MNS_TYPE is null)
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month 
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id 
  ),
(      

select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
  where drtype_id not in (8307)
  and MNS_TYPE=1
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month 
where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id 

),
(    
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1 
 and (td_3gcard_mark=1 or td_2gcard_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id

),
(    
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
  where ( MNS_TYPE=0 or MNS_TYPE is null)
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1 
 and (td_3gcard_mark=1 or td_2gcard_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id

),
(    
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$op_month
  where MNS_TYPE=1
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1  
 and (td_3gcard_mark=1 or td_2gcard_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9) and test_mark=0 ) b
    on a.user_id=b.user_id
)
,'0'
,'0'
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



