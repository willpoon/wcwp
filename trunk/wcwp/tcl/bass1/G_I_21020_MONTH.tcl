######################################################################################################
#�ӿ����ƣ����������û��������
#�ӿڱ��룺21020
#�ӿ�˵��������ͳ����ĩ���һ��24:00�����㵱�ա��������ֿͻ���������ͳ�������ľ��������û����뼯�ϡ�
#���������ֿͻ�����������ָ��90�������й��ƶ��ͻ�������������ͨ��������������Ե���ź͵�Ե���ţ��ľ��������û����뼯�ϡ�
#��������: g_i_21020_month.tcl
#��������: ����21020������
#��������: ��
#Դ    ��1.bass2.dw_comp_all_yyyymm
#          2.bass2.dw_comp_cust_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-09-19
#�����¼��1.ֻץȡ�����û������ģ����Ҿ���������Ӫ�������ڹ淶���������嵥
#�޸���ʷ: 1.
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

	global app_name
	set app_name "g_i_21020_month.tcl"                 

  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_21020_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	

    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass2.dw_comp_cust_$op_month with distribution and detailed indexes all
    
    exec db2 runstats on table bass2.dw_comp_all_$op_month with distribution and detailed indexes all
    
    exec db2 terminate
	
	
	#ֱ�ӻ��ܵ������
	#COMP_PRODUCT_NO
#13228986978*  
#13228986978
#�������ϵ��ظ���
  set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_i_21020_month
				select 
         TIME_ID
        ,COMP_PRODUCT_NO
        ,COMP_BRAND_ID
        ,COMP_BEGIN_DATE
        ,COMP_LAST_DATE
        ,CALL_COUNTS
        ,SMS_COUNTS
        ,MMS_COUNTS
        from 				
				(
				select 
						${op_month} TIME_ID,
						a.comp_product_no COMP_PRODUCT_NO,
						case 
						  when a.comp_brand_id in (3,4) then '021000'
						  when a.comp_brand_id in (7)   then '022000'
						  when a.comp_brand_id in (9,10,11) then '031000'
						  when a.comp_brand_id in (2) then '032000'
						  when a.comp_brand_id in (1,8) then '033000'
						  when a.comp_brand_id in (6) then '034000'
						end COMP_BRAND_ID ,
						replace(substr(char(a.comp_open_date),1,10),'-','') COMP_BEGIN_DATE,
						replace(substr(char(a.comp_last_date),1,10),'-','') COMP_LAST_DATE,
						value(char(sum(b.in_call_counts+b.out_call_counts)),'0') CALL_COUNTS,
						value(char(sum(b.mo_sms_counts+b.mt_sms_counts)),'0') SMS_COUNTS,
					  value(char(sum(b.mo_mms_counts+b.mt_mms_counts)),'0') MMS_COUNTS,
					  row_number()over(partition by substr(trim(a.COMP_PRODUCT_NO),1,11) order by a.COMP_PRODUCT_NO ) rn
				from  bass2.dw_comp_cust_$op_month a
				left join bass2.dw_comp_all_$op_month b on a.comp_product_no = b.comp_product_no
				where a.comp_userstatus_id = 1
				  and a.comp_brand_id in (1,2,3,4,6,7,8,9,10,11)
			group by a.comp_product_no,
						case 
						  when a.comp_brand_id in (3,4) then '021000'
						  when a.comp_brand_id in (7)   then '022000'
						  when a.comp_brand_id in (9,10,11) then '031000'
						  when a.comp_brand_id in (2) then '032000'
						  when a.comp_brand_id in (1,8) then '033000'
						  when a.comp_brand_id in (6) then '034000'
						end ,
						replace(substr(char(a.comp_open_date),1,10),'-',''),
						replace(substr(char(a.comp_last_date),1,10),'-','') 
						) t where t.rn = 1
			"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

set sql_buff "
	update g_i_21020_month a
	set COMP_PRODUCT_NO = substr(COMP_PRODUCT_NO,1,11)
	where time_id = $op_month
	and COMP_BRAND_ID = '021000'
	and length(trim(COMP_PRODUCT_NO)) > 11
"
exec_sql $sql_buff


set sql_buff "
	delete from 
	(
		select * from   G_I_21020_MONTH
			where COMP_BRAND_ID = '021000'
			and time_id = $op_month
			and length(trim(COMP_PRODUCT_NO)) <> 11
	) t
"
exec_sql $sql_buff

	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '021000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 1 "


set sql_buff "
	delete from 
	(
		select * from   G_I_21020_MONTH
			where COMP_BRAND_ID = '031000'
			and time_id = $op_month
			and length(trim(COMP_PRODUCT_NO)) <> 11
	) t
"
exec_sql $sql_buff


	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '031000'
				and time_id = $op_month
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
		"

	 chkzero2 $sql_buff "invalid COMP_PRODUCT_NO ! 2 "


  #1.���chkpkunique
        set tabname "G_I_21020_MONTH"
        set pk                  "COMP_PRODUCT_NO"
        chkpkunique ${tabname} ${pk} ${op_month}
        
        
	 
	return 0
}	