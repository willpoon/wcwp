#=====================================================================*
#*�����ţ�22036     G_S_22036_MONTH.tcl                              *
#ԭ��                                                                  *
#  bass2.dim_bs_enterprise_customer_code                               *
#  bass2.DW_ENTERPRISE_SUB_yyyymm                                      *
#	 bass2.dw_newbusi_ismg_yyyymm                                        *
#	 BASS2.DIM_NEWBUSI_SPINFO                                            *
#*�������ͣ���                                                          *
#*����������22036�ӿڳ���                                               *
#������������¼��ʡ�ĺ�����Դ��Ϣ                                        *
#�������̣�int -s G_S_22036_MONTH.tcl                                  *
#�������ڣ�2007-08-23                                                  *
#�� �� �ߣ�xiahuaxue                                                   *
#�޸���ʷ��1.5.3�汾��22036��Ϊ������ҵӦ��ҵ�����ӿ� 2008-03-27         *
#======================================================================*

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
	    global env
      global handle

       
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
        puts $op_month

        set ThisMonthFirstDay [string range $op_month 0 5]01
        puts $ThisMonthFirstDay
        set PrevMonthLastDay [GetLastDay [string range $ThisMonthFirstDay 0 7]]

        puts $PrevMonthLastDay
        
        set op_time [string range $PrevMonthLastDay 0 3]-[string range $PrevMonthLastDay 4 5]-[string range $PrevMonthLastDay 6 7]  
        puts $op_time
        
        set this_month [string range $op_time 0 3][string range $op_time 5 6]    
        

	      #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
              

        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
	      #�������� this_year_days	
        set this_year_days [GetThisYearDays ${op_month}01]

	      #��������	this_month_days
        set this_month_days [GetThisMonthDays ${op_month}01]


        #����	$last_month	
       set last_month [GetLastMonth [string range $op_month 0 5]]
       
       #�������һ����
       set LastYearMonth [GetLastMonth [string range $op_month 0 3]01]
   
      scan   $op_time "%04s-%02s-%02s" year month day
      set    tmp_time_string  ""
      set    last_1_month [GetLastMonth [string range $year$month 0 5]]01 
      set    last_2_month [GetLastMonth [string range $last_1_month 0 5]]01 
      set    last_3_month [GetLastMonth [string range $last_2_month 0 5]]01 
      set    last_4_month [GetLastMonth [string range $last_3_month 0 5]]01 
      set    last_5_month [GetLastMonth [string range $last_4_month 0 5]]01
      scan   $last_3_month "%04s%02s%02s" last3_month_year last3_month_month last3_month_days
      scan   $last_5_month "%04s%02s%02s" last5_month_year last5_month_month last5_month_days

     puts $timestamp
     puts $op_month
     
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22036_MONTH where TIME_ID= $op_month"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  

#  #1.�������� ���Ź�˾�������ʡ�ĺ�����
# 	set sql_buff "select count(distinct product_no)  from bass2.DWD_RES_MSISDN_$timestamp "
#  set Total_Count [get_single $sql_buff]
#  puts "Total_Count�� $Total_Count"
#  #--------------------------------------------------------------------------------------------------------------
#     
#  #2.���·ź������º���Ͷ����
# 	set sql_buff "select count(distinct a.user_id)
#                  from bass2.dw_product_$this_month a
#                       left join
#                         bass2.dw_product_$last_month b
#                       on a.product_no = b.product_no
#                 where a.month_new_mark = 1     and 
#                       a.usertype_id in (1,2,9) and 
#                       b.product_no is null"
#  set PutoutNewNo_Count [get_single $sql_buff]
#  puts "PutoutNewNo_Count�� $PutoutNewNo_Count"
#  #--------------------------------------------------------------------------------------------------------------
#  
#  #3.���·ź����оɺ���Ͷ����
# 	set sql_buff "select count(distinct a.user_id)
#                  from bass2.dw_product_$this_month a
#                       left join
#                         bass2.dw_product_$last_month b
#                       on a.product_no = b.product_no
#                 where a.month_new_mark = 1     and 
#                       a.usertype_id in (1,2,9) and 
#                       b.product_no is not null"
#  set PutoutOldNo_Count [get_single $sql_buff]
#  puts "PutoutOldNo_Count�� $PutoutOldNo_Count"
#  #--------------------------------------------------------------------------------------------------------------
#     
#  #4.�����ۼ�Ͷ�ŵľɺ�����
# 	set sql_buff "select value(char(SUM(bigint(PutoutOldNo_Count))),'0')
#                  from bass1.G_S_22036_MONTH
#                 where time_id/100 = $year"
#  set PutoutOldLJ_Count [get_single $sql_buff]
#  puts "PutoutOldLJ_Count�� $PutoutOldLJ_Count"
#  #--------------------------------------------------------------------------------------------------------------
#     
#  #5.δ���������
# 	set sql_buff "select count(distinct product_no) from bass2.DWD_RES_MSISDN_$this_month_last_day where sts=1"
#  set UnAllot_Count [get_single $sql_buff]
#  puts "UnAllot_Count�� $UnAllot_Count"
#  #--------------------------------------------------------------------------------------------------------------
#     
#  #6.δ�����������������
# 	set sql_buff "select count(*) from bass2.DWD_RES_SIM_$this_month_last_day where  sts=1 and  puk1 is null and pin1 is null"
#  set UnFacture_Count [get_single $sql_buff]
#  puts "UnFacture_Count�� $UnFacture_Count"
#  #--------------------------------------------------------------------------------------------------------------
#  
#  #7.��������
# 	set sql_buff "select count(distinct product_no) from bass2.DWD_RES_MSISDN_$this_month_last_day where sts=2"
#  set Store_Count [get_single $sql_buff]
#  puts "Store_Count�� $Store_Count"
#  #--------------------------------------------------------------------------------------------------------------
#  
#  #8.�����̺���
# 	set sql_buff "select count(distinct product_no)  
# 	                from bass2.DWD_RES_MSISDN_$this_month_last_day 
# 	               where sts=2 and res_sts=4 and purpose=3 and wholesale_date is not null"
#  set Trench_Count [get_single $sql_buff]
#  puts "Trench_Count�� $Trench_Count"
#  #--------------------------------------------------------------------------------------------------------------
#     
#  #9.�ɻ��պ�����
# 	set sql_buff "select count(distinct product_no)
#                  from
#                     ((select product_no
#                       from bass2.dw_product_$op_month
#                       where userstatus_id = 0
#                         and usertype_id in (1,2,9)
#                       except
#                       select product_no
#                       from bass2.dw_product_$op_month
#                       where userstatus_id <> 0
#                         and usertype_id in (1,2,9))
#                       union all
#                       select product_no
#                       from bass2.dw_product_$op_month   
#                       where userstatus_id not in (0,1,2,3,6)
#                         and usertype_id in (1,2,9)
#                         and sts_date < '${last5_month_year}-${last5_month_month}-${last5_month_days}') a"
#  set CanCallback_Count [get_single $sql_buff]
#  puts "CanCallback_Count�� $CanCallback_Count"
#  #--------------------------------------------------------------------------------------------------------------
#
#  #10.��ҵ��ռ�ú�����
# 	set sql_buff "select count(product_no)
#                  from bass2.dw_product_$op_month
#                 where userstatus_id in (1,2,3,6) and 
#                       usertype_id in (1,2,9)     and 
#                       crm_brand_id3 in (800,700)"
#  set NewOperate_Count [get_single $sql_buff]
#  puts "NewOperate_Count�� $NewOperate_Count"
#  #--------------------------------------------------------------------------------------------------------------
     
 
# (TIME_ID           INTEGER,
#  BILL_MONTH        CHARACTER(6),  /*�·�	��ʽ��YYYYMM	                     */   
#  VestIn_Flat       CHARACTER(1),  /*�������ƽ̨  1- BOSSƽ̨  2- ������ƽ̨*/ 
#  Total_Count       CHARACTER(8),  /*�������� ���Ź�˾�������ʡ�ĺ�����     */
#  PutoutNewNo_Count CHARACTER(8),  /*���·ź������º���Ͷ����                */
#  PutoutOldNo_Count CHARACTER(8),  /*���·ź����оɺ���Ͷ����                */
#  PutoutOldLJ_Count CHARACTER(8),  /*�����ۼ�Ͷ�ŵľɺ�����                  */
#  UnAllot_Count     CHARACTER(8),  /*δ���������        */
#  UnFacture_Count   CHARACTER(8),  /*δ����������������� */
#  Store_Count       CHARACTER(8),  /*��������	         */
#  Trench_Count      CHARACTER(8),  /*�����̺���          */
#  CanCallback_Count CHARACTER(8),  /*�ɻ��պ�����        */
#  NewOperate_Count  CHARACTER(8),  /*��ҵ��ռ�ú�����    */


  
  set sql_buff "insert into BASS1.G_S_22036_MONTH
                select a.TIME_ID, a.BILL_MONTH, a.CUST_TYPE, a.EC_CODE, a.SINAME, 
                       a.OPERATE_TYPE, a.APP_LENCODE, a.APNCODE 
                from
                (       
                           select distinct $op_month   TIME_ID,
                                  '$op_month'          BILL_MONTH,
                                  '0'                  CUST_TYPE,
                                  b.enterprise_id      EC_CODE,
                                  ''                   SINAME,        
                                  case 
	                                  when c.oper_code like 'QXZ%' then '1'
	                                  when c.oper_code like 'M%' then '1'
	                                  else '2'
	                                end as              OPERATE_TYPE,
                                  a.service_code      APP_LENCODE,
                                  '' APNCODE,
                                  row_number()over(partition by a.service_code order by b.enterprise_id desc) row_id
                            from bass2.dim_bs_enterprise_customer_code a,
                                 bass2.DW_ENTERPRISE_SUB_$op_month b,
                          	     bass2.dw_newbusi_ismg_$op_month c,
                          	     BASS2.DIM_NEWBUSI_SPINFO d
                           where a.user_billing_id = b.bill_id
                                 and a.service_code=c.ser_code
                          	   and c.sp_code = d.sp_code
                                 and b.service_id in ('910','142','603','604','606','903','904','906','933') 
                          	   and a.service_code like '10657%' 
                ) a
                where a.row_id=1         "  
    puts $sql_buff      
    exec_sql $sql_buff      
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


