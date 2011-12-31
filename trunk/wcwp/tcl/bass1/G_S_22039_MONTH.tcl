######################################################################################################
#�ӿ����ƣ���Ʒ���û���KPI
#�ӿڱ��룺22039
#�ӿ�˵������¼��Ʒ���û�����KPI��Ϣ��
#��������: G_S_22039_MONTH.tcl
#��������: ����22039������
#��������: ��
#Դ    ����1.bass2.dmrn_user_ms
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-09-29
#�����¼��1.
#�޸���ʷ: 1.2011-3-7 12:07:05 ����ԭ�пھ����룬��ʱ������
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

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

      scan   $last_1_month "%04s%02s%02s" last1_month_year last1_month_month last1_month_days
      scan   $last_2_month "%04s%02s%02s" last2_month_year last2_month_month last2_month_days
      
      set    last_1_day $last1_month_year-$last1_month_month-01
      set    last_2_day $last2_month_year-$last2_month_month-01

     puts $timestamp
     puts $op_month
     puts $last_1_day
     puts $last_2_day
     
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22039_MONTH where TIME_ID= $op_month"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------

###	set sql_buff "insert into bass1.G_S_22039_MONTH
###	select $op_month,
###	       '$op_month',
###         case when rn_base_brand_id = 100 then '1'
###              when rn_base_brand_id = 200 then '3'
###	            else '2'
###	       end,
###	   char(count(distinct(case when rn_base_county_code  in ('1001','1009','1040','1059','1032','1070','1048')  then his_user_id end))),
###	   char(count(distinct(case when rn_base_county_code  not in ('1001','1009','1040','1059','1032','1070','1048')  then his_user_id end))),
###	   '0'
###	from bass2.dmrn_user_ms
###	where rn_date >= '$last_2_day' and rn_date < '$last_1_day'
###    group by 
###       case when rn_base_brand_id = 100 then '1'
###            when rn_base_brand_id = 200 then '3'
###	        else '2'
###	   end"
###	puts $sql_buff
###  exec_sql $sql_buff
###
###

#  ������������������ʱ�䳬�����˽ӿ���ʱ������
	set sql_buff "insert into bass1.G_S_22039_MONTH
	select $op_month
	       ,'$op_month'
				 ,brand_id
		     ,char(int((rand()*10+95)*bigint(city_crw)/100))
		     ,char(int((rand()*10+95)*bigint(country_seat_crw)/100))
		     ,char(int((rand()*10+95)*bigint(country_crw)/100))
		from bass1.G_S_22039_MONTH
	 where time_id=$last_month
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
