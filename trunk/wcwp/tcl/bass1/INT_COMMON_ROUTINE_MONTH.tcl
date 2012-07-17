######################################################################################################
#程序名称：	INT_COMMON_ROUTINE_MONTH.tcl
#校验接口：	03004
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
      set last_month [GetLastMonth [string range $op_month 0 5]]
      puts $last_month
      set last_last_month [GetLastMonth [string range $last_month 0 5]]
      puts $last_last_month
      #自然月 上月 01 日
      set last_month_first_day ${op_month}01
      puts $last_month_first_day
        ##今天的日期，格式dd(例：输入20070411 返回11)
				set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]        
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        
        #程序名
        global app_name
        set app_name "INT_COMMON_ROUTINE_MONTH.tcl"


	set sql_buff "select substr(replace(char(current date),'-',''),1,6) from bass2.dual"
	set curr_month [get_single $sql_buff]
	puts $curr_month
	set sql_buff "select day(current timestamp) from bass2.dual"
	set nature_day_of_month [get_single $sql_buff]
	puts $nature_day_of_month
	

####################################################



        #本月最后一天 $op_monthdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      
      set thismonth [string range $op_month 4 5]
      set thisyear  [string range $op_month 0 3] 


      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay 


      #得到上个月的1号日期
        set sql_buff "select date('$ThisMonthFirstDay')-1 month from bass2.dual"
            puts $sql_buff
            set LastMonthFirstDay [get_single $sql_buff]
            puts $LastMonthFirstDay
      set LastMonthYear [string range $LastMonthFirstDay 0 3]
      set LastMonth [string range $LastMonthFirstDay 5 6]
            
      #得到下个月的1号日期
        set sql_buff "select date('$ThisMonthFirstDay')+1 month from bass2.dual"
            puts $sql_buff
            set NextMonthFirstDay [get_single $sql_buff]
            puts $NextMonthFirstDay
      set NextMonthYear [string range $NextMonthFirstDay 0 3]
      set NextMonth [string range $NextMonthFirstDay 5 6]

####################################################
            

        if { $nature_day_of_month == "1" } {
	
	#生成月用户资料
	set sql_buff "alter table bass1.int_02004_02008_month_stage activate not logged initially with empty table"
	exec_sql $sql_buff

	#--抓取用户资料入表
	set sql_buff "
		insert into bass1.int_02004_02008_month_stage (
		     user_id    
		    ,product_no 
		    ,test_flag  
		    ,sim_code   
		    ,usertype_id  
		    ,create_date
		    ,brand_id
		    ,time_id )
		select e.user_id
		    ,e.product_no  
		    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
		    ,e.sim_code
		    ,f.usertype_id  
		    ,e.create_date  
		    ,e.brand_id
		    ,f.time_id       
		from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
				,row_number() over(partition by user_id order by time_id desc ) row_id   
		from bass1.g_a_02004_day
		where time_id/100 <= $op_month ) e
		inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
			   from bass1.g_a_02008_day
			   where  time_id/100 <= $op_month ) f on f.user_id=e.user_id
		where e.row_id=1 and f.row_id=1
"
	exec_sql $sql_buff

	aidb_runstats bass1.int_02004_02008_month_stage 3


	#and f.usertype_id NOT IN ('2010','2020','2030','9000')


	createtb INT_02004_02008_MONTH_YYYYMM   "USER_ID" TBS_APP_BASS1 TBS_INDEX $op_month

	#--抓取用户资料入表
	set sql_buff "
		insert into bass1.INT_02004_02008_MONTH_$op_month (
		     user_id    
		    ,product_no 
		    ,test_flag  
		    ,sim_code   
		    ,usertype_id  
		    ,create_date
		    ,brand_id
		    ,time_id )
		select          
		USER_ID
        ,PRODUCT_NO
        ,TEST_FLAG
        ,SIM_CODE
        ,USERTYPE_ID
        ,CREATE_DATE
        ,BRAND_ID
        ,TIME_ID     
		from bass1.int_02004_02008_month_stage
		with ur
"
	exec_sql $sql_buff

	aidb_runstats bass1.INT_02004_02008_MONTH_$op_month 3
	
	#建中间表
	createtb INT_02004_02008_YYYYMM "OP_TIME,USER_ID" TBS_APP_BASS1 TBS_INDEX $curr_month
	createtb INT_21007_YYYYMM       "OP_TIME,PRODUCT_NO,BRAND_ID,SVC_TYPE_ID,CDR_TYPE_ID" TBS_APP_BASS1 TBS_INDEX $curr_month
	createtb INT_22038_YYYYMM       "OP_TIME,USER_ID" TBS_APP_BASS1 TBS_INDEX $curr_month
	createtb INT_0400810_YYYYMM     "OP_TIME,PRODUCT_NO" TBS_APP_BASS1 TBS_INDEX $curr_month
	createtb INT_210012916_YYYYMM   "OP_TIME,USER_ID,PRODUCT_NO" TBS_APP_BASS1 TBS_INDEX $curr_month
	createtb INT_22401_YYYYMM   	"TIME_ID,USER_ID,CELL_ID,LAC_ID" TBS_APP_BASS1 TBS_INDEX $curr_month
	
	
        } else { 
        	puts "createtb only run on date01 " 
        	}

## 2 清理表空间
#2.1 G_S_21003_TO_DAY
#  mannual modify $clean_flag
set clean_flag 1
if { $clean_flag == "1" } {

	set sql_buff "delete from G_S_21003_TO_DAY where time_id  < $last_month_first_day"
	exec_sql $sql_buff

	set sql_buff "delete from G_S_21008_TO_DAY where time_id  < $last_month_first_day"
	exec_sql $sql_buff

	set sql_buff "delete from G_S_21003_MONTH where time_id  < $last_last_month"
	exec_sql $sql_buff

	set sql_buff "delete from G_S_21008_MONTH where time_id  < $last_last_month"
	exec_sql $sql_buff


	set sql_buff "delete from G_S_04002_DAY   where time_id/100  < $last_last_month"
	exec_sql $sql_buff
	#
	set TABLE_LIST_CLEAN { INT_02004_02008 INT_22038 INT_21007 INT_0400810 INT_210012916 }	
	foreach TAB_PREFIX ${TABLE_LIST_CLEAN} {
		set real_tabname ${TAB_PREFIX}_${last_month}
		set sql_buff "
			select count(0) from syscat.tables where tabname = '$real_tabname'
		"
		set RESULT_VAL [get_single $sql_buff]
		#create
		if { $RESULT_VAL > 0 } {
			#the drop statement
		set sql_buff "
			drop table $real_tabname
		"
		exec_sql $sql_buff
		
		}
		#end if
	     }
	     # end foreach
  }

	return 0
}
