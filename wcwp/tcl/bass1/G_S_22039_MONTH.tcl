######################################################################################################
#接口名称：各品牌用户月KPI
#接口编码：22039
#接口说明：记录各品牌用户的月KPI信息。
#程序名称: G_S_22039_MONTH.tcl
#功能描述: 生成22039的数据
#运行粒度: 月
#源    表：1.bass2.dmrn_user_ms
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-09-29
#问题记录：1.
#修改历史: 1.2011-3-7 12:07:05 屏蔽原有口径代码，暂时造数据
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
        puts $op_month

        set ThisMonthFirstDay [string range $op_month 0 5]01
        puts $ThisMonthFirstDay
        set PrevMonthLastDay [GetLastDay [string range $ThisMonthFirstDay 0 7]]

        puts $PrevMonthLastDay
        
        set op_time [string range $PrevMonthLastDay 0 3]-[string range $PrevMonthLastDay 4 5]-[string range $PrevMonthLastDay 6 7]  
        puts $op_time
        
        set this_month [string range $op_time 0 3][string range $op_time 5 6]    
        

	      #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]    
        ##~   set op_month 201205      

        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
	      #本年天数 this_year_days	
        set this_year_days [GetThisYearDays ${op_month}01]

	      #本月天数	this_month_days
        set this_month_days [GetThisMonthDays ${op_month}01]


        #上月	$last_month	
       set last_month [GetLastMonth [string range $op_month 0 5]]
       
       #上年最后一个月
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
     
  #删除本期数据
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

##~   puts  $last_2_day
##~   puts  $last_1_day

##~   #  由于重入网数据运行时间超长，此接口暂时造数据
	##~   set sql_buff "insert into bass1.G_S_22039_MONTH
	##~   select $op_month
	       ##~   ,'$op_month'
				 ##~   ,brand_id
		     ##~   ,char(int((rand()*10+95)*bigint(city_crw)/100))
		     ##~   ,char(int((rand()*10+95)*bigint(country_seat_crw)/100))
		     ##~   ,char(int((rand()*10+95)*bigint(country_crw)/100))
		##~   from bass1.G_S_22039_MONTH
	 ##~   where time_id=$last_month
	 ##~   "
	##~   puts $sql_buff
  ##~   exec_sql $sql_buff
  


	set sql_buff "

insert into G_S_22039_MONTH(
         TIME_ID
        ,BILL_MONTH
        ,BRAND_ID
        ,CITY_CRW
        ,COUNTRY_SEAT_CRW
        ,COUNTRY_CRW
)
select 
         $op_month TIME_ID
        ,'$op_month' BILL_MONTH
		,case when rn_base_brand_id = 100 then '1' when rn_base_brand_id = 200 then '3' else '2' end BRAND_ID
		,char(sum( case when rn_base_county_code  in ('1001','1009','1040','1059','1032','1070','1048')  then 1 end )) CITY_CRW
		,char(sum( case when not( rn_base_county_code   in ('1001','1009','1040','1059','1032','1070','1048') )  then 1 end)) COUNTRY_SEAT_CRW
		,'0' COUNTRY_CRW
from 	bass2.dmrn_user_ms  a , bass2.dw_product_$op_month b
where substr(replace(char(a.RN_DATE),'-',''),1,6) =  '$last_month'
and a.RN_USER_ID = b.user_id 
and b.usertype_id in (1,2,9) 
and b.userstatus_id in (1,2,3,6,8)
group by 
case when rn_base_brand_id = 100 then '1'
     when rn_base_brand_id = 200 then '3'
else '2' end
with ur
	"
	exec_sql $sql_buff


	return 0
}
 
