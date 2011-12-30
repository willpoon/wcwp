######################################################################################################
#接口名称：用户积分回馈情况
#接口编码：02007
#接口说明：记录用户积分兑换情况
#程序名称: G_S_02007_MONTH.tcl
#功能描述: 生成02007的数据
#运行粒度: 日
#源    表：1.bass2.dwd_product_exchgscore_yyyymmdd(用户积分兑换情况)
#          2.bass2.dw_product_yyyymmdd(用户日表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 临时 20080710
# 1.6.1规范新增3个字段修改 liuqf
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        #set op_month 200810
        #本月 yyyy-mm
        set opmonth $optime_month       
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day

      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay 


#      set year 2008
#      set month 07
#      set day 01
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    tmp_time_string  ""
      
   ##两年前的1月1日,如：2007-01-01   
      set  syear [expr $year-2]
      puts $syear
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"  
      set    one  "01"
      set    BeginDate  ${syear}${sa}
      set    lasttwo_year_begin_month  ${syear}${one}
      puts   $BeginDate
      puts   $lasttwo_year_begin_month

   ##一年前的1月1日,如：2008-01-01         
      set  syear [expr $year-1]
      puts $syear
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"  
      set    one  "01"
      set    EndDate  ${syear}${sa}  
      set    last_year_begin_month  ${syear}${one}
      puts   $EndDate
      puts   $last_year_begin_month
      
   ##当年的1月1日,如：2009-01-01   
      set  syear [expr $year]
      puts $syear
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      set    sa   "-01-01 0:00:00"
      set    one  "01"  
      set    NowDate  ${syear}${sa}
      set    this_year_begin_month  ${syear}${one}
      puts   $this_year_begin_month
      puts $NowDate


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

      set NextMonth06 [string range $NextMonthFirstDay 0 3][string range $NextMonthFirstDay 5 6]06
      puts $NextMonth06
      set charge_date1 "$ThisMonthFirstDay 00:00:00"
      puts $charge_date1
      set charge_date2 "$NextMonthFirstDay 00:00:00"
      puts $charge_date2
      #charge_date> '2008-02-01 00:00:00' and charge_date < '2008-02-04 00:00:00';"


####20090731
####2009-07-01
####2007
####2007-01-01 0:00:00
####200701
####2008
####2008-01-01 0:00:00
####200801
####2009
####200901
####2009-01-01 0:00:00
####select date('2009-07-01')-1 month from bass2.dual
####2009-06-01
####select date('2009-07-01')+1 month from bass2.dual
####2009-08-01
####20090806
####2009-07-01 00:00:00
####2009-08-01 00:00:00



        #删除本期数据
	set sql_buff "delete from bass1.g_s_02007_month where time_id=$op_month"
 	puts $sql_buff
	exec_sql $sql_buff

       
#查找促销类型
#Select distinct b.promo_id,b.promo_name,b.cond_id,b.cond_name
#from BASS2.DWD_CM_BUSI_COIN_20080630 a
#left outer join
#     BASS2.DW_PRODUCT_BUSI_PROMO_200806 b
#on a.so_nbr = b.so_nbr 
#where a.charge_date > '2008-06-01 00:00:00' and a.charge_date < '2008-07-01 00:00:00' 
#with ur;

	set sql_buff "insert into bass1.G_S_02007_month
                      (
                      time_id
                      ,point_feedback_id
                      ,user_id
                      ,used_point
                      ,Used_Count
                      )
                    select
                      $op_month,
                      case when (b.cond_name like '%充值卡%' or b.cond_name like '%扣积分%送%话费%') then '1000'
                           when (b.cond_name like '%GPRS套餐%' or b.cond_name like '%手机报%' or b.cond_name like '%短信套餐%' or 
                                 b.cond_name like '%送彩铃%'   or b.cond_name like '%彩信套餐%' or b.cond_name like '%无线音乐俱乐部%') then '2000'
                           when (b.cond_name like '%VIP客户%') then '3000'
                           when (b.cond_name like '%兑手机%') then '4000'
                           when (b.cond_name like '%电影票%') or (b.cond_name like '%演唱会门票%') or (b.cond_name like '%新专辑%') then '5000'
                           else '5000'
                      end ,
                      a.user_id,
                      char(sum(a.value)),
                      char(count(*))
                 from BASS2.DWD_CM_BUSI_COIN_$this_month_last_day a
                    left outer join 
                      (select distinct so_nbr,user_id,cond_name from  BASS2.DW_PRODUCT_BUSI_PROMO_$op_month) b
                    on a.so_nbr = b.so_nbr 
                 where a.charge_date > '$charge_date1' and a.charge_date < '$charge_date2'
                 group by                       
                     case when (b.cond_name like '%充值卡%' or b.cond_name like '%扣积分%送%话费%') then '1000'
                           when (b.cond_name like '%GPRS套餐%' or b.cond_name like '%手机报%' or b.cond_name like '%短信套餐%' or 
                                 b.cond_name like '%送彩铃%'   or b.cond_name like '%彩信套餐%' or b.cond_name like '%无线音乐俱乐部%') then '2000'
                           when (b.cond_name like '%VIP客户%') then '3000'
                           when (b.cond_name like '%兑手机%') then '4000'
                           when (b.cond_name like '%电影票%') or (b.cond_name like '%演唱会门票%') or (b.cond_name like '%新专辑%') then '5000'
                           else '5000'
                      end ,
                      a.user_id
                  having sum(a.value) > 0;"           
 	puts $sql_buff
	exec_sql $sql_buff




	set sql_buff "insert into bass1.G_S_02007_month
                      (
                      time_id
                      ,point_feedback_id
                      ,user_id
                      ,used_point
                      ,Used_Count
                      )
                    select
                      $op_month,
                      '5000'
                      end ,
                      user_id,
                      char(sum(old_total_score-total_score)),
                      char(count(*))
                 from bass2.DWD_PRODUCT_SCORECHG_200812 where alter_type=22 and sts=1 and     
                      alter_date>='$charge_date1' and alter_date<'$charge_date2'
                 group by                       
                      user_id
               having sum(old_total_score-total_score) > 0;"           
 	puts $sql_buff
	exec_sql $sql_buff


#select count(distinct user_id) from bass2.DWD_PRODUCT_SCORECHG_200812 where alter_type=22 and sts=1 
# and      alter_date>='2008-07-01 00:00:00.000000' and alter_date<'2009-01-01 00:00:00.000000'
 
 
 
 
 
	set sql_buff "insert into bass1.G_S_02007_month
                      (
                      time_id
                      ,point_feedback_id
                      ,user_id
                      ,used_point
                      ,Used_Count
                      )
                    select
                      888888
                      ,point_feedback_id
                      ,a.user_id
                      ,used_point
                      ,Used_Count
                 from bass1.G_S_02007_month a,
                      bass2.dw_product_$op_month b 
                 where a.time_id = $op_month and a.user_id = b.user_id and userstatus_id in (1,2,3,6) and usertype_id in (1,2,9) and free_mark = 0;"           
 	puts $sql_buff
	exec_sql $sql_buff
	
	
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.G_S_02007_month where time_id = $op_month;"           
         puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}     
	aidb_commit $conn
	aidb_close $handle
	
	
  set handle [aidb_open $conn]
	set sql_buff "update bass1.G_S_02007_month set time_id = $op_month where time_id = 888888 "
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle	



  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.G_S_02007_month where  time_id = 888888 "
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle
	
		return 0
}
###########参考############
#and b.userstatus_id in (1,2,3,6)  --在网用户--
#userstatus_id in (4,5,7,8) --离网用户
#and b.usertype_id in (1,2,9) --非虚拟用户--
#and (b.crm_brand_id1=1 or b.crm_brand_id1=4) --全球通和动感地带用户--
#and a.sts=1 --正常记录--
#############################################################


#内部函数部分	
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

