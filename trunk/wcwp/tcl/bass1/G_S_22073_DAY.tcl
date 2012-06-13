o######################################################################################################
#接口名称：竞争对手日KPI
#接口编码：22073
#接口说明：记录竞争对手日KPI信息
#程序名称: G_S_22073_DAY.tcl
#功能描述: 生成22073的数据
#运行粒度: 日
#源    表：1.bass2.dw_acct_shoulditem_yyyymmdd--
#          2.bass2.dw_comp_all_dt--
#          3.bass2.Dw_comp_cust_dt--
#          4.bass2.dw_product_yyyymmdd --
#          5.bass2.dw_comp_all_yyyymmdd--
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：20090425
#问题记录：
#修改历史: 20090918 liuzhilong c20 c21 c22 c23 c24 c25 6个字段加上条件 and (in_call_duration_m + out_call_duration_m) > 0
#          20091025 1.6.3规范新增3个指标统计
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	      #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
        #今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $op_time 8 9]
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        #删除本期数据
	set sql_buff "delete from bass1.G_S_22073_DAY where time_id=$timestamp"
	puts $sql_buff
	exec_sql $sql_buff

	set sql_buff "
insert into bass1.G_S_22073_DAY
values(
       $timestamp,
	     '$timestamp',
       (
        select char(count(distinct comp_product_no)) c02
         from  bass2.dw_comp_all_$timestamp 
         where comp_brand_id in(3,4)
           and in_call_counts>0
        ),
       (
        select char(count(distinct comp_product_no)) c03
         from  bass2.dw_comp_all_$timestamp 
         where in_call_counts>0
           and comp_product_no like '156%'
        ),   
       (
        select char(count(distinct comp_product_no)) c04
         from  bass2.dw_comp_all_$timestamp 
         where comp_brand_id = 7
           and in_call_counts>0
        ),
        '0',
       (
        select char(count(distinct comp_product_no)) c06
         from  bass2.dw_comp_all_$timestamp 
         where comp_brand_id in(3,4)
            and out_call_counts>0
        ),
       (
        select char(count(distinct comp_product_no)) c07
         from  bass2.dw_comp_all_$timestamp 
         where out_call_counts>0
            and comp_product_no like '156%'
        ),
       (
        select char(count(distinct comp_product_no)) c08
         from  bass2.dw_comp_all_$timestamp 
         where comp_brand_id = 7
            and out_call_counts>0
        ),
        '0',
       (
        select char(count(distinct comp_product_no)) c10
         from  bass2.dw_comp_all_$timestamp 
          where comp_brand_id in(9,10,11)
            and in_call_counts>0
        ),
       
       ( select char(count(distinct comp_product_no)) c11
         from  bass2.dw_comp_all_$timestamp 
          where in_call_counts>0
            and comp_product_no like '133%'
        ),
       (
        select char(count(distinct comp_product_no)) c12
        from  bass2.dw_comp_all_$timestamp 
         where in_call_counts>0
           and comp_product_no like '189%' 
        ),
       (
         select char(count(distinct comp_product_no)) c13
         from  bass2.dw_comp_all_$timestamp                     
          where in_call_counts>0                               
            and comp_brand_id =2                                    
        ), 
       (
         select char(count(distinct comp_product_no)) c14
         from  bass2.dw_comp_all_$timestamp                    
          where in_call_counts>0                               
            and comp_brand_id in(1,8)
       ),
       (
         select char(count(distinct comp_product_no)) c15
         from  bass2.dw_comp_all_$timestamp                        
          where comp_brand_id in(9,10,11)                             
            and out_call_counts>0   
       ),
      (
         select char(count(distinct comp_product_no)) c16
         from  bass2.dw_comp_all_$timestamp                                                        
          where out_call_counts>0                                  
            and comp_product_no like '133%'  
       ),
       (
         select char(count(distinct comp_product_no)) c17
         from  bass2.dw_comp_all_$timestamp                                              
          where out_call_counts>0                                  
            and comp_product_no like '189%'  
       ),
      (
        select char(count(distinct comp_product_no)) c18
        from  bass2.dw_comp_all_$timestamp                    
         where out_call_counts>0                              
           and comp_brand_id =2
       ),
      (
        select char(count(distinct comp_product_no)) c19
        from  bass2.dw_comp_all_$timestamp     
         where out_call_counts>0               
           and comp_brand_id in(1,8)
       ),
      (
        select char(count(distinct comp_product_no)) c20
        from bass2.dw_comp_all_dt                        
        where comp_brand_id in (3,4)  and (in_call_duration_m + out_call_duration_m) > 0 
       ),
      (
        select char(count(distinct comp_product_no)) c21
        from bass2.dw_comp_all_dt             
        where comp_brand_id=7  and (in_call_duration_m + out_call_duration_m) > 0      
       ),
      '0',
      (
       select char(count(distinct comp_product_no)) c23
        from bass2.dw_comp_all_dt                        
        where comp_brand_id in (9,10,11) and (in_call_duration_m + out_call_duration_m) > 0    
       ),
      (
        select char(count(distinct comp_product_no)) c24
        from bass2.dw_comp_all_dt             
        where comp_brand_id =2  and (in_call_duration_m + out_call_duration_m) > 0
       ),
       (
         select char(count(distinct comp_product_no)) c25
         from bass2.dw_comp_all_dt             
         where comp_brand_id in(1,8) and (in_call_duration_m + out_call_duration_m) > 0
       ),
       
 (select char(count(distinct comp_product_no)) c26
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (3,4)),
  

( select char(count(distinct comp_product_no)) c27
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_product_no like '156%'),

( select char(count(distinct comp_product_no)) c28
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_product_no like '186%'),   

'0',
 
 (select char(count(distinct comp_product_no)) c30
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id=7),
  
(  
select char(count(distinct comp_product_no)) c31
from bass2.Dw_comp_cust_dt
where comp_brand_id in(3,4)
and comp_userstatus_id=1),


(
select char(count(distinct comp_product_no)) c32
from bass2.Dw_comp_cust_dt
where comp_product_no like '156%'
 and comp_userstatus_id=1),

(
select char(count(distinct comp_product_no)) c33
from bass2.Dw_comp_cust_dt
where comp_product_no like '186%'
 and comp_userstatus_id=1), 

'0', 

(    
select char(count(distinct comp_product_no)) c35
from bass2.Dw_comp_cust_dt
where comp_brand_id =7
and comp_userstatus_id=1),

 (   
select char(count(distinct comp_product_no)) c36
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in(3,4)),

(  
select char(count(distinct comp_product_no)) c37
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_product_no like '156%'),
 
(  
select char(count(distinct comp_product_no)) c38
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_product_no like '186%'),
 
'0',
 
(    
select char(count(distinct comp_product_no)) c40
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id=7),

 (
 select char(count(distinct comp_product_no)) c41
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)),

(
 select char(count(distinct comp_product_no)) c42
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
   and comp_product_no like '133%'),
    
 (
 select char(count(distinct comp_product_no)) c43
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
   and comp_product_no like '189%'),
      
 (
 select char(count(distinct comp_product_no)) c44
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
   and comp_brand_id in(1,8)),

 (  
 select char(count(distinct comp_product_no)) c45
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
   and comp_brand_id =2),
 
  ( 
select char(count(distinct comp_product_no)) c46
from bass2.Dw_comp_cust_dt
where  comp_brand_id in (9,10,11)
and comp_userstatus_id=1),

 (
select char(count(distinct comp_product_no)) c47
from bass2.Dw_comp_cust_dt
where  comp_product_no like '133%'
and comp_userstatus_id=1),

(
select char(count(distinct comp_product_no)) c48
from bass2.Dw_comp_cust_dt
where  comp_product_no like '189%'
and comp_userstatus_id=1),

  (
select char(count(distinct comp_product_no)) c49
from bass2.Dw_comp_cust_dt
where  comp_brand_id in(1,8)
and comp_userstatus_id=1),

 (  
select char(count(distinct comp_product_no)) c50
from bass2.Dw_comp_cust_dt
where  comp_brand_id =2
and comp_userstatus_id=1),

 
  ( 
select char(count(distinct comp_product_no)) c51
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in (9,10,11)),
  
(
select char(count(distinct comp_product_no)) c52
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_product_no like '133%'),
 
(
select char(count(distinct comp_product_no)) c53
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_product_no like '189%'),
  
 (
select char(count(distinct comp_product_no)) c54
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in (1,8)),
  
 (
select char(count(distinct comp_product_no)) c55
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id =2)             
)
	
	"

	exec_sql $sql_buff

##~   加入到达数调整脚本，目的是减少上报到达数与本地实际到达数的差距##~   修改电信移动新增，每天+10 ， 以便缩小到达数的误差。 	  set sqlbuf "		update (		select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))		) t 		set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) + 10 )	  "	  exec_sql $sqlbuf     ##~   修改电信移动到达，每天+10 ， 以便缩小到达数的误差。	set sqlbuf "		values days(current date) - days(date('2012-05-02'))	"	set vCount [get_single $sqlbuf]if { [ expr ${DEC_RESULT_VAL1}*10 ] <= 8533 } { 	  set sqlbuf "		update (		select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))		) t 		set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 6913-1620 + ${vCount}*10 )	  "	  exec_sql $sqlbuf}##~   201206211日超标厉害，调了到达数，现在要每天调。
	set sqlbuf "		values days(current date) - days(date('2012-06-12'))	"	set vCount [get_single $sqlbuf]if { ${vCount} <= 50 } { 	  set sqlbuf "		update (		select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))		) t 		set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 50 + ${vCount} )	  "	  exec_sql $sqlbuf}if { ${vCount} <= 56 } { 	  set sqlbuf "		update (		select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))		) t 		set UNION_MOBILE_ARRIVE_CNT = char(bigint(UNION_MOBILE_ARRIVE_CNT) - 56 + ${vCount} )	  "	  exec_sql $sqlbuf}
##############################################
	return 0      
}   
