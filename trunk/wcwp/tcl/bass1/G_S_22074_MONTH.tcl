
######################################################################################################		
#接口名称: 农信通个人业务发展情况月汇总                                                               
#接口编码：22074                                                                                          
#接口说明：此接口上报农信通个人业务用户订购、收入及业务量情况。
#程序名称: G_S_22074_MONTH.tcl                                                                            
#功能描述: 生成22074的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]      
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
 set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
puts $this_month_first_day
set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
puts $this_month_last_day


        #删除本期数据
	set sql_buff "delete from bass1.G_S_22074_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
	set sql_buff "ALTER TABLE bass1.G_S_22074_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
  exec_sql $sql_buff


	set sql_buff "
		insert into G_S_22074_MONTH_1
		 select product_instance_id , name,offer_id,SUPPLIER_ID,EXTEND_ID2
		,case 
		when offer_id = 113200000081 then '1'
		when offer_id = 113200000071 then '3'
		when offer_id in( 113110217758,113110148956) then '8'
		when offer_id in( 113110217757,113110148669) then '9'
		when offer_id in( 113110148955,113110217759) then '10'
		else '11'
		end NXT_TYPE
		,case when char(a.create_date) like '$optime_month%' then 1 else 0 end new_flag
		 from bass2.dw_product_ins_off_ins_prod_$op_month a, bass2.dim_prod_up_product_item  b
		where a.OFFER_ID = b.PRODUCT_ITEM_ID
		AND (
		B.NAME LIKE '%百事易%'
		or B.NAME LIKE '%务工易%'
		or B.NAME LIKE '%农情气象%'
		or B.NAME LIKE '%农信宝%'
		)
		with ur
"
exec_sql $sql_buff
  

## mms 

# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
        set i 0
# 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
        while { $i<=62 } {
                set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
                set i_time [get_single $sql_buff]
        
        if { $i_time >= ${this_month_first_day} && $i_time <= ${this_month_last_day} } {
        puts $i_time
                set sql_buff "select replace(char(current date - ( 1+$i ) days),'-','') from bass2.dual"
                set i_timestamp [get_single $sql_buff]
                Deal_22074_mmscdr $i_timestamp $optime_month
                Deal_22074_smscdr $i_timestamp $optime_month
        }
        incr i
        }






	set sql_buff "
	insert into bass1.G_S_22074_MONTH
                        select
                          $op_month
                          ,NXT_TYPE
			  ,char(count(distinct a.user_id)) ORDER_CNT
			  ,char(count(case when new_flag = 1 then a.user_id end )) NEW_CNT
			  ,'0' USED_CNT
			  ,value(char(sum(mms_cnt)),'0') DOWN_SMS_CNT
			  ,value(char(sum(sms_cnt)),'0') DOWN_MMS_CNT
			  ,value(char(sum(value(b.income,0)+value(c.income,0))),'0') income
			  from (select distinct PRODUCT_INSTANCE_ID user_id ,NXT_TYPE,NEW_FLAG from G_S_22074_MONTH_1) a
			  left join (select user_id , count(0) mms_cnt,sum(CHARGE1) income from G_S_22074_MONTH_2 group by user_id  ) b on a.user_id = b.user_id 
			  left join (select user_id , count(0) sms_cnt,sum(CHARGE1) income from G_S_22074_MONTH_3 group by user_id  ) c on a.user_id = c.user_id
                          group by NXT_TYPE
			  with ur
	"
  exec_sql $sql_buff

	return 0
}




proc Deal_22074_mmscdr { op_time optime_month } {

  #set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  set timestamp $op_time
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
  
set sql_buff "delete from G_S_22074_MONTH_2 where time_id = $timestamp"
exec_sql $sql_buff

set sql_buff "
insert into G_S_22074_MONTH_2
		    	  select
				 $timestamp,
		    	  	 A.user_id,
				 a.PRODUCT_NO,
				 a.ACCT_ID,
		    	  	 a.SP_CODE,
		    	  	 a.OPER_CODE,
				 CHARGE1+CHARGE2+CHARGE3+CHARGE4  charge
		    	  from bass2.cdr_mms_$timestamp a 
			  where (USER_ID,SP_CODE,OPER_CODE) in 
				(select PRODUCT_INSTANCE_ID,SUPPLIER_ID,EXTEND_ID2 from G_S_22074_MONTH_1)
with ur
"
exec_sql $sql_buff

return 0

}






proc Deal_22074_smscdr { op_time optime_month } {

  #set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  set timestamp $op_time
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
  
set sql_buff "delete from G_S_22074_MONTH_3 where time_id = $timestamp"
exec_sql $sql_buff

set sql_buff "
insert into G_S_22074_MONTH_3
		    	  select
				 $timestamp,
		    	  	 A.user_id,
				 a.PRODUCT_NO,
				 a.ACCT_ID,
		    	  	 a.SP_CODE,
		    	  	 a.OPER_CODE,
				 CHARGE1+CHARGE2+CHARGE3+CHARGE4  charge
		    	  from bass2.cdr_ismg_dtl_$timestamp a 
			  where (USER_ID,SP_CODE,OPER_CODE) in 
				(select PRODUCT_INSTANCE_ID,SUPPLIER_ID,EXTEND_ID2 from G_S_22074_MONTH_1)
with ur
"
exec_sql $sql_buff

return 0

}

