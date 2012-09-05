
######################################################################################################		
#接口名称: 定制终端捆绑营销方案及用户订购情况                                                               
#接口编码：02067                                                                                          
#接口说明：用户购买定制终端时，捆绑的终端销售营销方案情况
#程序名称: G_S_02067_MONTH.tcl                                                                            
#功能描述: 生成02067的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120605
#问题记录：
#修改历史: 1. panzw 20120605	中国移动一级经营分析系统省级数据接口规范 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        global app_name
        set app_name "G_S_02067_MONTH.tcl"
          
    #删除本期数据
	set sql_buf "delete from bass1.G_S_02067_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#
	set sql_buf "ALTER TABLE BASS1.G_S_02067_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
	set sql_buf "ALTER TABLE BASS1.G_S_02067_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_02067_MONTH_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf



    set sql_buff "
			insert into G_S_02067_MONTH_1
			select product_item_id,EXTEND_ID,name,NUMERATOR  from table(
			select a.product_item_id,EXTEND_ID ,a.name
			from bass2.DIM_PROD_UP_PRODUCT_ITEM a 
			,bass2.DIM_PROD_UP_PLAN_PLAN_REL b 
			where a.product_item_id = b.relat_product_item_id 
			and extend_attr_g='0'
			) a,(
			select c.prod_id,d.priority,a.numerator/100 NUMERATOR,a.base_item,a.expr_id 
			from bass2.DW_PROD_PM_ADJUST_SEGMENT_$this_month_last_day a ,bass2.DW_PROD_PM_ADJSCHEME_DETAILS_$this_month_last_day b 
			,bass2.DW_PROD_PM_PROD_PKGS_$this_month_last_day c, bass2.DW_PROD_PM_PROM_PRIORITY_$this_month_last_day d
			where a.formula_id=89070001
			and a.adjustrate_id=b.adjustrate_id
			and b.scheme_id=c.scheme_id 
			and c.prod_id=d.prod_id
			) b where a.EXTEND_ID = b.prod_id
			with ur
	    "

    exec_sql $sql_buff    


##~   db2 "alter table bass1.G_S_02067_MONTH_2 add column  rule_val varchar(32)"

    set sql_buff "

			insert into G_S_02067_MONTH_2
			 select a.product_item_id
			 ,times 
			 ,c.fee_value 
			 ,d.rule_code 
			 ,substr(RULE_CODE,1,case when posstr(RULE_CODE,'|')-1 >= 1 then posstr(RULE_CODE,'|')-1 else 1 end )
			 from 
			 bass2.ODS_PROD_UP_ITEM_RELAT_$this_month_last_day  a
			 , bass2.ODS_PROD_UP_ITEM_RELAT_PRICE_$this_month_last_day b 
			 , bass2.ODS_PROD_UP_PRICE_PLAN_$this_month_last_day c 
			 , bass2.DW_PROD_UP_APPORT_RULE_$this_month_last_day d
			 where a.item_relat_id=b.item_relat_id
			 and c.price_plan_type_cd like '%APPORT%'
			 and  b.PRICE_PLAN_ID = c.PRICE_PLAN_ID
			 and c.apport_rule = d.rule_id								 
			with ur
	    "

    exec_sql $sql_buff    


 
 ##~   20120725 :
   ##~     where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6) <= '$op_month'					
##~   --> where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6) <= '$op_month'		
 
 
 ##~   case 
 ##~   when upper(rule_val) = 'A' then  bigint(FEE_VALUE/100/TIMES)
 ##~   when RULE_CODE like '%|%' then  bigint(FEE_VALUE/100)
 ##~   when e.TIMES = 1 then 0 
 ##~   else bigint(FEE_VALUE/100/TIMES) 
 ##~   end GIFT_FEE
if { $op_month == 201207 }  {
    set sql_buff "
	    insert into G_S_02067_MONTH_3
					  (
						 TIME_ID
						,USER_ID
						,IMEI
						,PLAN_DESC
						,EFF_DT
						,PLAN_DUR
						,MIN_CONSUME
						,PREPAY_FEE
						,PHONE_COST
						,GIFT_FEE
						,GIFT_DUR
						)
					select 
						$op_month TIME_ID
						, a.USER_ID
						,value(c.imei,'35470604221430') IMEI
						,a.COND_NAME PLAN_DESC
						, substr(replace(char(date(a.VALID_DATE)),'-',''),1,8) EFF_DT
						,char((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) PLAN_DUR
						,value(char(d.NUMERATOR),'0')MIN_CONSUME
						,char(b.PROM_APPOR) PREPAY_FEE
						,char(b.RES_FEE) PHONE_COST
						,value(char( 
						     case 
							 when upper(rule_val) = 'A' then  bigint(FEE_VALUE/100/TIMES)
							 when RULE_CODE like '%|%' then  bigint(FEE_VALUE/100)
							 when e.TIMES = 1 then 0 
							 else bigint(FEE_VALUE/100/TIMES) 
							 end  
							 ),'0') GIFT_FEE
						,value(char(e.times),'1') GIFT_DUR
					from bass2.dw_product_user_promo_$op_month a 
					join bass2.ods_up_res_tiem_$op_month b  on  a.cond_id = bigint(b.prom_id)
					left join (					
						select user_id,max(imei) imei from 
						bass2.dw_product_mobilefunc_$op_month a
						where USERSTATUS_ID in (1,2,3,6,8)
						and  usertype_id in (1,2,9) 
						and a.imei is not null
						group by user_id 
					) c on a.user_id = c.user_id
					left join G_S_02067_MONTH_1 d on a.cond_id = d.product_item_id
					left join G_S_02067_MONTH_2 e on a.cond_id = e.product_item_id
 					where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6)  <= '$op_month'					
					with ur
	    "

    exec_sql $sql_buff 

#2011年部分

    set sql_buff "
insert into G_S_02067_MONTH_3
select 
	$op_month TIME_ID
	, a.USER_ID
	,value(c.imei,'35470604221430') IMEI
	,a.COND_NAME PLAN_DESC
	, substr(replace(char(date(a.VALID_DATE)),'-',''),1,8) EFF_DT
	,char((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) PLAN_DUR
	,value(char(d.MIN),'0')MIN_CONSUME
	,char(d.PREPAY) PREPAY_FEE
	,char(d.RES_FEE) PHONE_COST
	,char(case when APPORT > 0 then APPORT/((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) else 1 end) GIFT_FEE
    ,char((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) GIFT_DUR
from bass2.dw_product_user_promo_$op_month a 
left join (                                     
	select user_id,max(imei) imei from 
	bass2.dw_product_mobilefunc_$op_month a
	where USERSTATUS_ID in (1,2,3,6,8)
	and  usertype_id in (1,2,9) 
	and a.imei is not null
	group by user_id 
) c on a.user_id = c.user_id
 join G_S_02067_MONTH_2011 d on a.cond_id = d.product_item_id
where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6)  < '201201'                                 
and (year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1 > 0
and (case when APPORT > 0 then APPORT/((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) else 1 end) >= 10
with ur
	    "
    exec_sql $sql_buff 

} else {

# 后续月份正常抽取：
    set sql_buff "
	    insert into G_S_02067_MONTH_3
					  (
						 TIME_ID
						,USER_ID
						,IMEI
						,PLAN_DESC
						,EFF_DT
						,PLAN_DUR
						,MIN_CONSUME
						,PREPAY_FEE
						,PHONE_COST
						,GIFT_FEE
						,GIFT_DUR
						)
					select 
						$op_month TIME_ID
						, a.USER_ID
						,value(c.imei,'35470604221430') IMEI
						,a.COND_NAME PLAN_DESC
						, substr(replace(char(date(a.VALID_DATE)),'-',''),1,8) EFF_DT
						,char((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) PLAN_DUR
						,value(char(d.NUMERATOR),'0')MIN_CONSUME
						,char(b.PROM_APPOR) PREPAY_FEE
						,char(b.RES_FEE) PHONE_COST
						,value(char(
							 case 
							 when upper(rule_val) = 'A' then  bigint(FEE_VALUE/100/TIMES)
							 when RULE_CODE like '%|%' then  bigint(FEE_VALUE/100)
							 when e.TIMES = 1 then 0 
							 else bigint(FEE_VALUE/100/TIMES) 
							 end  						
						),'0') GIFT_FEE
						,value(char(e.times),'1') GIFT_DUR
					from bass2.dw_product_user_promo_$op_month a 
					join bass2.ods_up_res_tiem_$op_month b  on  a.cond_id = bigint(b.prom_id)
					left join (					
						select user_id,max(imei) imei from 
						bass2.dw_product_mobilefunc_$op_month a
						where USERSTATUS_ID in (1,2,3,6,8)
						and  usertype_id in (1,2,9) 
						and a.imei is not null
						group by user_id 
					) c on a.user_id = c.user_id
					left join G_S_02067_MONTH_1 d on a.cond_id = d.product_item_id
					left join G_S_02067_MONTH_2 e on a.cond_id = e.product_item_id
 					where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6)  = '$op_month'					
					with ur
	    "

    exec_sql $sql_buff 


}

    set sql_buff "
	    insert into G_S_02067_MONTH
					  (
						 TIME_ID
						,USER_ID
						,IMEI
						,PLAN_DESC
						,EFF_DT
						,PLAN_DUR
						,MIN_CONSUME
						,PREPAY_FEE
						,PHONE_COST
						,GIFT_FEE
						,GIFT_DUR
						)
		select 
		o.TIME_ID
        ,o.USER_ID
        ,o.IMEI
        ,o.PLAN_DESC
        ,o.EFF_DT
        ,o.PLAN_DUR
        ,o.MIN_CONSUME
        ,o.PREPAY_FEE
        ,o.PHONE_COST
        ,o.GIFT_FEE
        ,o.GIFT_DUR
		from (
			select i.*
			,row_number()over(partition by i.USER_ID ,IMEI order by PLAN_DUR desc ) rn 
			from G_S_02067_MONTH_3 i 
		) o , bass2.dw_product_$op_month p 		
		where o.rn= 1 
		and o.USER_ID = p.user_id 
		and p.TEST_MARK = 0 
	with ur
	    "

    exec_sql $sql_buff 


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_02067_MONTH"
  set pk   "USER_ID||IMEI"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
        
  aidb_runstats bass1.G_S_02067_MONTH 3


## 由于 product.up_res_tiem  默认没有2011 年的方案，需要boss添加。但他们没有添加，为手工提供。20120903：不再告警。
		##~   set grade 3
	        ##~   set alarmcontent "检查 product.up_res_tiem 维表更新情况"
	        ##~   WriteAlarm $app_name $op_month $grade ${alarmcontent}


	return 0
}



##~   数据调整 加强 imei 唯一性

##~   update(

##~   select * from G_S_02067_MONTH where  TIME_ID = 201207 and imei in 
 ##~   (
##~   select imei
##~   from G_S_02067_MONTH 
##~   where TIME_ID = 201207
##~   and  imei <> 'FFFFFFFFFFFFFF'
##~   group by  imei having count(0)   > 1
##~   ) 
##~   ) t set imei = char(bigint(imei)+bigint(rand(1)*10000))




##~   select TIME_ID , substr(EFF_DT,1,6) ,count(0) 
##~   --,  count(distinct TIME_ID ) 
##~   from G_S_02067_MONTH 
##~   where TIME_ID = 201207
##~   group by  TIME_ID , substr(EFF_DT,1,6) 
##~   order by 1 ,2

