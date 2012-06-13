
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



#此字段仅取以下分类：
#01:门户网站
#02:10086电话营业厅
#03: 短信营业厅
#04:WAP网站
#05:自助终端（包括所有的自助终端，即包括实体渠道和24小时营业厅内布放的自助终端，还包括商场等场所独立摆放的自助终端。）
#

    #电子渠道登陆客户清单-------------
    #网站
#这里用二经的代码，代码有误：dw_product_ 不能用号码直接关联 光限制  a.userstatus_id>0 是不够的 , 这里改成 a.userstatus_id in (1,2,3,6,8)

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



    set sql_buff "

			insert into G_S_02067_MONTH_2
			 select a.product_item_id
			 ,times 
			 ,c.fee_value 
			 ,d.rule_code from 
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
						,value(char(case when e.TIMES = 1 then 0 else bigint(FEE_VALUE/TIMES) end ),'0') GIFT_FEE
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
		from (
			select i.*
			,row_number()over(partition by USER_ID ,IMEI order by PLAN_DUR desc ) rn 
			from G_S_02067_MONTH_3 i 
		) o where o.rn= 1
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


	return 0
}


