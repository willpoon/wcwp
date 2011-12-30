
######################################################################################################		
#接口名称: 自营厅营业员业务办理信息                                                               
#接口编码：22069                                                                                          
#接口说明：记录自营厅营业员业务办理情况；
#程序名称: G_S_22069_MONTH.tcl                                                                            
#功能描述: 生成22069的数据
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
	set sql_buff "delete from bass1.G_S_22069_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
	set sql_buff "ALTER TABLE bass1.G_S_22069_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
  exec_sql $sql_buff


##新增客户数
set sql_buff "
	insert into G_S_22069_MONTH_1
	select 
	char(b.OP_ID)
	,char(ORG_ID)
	,1
	,count(distinct product_instance_id) 
	from  (select user_id,so_nbr from  bass2.dw_product_$op_month where MONTH_NEW_MARK = 1 ) a
	,bass2.dw_product_ins_off_ins_prod_$op_month  b 
	where char(a.SO_NBR) =b.done_code
	and a.user_id = b.product_instance_id
	group by 	char(b.OP_ID)
	,char(ORG_ID)
	with ur
"
exec_sql $sql_buff




#缴费笔数
set sql_buff "
insert into G_S_22069_MONTH_1
select  
char(STAFF_ID)
,char(STAFF_ORG_ID)
,2
,count(distinct payment_id)
from BASS2.dw_acct_payment_dm_$op_month a 
where  a.opt_code not in (select paytype_id from bass2.dim_acct_paytype where paytype_name like '%空中充值%') 			
group by char(STAFF_ID)
,char(STAFF_ORG_ID)
with ur

"
exec_sql $sql_buff

#

# sum(amount)

set sql_buff "
insert into G_S_22069_MONTH_1
select  
char(STAFF_ID)
,char(STAFF_ORG_ID)
,3
,sum(amount)
from BASS2.dw_acct_payment_dm_$op_month a 
where  a.opt_code not in (select paytype_id from bass2.dim_acct_paytype where paytype_name like '%空中充值%') 			
group by  char(STAFF_ID)
,char(STAFF_ORG_ID)
with ur

"
exec_sql $sql_buff



set sql_buff "
insert into G_S_22069_MONTH_1
select 
char(a.OP_ID)
,char(a.org_id)
,4
,count(0)
from bass2.dw_product_ord_cust_dm_$op_month a,
	bass2.dw_product_ord_offer_dm_$op_month b,
	bass2.dim_prod_up_product_item d,
	bass2.dim_pub_channel e,
	bass2.dim_sys_org_role_type f,
	bass2.dim_cfg_static_data g,
	bass2.Dim_channel_info h
where a.customer_order_id = b.customer_order_id
	and b.offer_id = d.product_item_id
	and a.org_id = e.channel_id
	and a.channel_type = g.code_value
	and g.code_type = '911000'
	and b.offer_type=2
	and e.channeltype_id = f.org_role_type_id
	and a.org_id = h.channel_id
	and h.channel_type_class in (90105,90102)
	and a.channel_type in ('b','9','5')
	group by  char(a.OP_ID)
,char(a.org_id)
with ur
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_22069_MONTH_1
select  
char(a.OP_ID)
,char(a.org_id)
,5
,count(distinct value(a.PRODUCT_INSTANCE_ID,'0') || value(char(b.OFFER_ID),'0') )
  from bass2.dw_product_ord_cust_dm_$op_month a,
       bass2.dw_product_ord_offer_dm_$op_month b,
       bass2.dim_prod_up_product_item d,
       bass2.dim_pub_channel e,
       bass2.dim_sys_org_role_type f,
       bass2.dim_cfg_static_data g,
       bass2.Dim_channel_info h
 where a.customer_order_id = b.customer_order_id
   and b.offer_id = d.product_item_id
   and a.org_id = e.channel_id
   and a.channel_type = g.code_value
   and g.code_type = '911000'
   and b.offer_type=2
   and e.channeltype_id = f.org_role_type_id
   and a.org_id = h.channel_id
   and h.channel_type_class in (90105,90102)
   and a.channel_type in ('b','9','5')
 group by char(a.OP_ID)
,char(a.org_id)
 with ur
"
exec_sql $sql_buff



##---imp

  
set sql_buff "
ALTER TABLE BASS1.G_S_22069_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff
	set sql_buff "
    insert into BASS1.G_S_22069_MONTH_2
                                  (	
					op_id,channel_id
                                        ,accept_type
                                        ,imp_accepttype
                                        ,cnt)
                select bigint(b.op_id),b.org_id
           ,case when b.so_mode='5' then 2 else 1 end accept_type
                       ,case when b.busi_type in ('115','730') then '06'
                             when b.busi_type='117' then '12'
                             when b.busi_type='118' then '25'
                             when b.busi_type='126' then '10'
                             when b.busi_type='132' then '11'
                             when b.busi_type='137' then '19'
                             when b.busi_type='139' then '16'
                             when b.busi_type='708' then '26'
                             when b.busi_type='709' then '04'
                             when b.busi_type='716' then '27'
                             when b.busi_type='733' then '22'
                             when b.busi_type='726' then '20'
                             when b.busi_type='140' then '28'
                        end imp_accepttype
                       ,count(distinct b.user_id||char(b.offer_id)) 
                from bass2.Dim_channel_info a,
                (
                        select c.org_id,b.busi_type,b.so_nbr,b.so_mode ,c.USER_ID,c.OFFER_ID,c.op_id
                          from bass2.Dw_cm_busi_radius_dm_$op_month b,
                               bass2.dw_product_ord_so_log_dm_$op_month c 
                         where b.ext_holds2=c.so_log_id
                 ) b
                where a.channel_id = b.org_id
                  and a.channel_type_class in (90105,90102)
                  and b.busi_type in ('115','117','118','126','132','137','139','708','709','716','730','733','726','140')
                group by bigint(b.op_id), b.org_id,
                          case when b.so_mode='5' then 2 else 1 end,
                    case when b.busi_type in ('115','730') then '06'
                                     when b.busi_type='117' then '12'
                                     when b.busi_type='118' then '25'
                                     when b.busi_type='126' then '10'
                                     when b.busi_type='132' then '11'
                                     when b.busi_type='137' then '19'
                                     when b.busi_type='139' then '16'
                                     when b.busi_type='708' then '26'
                                     when b.busi_type='709' then '04'
                                     when b.busi_type='716' then '27'
                                     when b.busi_type='733' then '22'
                                     when b.busi_type='726' then '20'
                                     when b.busi_type='140' then '28'
                                end                       
        
"
exec_sql $sql_buff

	set sql_buff "
    insert into BASS1.G_S_22069_MONTH_2
                                  (
					op_id,channel_id
                                        ,accept_type
                                        ,imp_accepttype
                                        ,cnt)
                        select
                         a.OP_ID,a.org_id,
                         case when a.channel_type='9' then 2 else 1 end accept_type,
                         case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
                              when b.offer_id in (113090000003) then '03'
                              when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
                              when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
                              when b.offer_id in (111090002901) then '02'
                              when b.offer_id in (113110166406) then '05'
                              when b.offer_id in (113110203699) then '24'
                              when b.offer_id in (111090003215,113110180134) then '09'
                              when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
                              when b.offer_id in (111000000705) then '14'
                              when b.offer_id in (113110175125,113110195329) then '17'
                              when b.offer_id in (111098000044,113110180139,113110197954) then '18'
                              when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
                              when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
                                          when b.offer_id in (113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972) then '29'
                         end imp_accepttype,
       count(distinct value(a.PRODUCT_INSTANCE_ID,'0') || value(char(b.OFFER_ID),'0') )
                          from bass2.dw_product_ord_cust_dm_$op_month a,
                               bass2.dw_product_ord_offer_dm_$op_month b,
                               bass2.dim_prod_up_product_item d,
                               bass2.dim_pub_channel e,
                               bass2.dim_sys_org_role_type f,
                               bass2.dim_cfg_static_data g,
                               bass2.Dim_channel_info h
                         where a.customer_order_id = b.customer_order_id
                           and b.offer_id = d.product_item_id
                           and a.org_id = e.channel_id
                           and a.channel_type = g.code_value
                           and g.code_type = '911000'
                           and b.offer_type=2
                           and e.channeltype_id = f.org_role_type_id
                           and a.org_id = h.channel_id
                           and h.channel_type_class in (90105,90102)
                           and b.offer_id in 
                           (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230,
                                        113090000003,
                                        113090003319,113090003320,113090003321,111090033319,
                                        111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631,
                                        111090002901,
                                        113110166406,
                                        113110203699,
                                        111090003215,113110180134,
                                        111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002,
                                        111000000705,
                                        113110175125,113110195329,
                                        111098000044,113110180139,113110197954,
                                        113110156821,113110166390,113110168281,113110178283,113110208633,113110208635,
                                        111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290,
                                        113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972
                                        )
                           and a.channel_type in ('b','9','5')
                           and b.customer_order_id not in 
                           (select b.ORD_CUST_ID
                           from bass2.Dim_channel_info a,
                                                (
                                                        select c.org_id,b.busi_type,b.so_nbr,b.so_mode ,c.ORD_CUST_ID
                                                          from bass2.Dw_cm_busi_radius_dm_$op_month b,
                                                               bass2.dw_product_ord_so_log_dm_$op_month c 
                                                         where b.ext_holds2=c.so_log_id                                                 ) b
                                                        where a.channel_id = b.org_id
                                                          and a.channel_type_class in (90105,90102)
                                                          and b.busi_type in ('115','117','118','126','132','137','139','708','709','716','730','733','726','140')
                                 )
                         group by  a.OP_ID,a.org_id,
                                 case when a.channel_type='9' then 2 else 1 end,
                                                         case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
                                                              when b.offer_id in (113090000003) then '03'
                                                              when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
                                                              when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
                                                              when b.offer_id in (111090002901) then '02'
                                                              when b.offer_id in (113110166406) then '05'
                                                              when b.offer_id in (113110203699) then '24'
                                                              when b.offer_id in (111090003215,113110180134) then '09'
                                                              when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
                                                              when b.offer_id in (111000000705) then '14'
                                                              when b.offer_id in (113110175125,113110195329) then '17'
                                                              when b.offer_id in (111098000044,113110180139,113110197954) then '18'
                                                              when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
                                                              when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
                                                              when b.offer_id in (113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972) then '29'
                                                         end
        
"
	set sql_buff "
ALTER TABLE BASS1.G_S_22069_MONTH_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

	set sql_buff "
    insert into BASS1.G_S_22069_MONTH_3
                                  (
					op_id,channel_id
                                        ,accept_type
                                        ,imp_accepttype
                                        ,cnt)
                select 
                      op_id,trim(char(channel_id))
                                        ,trim(char(accept_type))
                                        ,imp_accepttype
                                        ,sum(cnt)
                        from BASS1.G_S_22069_MONTH_2
        group by op_id,trim(char(channel_id)),
                 trim(char(accept_type)),
                 imp_accepttype
"
exec_sql $sql_buff
        

##
set sql_buff "
insert into G_S_22069_MONTH_1
select 
char(a.OP_ID)
,a.CHANNEL_ID
,6
,sum(CNT) TERM_SALE_CNT
	 from  G_S_22069_MONTH_3 a
group by 
char(a.OP_ID)
,a.CHANNEL_ID
with ur
"
exec_sql $sql_buff



##
set sql_buff "
insert into G_S_22069_MONTH_1
select 
char(a.OP_ID)
,a.org_id
,7
,count(0) TERM_SALE_CNT
	 from    
	 bass2.dw_res_ctms_exchg_$op_month a ,   BASS2.DIM_TERM_TAC b 
where  a.sale_type like '%销售%'
and substr(a.imei,1,8)  = b.TAC_NUM 
group by 
char(a.OP_ID)
,a.org_id
with ur
"
exec_sql $sql_buff


##---term

set sql_buff "
insert into G_S_22069_MONTH_1
select 
char(a.OP_ID)
,a.org_id
,8
,sum(case when TERM_TYPE in ('0','1') then 1 else 0 end ) OTHER_SALE_CNT
	 from    
	 bass2.dw_res_ctms_exchg_$op_month a ,   BASS2.DIM_TERM_TAC b 
where  a.sale_type like '%销售%'
and substr(a.imei,1,8)  = b.TAC_NUM 
group by 
char(a.OP_ID)
,a.org_id
with ur
"
exec_sql $sql_buff



set sql_buff "
insert into G_S_22069_MONTH
select 
         $op_month TIME_ID
        ,'$op_month' OP_MONTH
        ,STAFF_ID
        ,CHANNEL_ID
        ,char(sum(case when ind = 1 then value else 0 end )) NEW_DEV_CNT
        ,char(sum(case when ind = 2 then value else 0 end )) REC_CNT
        ,char(sum(case when ind = 3 then value else 0 end )) REC_FEE
        ,char(sum(case when ind = 4 then value else 0 end )) VAL_REC_CNT
        ,char(sum(case when ind = 5 then value else 0 end )) VAL_OPEN_CNT
        ,char(sum(case when ind = 6 then value else 0 end )) IMP_VAL_OPEN_CNT
        ,char(sum(case when ind = 7 then value else 0 end )) TERM_SALE_CNT
        ,char(sum(case when ind = 8 then value else 0 end )) MOBILE_SALE_CNT
from G_S_22069_MONTH_1  a
where STAFF_ID  in (select STAFF_ID from G_I_06034_MONTH where time_id = $op_month ) 
group by 
        STAFF_ID
        ,CHANNEL_ID
with ur
"
exec_sql $sql_buff


return 0

}

