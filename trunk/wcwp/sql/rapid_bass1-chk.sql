---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc


select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '13989007120'
and send_time >=  current timestamp - 1 days
and date(send_time) = char(current date )
order by send_time desc
;



select * from   table( bass1.chk_wave(0) ) a order by 2

select * from   table( bass1.chk_wave(20120324) ) a order by 2

select val1
,val2-val3
,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
from 
(select ( int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) )- 
 ( int(UNION_MOBILE_LOST_CNT)+int(UNION_NET_LOST_CNT)+int(UNION_FIX_LOST_CNT) ) val1
         ,int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val2
from bass1.G_S_22073_DAY
where time_id=20120322 ) M
,(select int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val3
from bass1.G_S_22073_DAY
where time_id=20120321 ) N 


select ( int(UNION_MOBILE_NEW_ADD_CNT)+int(UNION_NET_NEW_ADD_CNT)+int(UNION_FIX_NEW_ADD_CNT) )- 
         ( int(UNION_MOBILE_LOST_CNT)+int(UNION_NET_LOST_CNT)+int(UNION_FIX_LOST_CNT) ) val1
         ,int(UNION_MOBILE_ARRIVE_CNT)+int(UNION_NET_ARRIVE_CNT)+int(UNION_FIX_ARRIVE_CNT) val2
from bass1.G_S_22073_DAY
where time_id=20120322


select * from G_S_22073_DAY where time_id in(20120322,20120321,20120320)
select * from G_S_22073_DAY where time_id=20120322
select * from app.sch_control_runlog where control_code like 'BASS1%'
select * from   table( bass1.chk_same(0) ) a order by 2


    update bass1.g_s_22012_day set m_off_users='91' 
    where time_id=int(replace(char(current date - 1 days),'-',''))
    
update bass1.g_s_22012_day set m_off_users='133' 
    where time_id=int(replace(char(current date - 1 days),'-',''))
    
    
   select * from  table( bass1.get_after('BASS1_G_S_22073_DAY.tcl')) a 
   select * from  table( bass1.get_before('06040')) a 
G_S_22062_MONTH

                      select user_id,count(*) cnt from bass1.g_a_02008_day
                      where time_id =20120322
                     group by user_id
                     having count(*)>1
 
select  substr(filename,18,5)  from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'



select char(count(distinct comp_product_no)) c36
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in(3,4)),
 
 
 

select OP_TIME , count(0) 
--,  count(distinct OP_TIME ) 
from bass2.Dw_comp_cust_dt 
group by  OP_TIME 
order by 1 

OP_TIME	2	   
2012/3/22 	1914453	   
		
select count(0) from  bass2.Dw_comp_cust_20120322
1914453


select count(0) from  bass2.Dw_comp_cust_dt
1914453




select distinct comp_product_no
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in(3,4)
 

select comp_product_no from (
select distinct comp_product_no
from bass2.Dw_comp_cust_dt
where comp_brand_id in(3,4)
and comp_userstatus_id=1

except

select distinct comp_product_no
from bass2.Dw_comp_cust_20120321
where comp_brand_id in(3,4)
and comp_userstatus_id=1) t
except
select distinct comp_product_no
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in(3,4)


 
 select * from bass1.g_rule_check where rule_code in ('R153','R154') 
and time_id in (20120321,20120322)
order by time_id desc
TIME_ID	RULE_CODE	TARGET1	TARGET2	TARGET3	TARGET4	   
20120322	R154	63.00000	64.00000	-0.01560	0.00000	   
20120322	R153	66.00000	67.00000	-0.01490	0.00000	   
20120321	R153	101.00000	101.00000	0.00000	0.00000	   
20120321	R154	94.00000	94.00000	0.00000	0.00000	   


select val1
,val2-val3
,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
from 
(select ( int(UNION_MOBILE_NEW_ADD_CNT)- int(UNION_MOBILE_LOST_CNT) ) val1
         ,int(UNION_MOBILE_ARRIVE_CNT)  val2
from bass1.G_S_22073_DAY
where time_id=20120322 ) M
,(select int(UNION_MOBILE_ARRIVE_CNT) val3
from bass1.G_S_22073_DAY
where time_id=20120321 ) N
                                        
VAL1	2	RATE	   
63	64	-0.0156	   
			
--UNION_MOBILE_ARRIVE_CNT           
select char(count(distinct comp_product_no)) c31
from bass2.Dw_comp_cust_dt
where comp_brand_id in(3,4)
and comp_userstatus_id=1164667     
--UNION_MOBILE_ARRIVE_CNT
select int(UNION_MOBILE_ARRIVE_CNT) val3
from bass1.G_S_22073_DAY
where time_id=20120321
164603

--UNION_MOBILE_NEW_ADD_CNT
select distinct comp_product_no
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (3,4)     

--UNION_MOBILE_LOST_CNT
select distinct comp_product_no
from bass2.Dw_comp_cust_dt
where comp_day_off_mark=1
 and comp_brand_id in(3,4)
      

--UNION_MOBILE_ARRIVE_CNT - UNION_MOBILE_ARRIVE_CNT
select distinct comp_product_no
from bass2.Dw_comp_cust_dt
where comp_brand_id in(3,4)
and comp_userstatus_id=1     
except
 select distinct comp_product_no
from bass2.Dw_comp_cust_20120321
where comp_brand_id in(3,4)
and comp_userstatus_id=1  
except 
select distinct comp_product_no
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (3,4)  

15692631095
   

select * from  bass2.Dw_comp_cust_dt
where   comp_product_no =  '15692631095'
union all
select * from  bass2.Dw_comp_cust_20120321
where   comp_product_no =  '15692631095'



15692631095 离网又复活，导致到达数多了一个
把这个用户当做新增,把数据调整 UNION_MOBILE_NEW_ADD_CNT +1 ,则其余指标不影响
update(
select * from G_S_22073_DAY where time_id=20120322
) t set UNION_MOBILE_NEW_ADD_CNT = char(int(UNION_MOBILE_NEW_ADD_CNT)+1)


 select char(count(distinct comp_product_no)) c41
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
  C41
2394       


select 
         time_id,
         case when rule_code='R159_1' then '新增客户数'
              when rule_code='R159_2' then '客户到达数'
              when rule_code='R159_3' then '上网本客户数'
              when rule_code='R159_4' then '离网客户数'
         end,
         target1,
         target2,
         target3,target3*100
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_1'
order by 1 desc 



 bass1.g_a_02004_02008_stage  
 
 select count(0)
  from bass1.int_02004_02008_month_stage
  where 
 
 
 select count(0) from  bass1.g_a_02004_02008_stage  
 where CREATE_DATE between '20120201' and '20120229'
 
 62333
  select count(0) from  bass1.g_a_02004_02008_stage  
 where CREATE_DATE between '20120201' and '20120229'
 and USERSTATUS not in ('2010','2020','2030','9000') 
 62202
 
 values (62333-62302)*1.00/62333
 
 
 
 	select *from  app.sch_control_runlog
	where   control_code  = 'BASS1_INT_CHECK_02004_02008_DAY.tcl'
    and begintime >=  current timestamp - 1 days
    
    
 	select *from  app.sch_control_runlog
	where   control_code  like 'BASS1_INT_CHECK_%'
    and begintime >=  current timestamp - 1 days
    
    

select       
c.MO_GROUP_DESC,--模块名
count( case when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then a.control_code end ) ,--未完成数
count( case when   b.flag=1  then a.control_code end ) ,--执行数
count( case when   b.flag=-1 then a.control_code end ) , --执行出错
count( case when date(current timestamp)=date(b.begintime)    and  b.flag=0 then a.control_code end ) 	--完成数
from APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1,3)
group by c.MO_GROUP_DESC,c.sort_id
order by c.sort_id
with ur	



select       
case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then '未完成' 
      when   b.flag=1   then '执行中'
      when   b.flag=-1  then '执行出错'
      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '完成'
      else  '未知'
end,
b.*
,a.FUNCTION_DESC
from  APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1,3)
   and MO_GROUP_DESC like '%一经%'
order by c.sort_id,begintime asc
with ur


select control_Code,begintime,endtime,runtime,
timestampdiff (2, char(timestamp(endtime)- timestamp(begintime))) 
from app.sch_control_runlog

    
    
    select months(current timestamp) from bass2.dual
    
    
    
values    TIMESTAMP ('2002-10-20-12.00.00.000000') 



select enterprise_id,
       prod_id,
       valid_Date,
       expire_date,
       timestampdiff (2, char(timestamp(expire_date,'yyyy-mm-dd 00:00:000000')- timestamp(valid_Date,'yyyy-mm-dd 00:00:000000'))) 
from bass2.dw_enterprise_sub_201202
where prod_id in ('91201001','91201002')
  and rec_status=1
  and date(create_date)>='2012-02-01'
  and date(expire_date)>='2012-02-01'
  
  select timestamp(expire_date) from bass2.dw_enterprise_sub_201202
values timestamp('20100303'||'000000')


  select timestamp(replace(char(expire_date),'-','')||'000000') from bass2.dw_enterprise_sub_201202


  select timestamp(replace(char(expire_date),'-','')||'000000') from bass2.dw_enterprise_sub_201202
  
  select MONTHS_BETWEEN(expire_date,expire_date) from bass2.dw_enterprise_sub_201202
  values MONTHS_BETWEEN ('2008-01-17','2008-02-17') 
  
  
  select (year('2012-01-01')-year('2011-03-01'))*12+(month('2012-01-01')-month('2011-03-01'))
  from bass2.dual
  
  
 
insert into G_S_22080_DAY
select   20120323 TIME_ID
        ,'20120323' OP_TIME
        ,char(bigint(QRY_CNT) - 10 ) QRY_CNT
        ,char(bigint(CANCEL_CNT) - 10 ) CANCEL_CNT
        ,char(bigint(CANCEL_FAIL_CNT) - 5 ) CANCEL_FAIL_CNT
        ,COMPLAINT_CNT 
        ,char(bigint(CANCEL_BUSI_TYPE_CNT) - 5 ) CANCEL_BUSI_TYPE_CNT
 from bass1.G_S_22080_DAY
where time_id = 20120322
 
 
 select * from G_S_22080_DAY where time_id in (20120322,20120323)
 
  select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 order by 1 desc 
 
 
 
                        select * from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
                                        from 
                                        G_A_02054_DAY  t
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN bass1.dim_trans_enterprise_id B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur
                

TIME_ID	ENTERPRISE_ID	ENTERPRISE_BUSI_TYPE	MANAGE_MODE	ORDER_DATE	STATUS_ID	RN	   
20120323	89100000064367      	1330	2	20120324	1	1	   
							
select * from   G_A_01004_DAY where   ENTERPRISE_ID = '89100000064367'
select * from   G_A_02054_DAY where   ENTERPRISE_ID = '89100000064367'


G_A_02054_DAY

select 

select * from 
bass2.dwd_enterprise_msg_20120323
where ENTERPRISE_ID = '89100000064367'


                     
                     
delete from (
select * from   G_A_02054_DAY where   ENTERPRISE_ID = '89100000064367'
)    a
                  
                  

                  
				  

重传 22038,


select  substr(filename,18,5)  from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 2 days),'-','') and err_code='00'
except
select  substr(filename,18,5)  from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'


select count(0) from all_tables


select AUTH_TYPE , count(0) 
--,  count(distinct AUTH_TYPE ) 
from bass2.CDR_WLAN_20120325 
group by  AUTH_TYPE 
order by 1 


select * from bass1.g_rule_check where rule_code in ('93') order by time_id desc
select * from bass1.g_rule_check where rule_code in ('56') order by time_id desc



select * from bass1.g_rule_check where rule_code in ('R173') order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R142') order by time_id desc




select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '15089051890'
and send_time >=  current timestamp - 5 days
--and date(send_time) = char(current date )
order by send_time desc
;


WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE control_code = 'BASS2_Dwd_sys_cmd_def_ds.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.control_code = n.before_control_code
             )
SELECT distinct n.* FROM n
,app.sch_control_task c
where n.control_code = c.control_code
and c.control_code like 'BASS1%'




select * from bass1.g_rule_check where rule_code in ('R140')
update (
select * from app.sch_control_runlog where control_code 
in
('TR1_DMK015'
,'TR1_DMK017'
,'TR1_DMK018'
,'TR1_DMK058'
,'TR1_DMK044'
,'TR1_DMK055'
)
)  t set flag = 0


update (
select * from app.sch_control_alarm where control_code 
in
('TR1_DMK015'
,'TR1_DMK017'
,'TR1_DMK018'
,'TR1_DMK058'
)
and alarmtime >=  current timestamp - 1 days
) 
set 



---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc


select * from app.sch_control_task
where control_code in ('BASS1_G_S_22066_DAY.tcl','BASS1_EXP_G_S_22082_DAY','BASS1_EXP_G_S_22066_DAY')




select * from app.sch_control_runlog where control_code 

   
in (
'BASS1_G_S_22066_DAY.tcl'
,'BASS2_Dw_acct_payitem_dm.tcl'
,'BASS2_Dw_custsvc_ivr_log_dm.tcl'
,'BASS2_Dw_kf_cmd_hint_def_dm.tcl'
,'BASS2_Dw_kf_sms_cmd_receive_dm.tcl'
,'BASS2_Dw_product_busi_dm.tcl'
,'BASS2_Dw_product_ds.tcl'
,'BASS2_Dw_product_ord_cust_dm.tcl'
,'BASS2_Dw_product_ord_so_log_dm.tcl'
,'BASS2_Dw_product_ord_srvpkg_dm.tcl'
,'BASS2_Dwd_ow_loading_log_ds.tcl'
,'BASS2_Dwd_product_ord_busi_other_ds.tcl'
,'BASS2_Dwd_sys_cmd_def_ds.tcl'
,'TR1_L_11060'
)

select * from app.sch_control_runlog where control_code  = 'TR1_L_11054'


PRIORITY_VAL
0

update (
select * from app.sch_control_task
where control_code in ('BASS2_Dwd_sys_cmd_def_ds.tcl')
) t 
set PRIORITY_VAL = 0





---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 3 days
and control_code like 'BASS1%'
order by alarmtime desc


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22056_MONTH 
group by  time_id 
order by 1 


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22048_DAY 
group by  time_id 
order by 1 




select * from bass2.dim_channel_info 
where shop_number is not null

fetch first 10 rows only 

select a.OPPONENT_TYPE,a.* from 
 bass2.dw_opponent_base_info_20120325 a
 where OPPONENT_TYPE = 1
 
 
 
 fetch first 10 rows only 
 select * from g_s_22056_month
 code=804007
1.实体渠道:rel=804008
2.代理渠道:rel=804009
3.直销渠道:rel=804010

select *
        from BASS2.dw_acct_payment_dm_201111 a
        where replace(char(a.OP_TIME),'-','') = '20111127' 
        and OPT_CODE in ('4158','4159')
        fetch first 10 rows only
        
        
        


rename bass1.G_S_22058_MONTH to G_S_22058_MONTH_old;
CREATE TABLE "BASS1   "."G_S_22058_MONTH"  (
                  "TIME_ID" INTEGER , 
                  "OP_TIME" CHAR(6) , 
                  "CHANNEL_ID" CHAR(40) , 
                  "YK_MODE" CHAR(2) , 
                  "YK_CNT" CHAR(12) , 
                  "YK_AMT" CHAR(12) )   
                 DISTRIBUTE BY HASH("OP_TIME","CHANNEL_ID","YK_MODE")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
insert into G_S_22058_MONTH
(
         TIME_ID
        ,OP_TIME
        ,CHANNEL_ID
		,YK_MODE
        ,YK_CNT
        ,YK_AMT
)
select 
         TIME_ID
        ,OP_TIME
        ,CHANNEL_ID
		,'' YK_MODE
        ,YK_CNT
        ,YK_AMT
from G_S_22058_MONTH_old





select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04011_DAY 
group by  time_id 
order by 1 

        
        
        

select * from APP.G_RUNLOG
where time_id = 20120327



select 


select *
from  BASS2.dw_acct_payment_dm_201203  
fetch first 10 rows only



select * from  bass2.dim_cfg_static_data
where code_name like '%二卡合一%'

select * from bass2.dim_cfg_static_data
where code_name like '%银行%'
fetch first 10 rows only


select * from  bass2.dim_cfg_static_data
where code_value in ('4103','4468','','')


select * from bass2.dim_cfg_static_data
where code_name like '%代%'

SELECT * FROM bass2.DIM_ACCT_PAYTYPE
where paytype_name like '%代%'
 and paytype_name not like '%代理%'


 SELECT * FROM bass2.DIM_ACCT_PAYTYPE
where paytype_name like '%扣%'


select *
from 
 
 select * from 
 
 rename G_S_22093_DAY to G_S_22093_DAY_CANCEL_OLD;

CREATE TABLE G_S_22093_DAY (
        TIME_ID                INTEGER             --  记录行号        
        ,CHRG_DT                CHAR(8)             --  缴费日期        
        ,CHRG_TM                CHAR(6)             --  缴费时间        
        ,MSISDN                 CHAR(15)            --  MSISDN          
        ,CHRG_TYPE              CHAR(1)             --  缴费类型        
        ,CHRG_AMT               CHAR(8)             --  缴费金额    
 ) DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX PARTITIONING KEY( TIME_ID,MSISDN ) USING HASHING;



select * from syscat.tables where tabname like 'G_%'


select * from syscat.tables where TABSCHEMA = 'BASS1'



select * from G_S_22093_DAY



select *
from  BASS2.dw_acct_payment_dm_201202
fetch first 10 rows only

DW_ACCT_PAYITEM_


select *
from  BASS2.DW_ACCT_PAYITEM_201202
where paytype_id in ('4238','4238')
fetch first 10 rows only

select 


select *
from 
(
select paytype_id , count(0)  cnt 
--,  count(distinct paytype_id ) 
from BASS2.DW_ACCT_PAYITEM_201202 
group by  paytype_id 
) t , bass2.DIM_ACCT_PAYTYPE b 
where t.paytype_id = b.paytype_id 

1.
GJFY1	19186	GJFY1	邮政帐户缴费	1	   
4103	3427	4103	银行代收费	1	   

2.

3.
4115	29	4115	托收缴费	1	   


select *
from  BASS2.dw_acct_payment_dm_201202
where opt_code in ('GJFY1','4103')
fetch first 10 rows only

select * from bass1.g_rule_check where rule_code in ('C1') 
order by time_id desc


select CERTIFICATE_TYPE , count(0) 
--,  count(distinct CERTIFICATE_TYPE ) 
from BASS2.dw_acct_payment_dm_201202 
group by  CERTIFICATE_TYPE 
order by 1 


11111144




select OPT_CODE , count(0) 
--,  count(distinct OPT_CODE ) 
from BASS2.dw_acct_payment_dm_201202 
where staff_org_id = 11111144
group by  OPT_CODE 
order by 1 


select *
from 
(
select OPT_CODE paytype_id, count(0) cnt 
--,  count(distinct OPT_CODE ) 
from BASS2.dw_acct_payment_dm_201202 
where staff_org_id = 11111144
group by  OPT_CODE 
) t , bass2.DIM_ACCT_PAYTYPE b 
where t.paytype_id = b.paytype_id 



select count(0) from INT_02004_02008_MONTH_STAGE_201112
where SIM_CODE = '2'






CREATE TABLE "BASS1   "."INT_02004_02008_MONTH_STAGE_201112"  (
                  "USER_ID" CHAR(15) , 
                  "PRODUCT_NO" CHAR(15) , 
                  "TEST_FLAG" CHAR(1) , 
                  "SIM_CODE" CHAR(15) , 
                  "USERTYPE_ID" CHAR(4) , 
                  "CREATE_DATE" CHAR(15) , 
                  "BRAND_ID" CHAR(4) , 
                  "TIME_ID" INTEGER )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



insert into bass1.INT_02004_02008_MONTH_STAGE_201112 (
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
                where time_id/100 <= 201112 ) e
                inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                           from bass1.g_a_02008_day
                           where  time_id/100 <= 201112 ) f on f.user_id=e.user_id
                where e.row_id=1 and f.row_id=1

                    
                    
                    
SIM_CODE

select count(0)from G_A_02004_02008_STAGE
where SIM_CODE = '1'
                    
                    
select count(0)
from G_A_02062_DAY 



        select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id <= 20111231
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )


        select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id,USER_ID order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id <= 20111231
			  ) a
			where rn = 1	and STATUS_ID = '1'
            

select count(0) from G_A_02062_DAY
              


select count(distinct a.user_id),
 	                        
 	               from 
 	                  ( select distinct user_id
                               from dw_enterprise_membersub_$year$month 
                               where order_id in (select distinct order_id 
                                                 from bass2.dw_enterprise_sub_$year$month
                                                 where service_id  in ('942','944','946','947','949')
                                                       and valid_date <= '$last_day'
                                                       and expire_date >='$year-$month-$day' )
                                     and valid_date <= '$last_day'
                                     and expire_date >='$year-$month-$day' 
                               ) a,
                      dw_product_$year$month c
                      where a.user_id = c.user_id
                      
                      

select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108'
                      
                      



select count(distinct a.user_id),
 	                        
 	               from 
 	                  ( select distinct user_id
                               from dw_enterprise_membersub_$year$month 
                               where order_id in (select distinct order_id 
                                                 from bass2.dw_enterprise_sub_$year$month
                                                 where service_id  in ('942','944','946','947','949')
                                                       and valid_date <= '$last_day'
                                                       and expire_date >='$year-$month-$day' )
                                     and valid_date <= '$last_day'
                                     and expire_date >='$year-$month-$day' 
                               ) a,
                      dw_product_$year$month c
                      where a.user_id = c.user_id
                      
                      
                      

select userstatus_id,count(0)
from (
        select distinct USER_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id,USER_ID order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id <= 20111231
			  ) a
			where rn = 1	and STATUS_ID = '1'
     ) a , bass2.dw_product_201112 b 
     where a.USER_ID = b.user_id 
     group by userstatus_id
     
                      



                      
                      


select userstatus_id,count(0)
from (
        select distinct USER_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id,USER_ID order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id <= 20120229
			  ) a
			where rn = 1	and STATUS_ID = '1'
     ) a , bass2.dw_product_201202 b 
     where a.USER_ID = b.user_id 
     group by userstatus_id
     
                      
                      

select * from syscat.tables where                       

select * from bass2.Dw_channel_dealer_201202

select  a.CHANNEL_ID,b.REGION_CODE,a.OTHER_INFO ,a.DEALER_NAME,a.PARTNER_CODE
from bass2.Dw_channel_dealer_201202 a
,bass2.dim_channel_info b 
where a.CHANNEL_ID = b.CHANNEL_ID
and a.DEALER_STATE = 1

select  , count(0) 
--,  count(distinct  ) 
from bass2.Dw_channel_dealer_201202DEALER_STATE 
group by   
order by 1 


select DEALER_STATE , count(0) 
--,  count(distinct DEALER_STATE ) 
from bass2.Dw_channel_dealer_201202 
group by  DEALER_STATE 
order by 1 





select PARTNER_CODE , count(0) 
--,  count(distinct PARTNER_CODE ) 
from bass2.Dw_channel_dealer_201202 
group by  PARTNER_CODE 
order by 1 




select count(0) from syscat.tables where tabschema like 'BASS2%'



select * from app.sch_control_task where control_code like '%02048%'


select * from app.sch_control_task where lower(control_code) like '%channel_dealer%'



 select count(0) dup_cnt
        from (
                select count(0)  cnt
                from bass1.G_S_22057_MONTH
                where time_id =201201
                group by OP_TIME||CHANNEL_ID having count(0) > 1
                ) t 
                
select count(0) from G_A_06035_DAY_1
                drop table G_A_06035_DAY_1
                
rename bass1.G_A_06035_DAY_1_old20120331 to G_A_06035_DAY_1

rename bass1.G_A_06035_DAY_1 to G_A_06035_DAY_1_old20120331
CREATE TABLE "BASS1   "."G_A_06035_DAY_1"  (
                  "CHANNEL_ID" CHAR(40) , 
                  "CHANNEL_TYPE" CHAR(1) , 
                  "SELF_24_IND" CHAR(25) , 
                  "CMCC_ID" CHAR(5) , 
                  "COUNTY" CHAR(30) , 
                  "REGION" CHAR(50) , 
                  "CHANNEL_NAME" CHAR(100) , 
                  "CHANNEL_ADDR" CHAR(100) , 
                  "CHNL_MANAGER_NAME" CHAR(40) , 
                  "CHNL_MANAGER_PHONE" CHAR(40) , 
                  "CHNL_PHONE" CHAR(15) , --渠道联系电话                  
                  "GEO_ID" CHAR(1) , 
                  "AREA_TYPE" CHAR(1) , 
                  "CHANNEL_BASE_TYPE" CHAR(1) , 
                  "IF_EX" CHAR(1) , 
                  "IF_MOB_SALEHALL" CHAR(1) , 
                  "IF_PICKUP" CHAR(1) , --是否提供到店取货
                  "IF_SRV_VIP" CHAR(1) , --是否提供VIP服务
                  "IF_SALE_TERM" CHAR(1) , --是否支持终端销售
                  "IF_SRV_ACROSS" CHAR(1) , --是否提供跨区服务                  
                  "CHNL_STAR" CHAR(1) , 
                  "CHNL_STATE" CHAR(1) , 
                  "OPEN_TIME" CHAR(4) , 
                  "CLOSE_TIME" CHAR(4) , 
                  "CONTRACT_EFF_DT" CHAR(8) , 
                  "CONTRACT_END_DT" CHAR(8) , 
                  "CO_OP_DUR" CHAR(4) , 
                  "LONGTITUDE" CHAR(10) , 
                  "LATITUDE" CHAR(10) , 
                  "ZX_INVEST_FEE" CHAR(10) , 
                  "SB_INVEST_FEE" CHAR(10) , 
                  "OFFICE_INVEST_FEE" CHAR(10) , 
                  "SUBSIDY_FEE" CHAR(10) )   
                 DISTRIBUTE BY HASH("CHANNEL_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


                
                
                
                
                select count(0) from G_A_06035_DAY_NEWEST
                
                


RENAME "BASS1   "."G_S_22056_MONTH_OLD20120328"  TO G_S_22056_MONTH

RENAME "BASS1   "."G_S_22056_MONTH"  TO G_S_22056_MONTH_OLD20120328
CREATE TABLE "BASS1   "."G_S_22056_MONTH"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "STATMONTH" CHAR(6) NOT NULL , 
                  "CMCC_ID" CHAR(5) NOT NULL , 
                  "AREA_TYPE" CHAR(1) NOT NULL , 
                  "JINZHENTYPE" CHAR(1) NOT NULL , 
                  "JINZHEN_CHNLTYPE" CHAR(2) NOT NULL , --竞争对手渠道类型
                  "CHANNELCOUNT" CHAR(8) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ;                 
                   
                   
                   

select * from bass2.dw_product_20120331 
where user_id = '89160000184970'
select * from bass2.dw_cust_20120331 
where cust_id = '89103001574212'
89103001574212
                   
                   
select * from G_S_03004_MONTH_ADJ_BAK
                   
                   
                   
select * from bass1.g_rule_check where 
rule_code in ('R174') 
AND TIME_ID / 100 = 201201
order by time_id desc


SELECT COUNT(0) FROM bass2.CDR_GPRS_DTL_20120107
5441128

SELECT COUNT(0) FROM bass2.CDR_GPRS_DTL_20120108
4367338

SELECT COUNT(0) FROM bass2.CDR_GPRS_DTL_20120109
6542305


select 
sum(bigint(UPFLOW1+UPFLOW2))
,sum(bigint(DOWNFLOW1+DOWNFLOW2))
from bass2.CDR_GPRS_DTL_20120109



ods_cdr_gprs_local

                   
                   
select * from syscat.tables where tabname like '%ODS%GPRS%'





 
                 select 
                        20120331 time_id
                        ,'20120331'  CHRG_DT
                        ,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
                        ,key_num MSISDN
                        ,case 
                                when opt_code in ('4103','4144') then '01'
                                when opt_code in ('4115') then '03'
                        end CHRG_TYPE
                        ,char(bigint(AMOUNT)) CHRG_AMT
                from BASS2.dw_acct_payment_dm_201203 a
                where  replace(char(a.OP_TIME),'-','') = '20120331' 
                and OPT_CODE in ('4103','4144','4115')
                with ur
                
                
                   
                   

                select CHRG_NBR, count(0)  cnt
                from bass1.g_i_06040_day
                where time_id =20120331
                group by CHRG_NBR having count(0) > 1

CHRG_NBR
13908961606    
15289014457    
15728909368    
13648963970    


select * from bass1.g_i_06040_day
where   CHRG_NBR = '13908961606'                 


select count(0) from bass1.g_i_06040_day



select time_id,count(0) cnt from bass1.g_user_lst
where time_id / 100 = 2012
group by time_id 
order by 1 desc 
TIME_ID	cnt	   
201204	36284	   
201203	35316	   
201202	34693	   
201201	34121	   



06040	空中充值点基础信息	每日全量	每日11点前	1	

select * from mon_all_interface 
d	22420	垃圾短信黑名单	每日全量	每日13点前	13	1	1	垃圾短信分册 v1.0 start from 20110815 	   
insert into mon_all_interface
select 
'd'
,'06040'
,'空中充值点基础信息'
,'每日全量'
,'每日11点前'
,13
,1
,0
,1
,'1.7.9new add'
from bass2.dual

delete from mon_all_interface
where INTERFACE_CODE = '06040'

insert into mon_all_interface
select 
       'd'  TYPE
        ,'06040' INTERFACE_CODE
        , '空中充值点基础信息' INTERFACE_NAME
        ,'每日全量' COARSE_TYPE
        ,'每日11点前' UPLOAD_TIME
        ,11 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        


22093	银行缴费记录	每日新增	每日13点前
22094	渠道缴费记录	每日新增	每日13点前

insert into mon_all_interface
select 
       'd'  TYPE
        ,'22094' INTERFACE_CODE
        , '渠道缴费记录' INTERFACE_NAME
        ,'每日新增' COARSE_TYPE
        ,'每日13点前' UPLOAD_TIME
        ,13 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        
        
insert into mon_all_interface
select 
       'd'  TYPE
        ,'22093' INTERFACE_CODE
        , '银行缴费记录' INTERFACE_NAME
        ,'每日新增' COARSE_TYPE
        ,'每日13点前' UPLOAD_TIME
        ,13 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        
        
        
02048	WLAN用户（手机）的IMEI信息	每日新增	每日15点前


insert into mon_all_interface
select 
       'd'  TYPE
        ,'02048' INTERFACE_CODE
        , 'WLAN用户（手机）的IMEI信息' INTERFACE_NAME
        ,'每日新增' COARSE_TYPE
        ,'每日15点前' UPLOAD_TIME
        ,15 DEADLINE
        ,1 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        
        

select deadline ,count(0)
from mon_all_interface
where type = 'd'
and sts=1
group by deadline 

DEADLINE	2	   
9	8	   
11	40	   
13	26	   
15	7	   
		
		
        
        
        

select CHRG_DT||CHRG_TM||MSISDN||CHNL_ID||CHRG_TYPE||CHRG_AMT from bass1.G_S_22094_DAY where time_id=20120331
        
       
select * from bass1.G_S_22094_DAY where time_id=20120331

       
       
      
select * from g_s_04003_day where time_id = 20120331


select * from G_S_22047_DAY



select 
c.key_num,c.REMARKS,d.* from bass2.dw_agent_acc_info_20120401 a
join bass2.dwd_channel_dept_20120401  b on a.channel_id = b.organize_id
join bass2.dw_acct_payment_dm_201204 c on  a.mobile_id=c.key_num
left join (select PAYMENT_ID,OPT_CODE,AMOUNT,BALANCE ,PEER_DATE,key_num,remarks
                from bass2.dw_acct_payment_dm_201204
                where  opt_code = 'GTFG' 
          ) d  on c.PAYMENT_ID=d.PAYMENT_ID 
where c.opt_code in ('GJFK','GTFK')
and c.op_time = '2012-04-01'
and c.remarks is null
with ur      



select * from G_S_22094_DAY
where time_id = 20120402




select * from G_I_06040_DAY
where time_id = 20120401


select * from  bass1.g_s_02048_day
where time_id = 20120401

select * from G_S_04003_DAY
where time_id = 20120401



select * from G_A_06035_DAY


select * from G_S_22047_DAY




select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1




select *
from mon_all_interface
where type = 'd'
and sts=1






select 
 b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
, lower(
 'rm *'||b.interface_code||'* ' 
) put_verf
, lower(
 ' *'||b.interface_code||'*dat \' 
) list
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (15)
and type = 'm'
and sts = 1

PRIORITY_VAL
0


update(
select * from app.sch_control_task where control_code = 'BASS1_REPORT'
) t 
set PRIORITY_VAL = 10000





select * from sys


update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110629'
where user_id = '89160001048760'
and time_id = 201203



select * from G_I_02005_MONTH
where user_id = '89160001048760'
and time_id = 201203



select * from bass1.g_rule_check where rule_code in ('R142') AND TIME_ID = 201203




update G_I_21020_MONTH 
set CALL_COUNTS = char(int(rand(1)*5+1))
where (
char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
and CALL_COUNTS = '0'
and SMS_COUNTS = '0'
and MMS_COUNTS = '0'
and time_id = 201203 )




select * from app.sch_control_alarm where content like '%准确性指标56超出集团考核范围%'


select * from bass1.g_rule_check where rule_code in ('R109')
and TIME_ID <= 201204
 order by time_id desc
 



select count(0)
from bass2.stat_channel_reward_0019  a
where op_time = 201203
and CHANNEL_TYPE in (90105,90102)
with ur



select 
        201203 TIME_ID
        ,'201203' OP_MONTH
        ,char(CHANNEL_ID) CHANNEL_ID
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS like '%当月%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%前台缴费酬金%' 
                                or BUSI_NOTES  like '%银行代收费酬金%'  
                                or BUSI_NOTES  like '%营销活动老用户预存费酬金%'  
                                or BUSI_NOTES  like '%邮政公司当月代收费酬金%'  
                                then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-无线音乐俱乐部%' 
                                or BUSI_NOTES  like '%新业务-彩铃%'
                                then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-飞信功能%' 
                                or BUSI_NOTES  like '%新业务-139手机邮箱%元版%'
                                or BUSI_NOTES  like '%号簿管家%'
                                then RESULT else 0 end))) VAL_TYPE2_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%音乐随身听%' 
                                or BUSI_NOTES  like '%歌曲下载%'
                                or BUSI_NOTES  like '%12580生活播报%'
                                or BUSI_NOTES  like '%无线体育俱乐部%'
                                or BUSI_NOTES  like '%信息管家%'
                                or BUSI_NOTES  like '%手机商界%'
                                or BUSI_NOTES  like '%手机医疗%'
                                or BUSI_NOTES  like '%手机地图%'
                                or BUSI_NOTES  like '%手机导航%'
                                or BUSI_NOTES  like '%快讯%'
                                or BUSI_NOTES  like '%手机报%'
                                or BUSI_NOTES  like '%手机阅读%'
                                or BUSI_NOTES  like '%手机视频%'
                                or BUSI_NOTES  like '%手机游戏%'
                                or BUSI_NOTES  like '%手机电视%'
                                then RESULT else 0 end))) VAL_TYPE3_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%移动应用商城%'  then RESULT else 0 end))) VAL_DIANBO
        ,'0' STORE_SUSIDY
        ,'0' SALE_ACTIVE_REWARD
        ,'0' ADD_REWARD
        ,'0' B_CLASS_REWARD
from bass2.stat_channel_reward_0019  a
where op_time = 201203
and CHANNEL_TYPE in (90105,90102)
group by char(CHANNEL_ID)
with ur




select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22062_MONTH 
group by  time_id 
order by 1 




insert into      bass1.G_S_22062_MONTH 
select 
        201203 TIME_ID
        ,'201203' OP_MONTH
        ,char(CHANNEL_ID) CHANNEL_ID
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS like '%当月%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%前台缴费酬金%' 
                                or BUSI_NOTES  like '%银行代收费酬金%'  
                                or BUSI_NOTES  like '%营销活动老用户预存费酬金%'  
                                or BUSI_NOTES  like '%邮政公司当月代收费酬金%'  
                                then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-无线音乐俱乐部%' 
                                or BUSI_NOTES  like '%新业务-彩铃%'
                                then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-飞信功能%' 
                                or BUSI_NOTES  like '%新业务-139手机邮箱%元版%'
                                or BUSI_NOTES  like '%号簿管家%'
                                then RESULT else 0 end))) VAL_TYPE2_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%音乐随身听%' 
                                or BUSI_NOTES  like '%歌曲下载%'
                                or BUSI_NOTES  like '%12580生活播报%'
                                or BUSI_NOTES  like '%无线体育俱乐部%'
                                or BUSI_NOTES  like '%信息管家%'
                                or BUSI_NOTES  like '%手机商界%'
                                or BUSI_NOTES  like '%手机医疗%'
                                or BUSI_NOTES  like '%手机地图%'
                                or BUSI_NOTES  like '%手机导航%'
                                or BUSI_NOTES  like '%快讯%'
                                or BUSI_NOTES  like '%手机报%'
                                or BUSI_NOTES  like '%手机阅读%'
                                or BUSI_NOTES  like '%手机视频%'
                                or BUSI_NOTES  like '%手机游戏%'
                                or BUSI_NOTES  like '%手机电视%'
                                then RESULT else 0 end))) VAL_TYPE3_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%移动应用商城%'  then RESULT else 0 end))) VAL_DIANBO
        ,'0' STORE_SUSIDY
        ,'0' SALE_ACTIVE_REWARD
        ,'0' ADD_REWARD
        ,'0' B_CLASS_REWARD
from bass2.stat_channel_reward_0019  a
where op_time = 201203
and CHANNEL_TYPE in (90105,90102)
group by char(CHANNEL_ID)
with ur



select * from  bass1.G_S_22062_MONTH 
where time_id = 201203

select count(0) from G_S_22062_MONTH 
where time_id = 201203
and channel_id  in (
                select distinct channel_id from G_S_22062_MONTH where time_id = 201203
                except
                select distinct channel_id
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06035_DAY a
                where time_id / 100 <= 201203
                ) t where t.rn =1  and CHNL_STATE = '1'
                ) 
                
                


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_06021_MONTH 
group by  time_id 
order by 1 


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_06021_MONTH 
group by  time_id 
order by 1 


                
                
                

        select * from (
                select distinct CHANNEL_ID 
                from G_I_06021_MONTH 
                where time_id = 201203
                and CHANNEL_TYPE = '1'
                and CHANNEL_STATUS = '1'
                except
                select distinct CHANNEL_ID from 
                G_S_22091_DAY where time_id / 100 = 201203
                ) t


CHANNEL_ID
100002329                               
100003064                               



select * from g_a_06035_day_old20120331
where CHANNEL_ID in ('100002329','100003064')

                
                

select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_06021_MONTH 
group by  time_id 
order by 1 
                
                
               


               
               
               
              
update(
select * from app.sch_control_task where control_code like 'BASS1%'              
AND TIME_VALUE > 312 and time_value < 600
) t set time_value = 316


select control_code from app.sch_control_task where control_code like 'BASS1_%CHECK%' and cc_flag = 1       
except
select control_code from app.sch_control_map

select * from app.sch_control_map where control_code like '%02052%'

update(
select module,program_name,control_code from app.sch_control_map where program_name='G_S_22056_MONTH.tcl' and module=2
) t 
set control_code = 'BASS1_G_S_22056_MONTH.tcl'


select * from app.sch_control_alarm where control_code like '%G_S_22056_MONTH%'






select * from app.sch_control_task where control_code like '%22056%'              


select * from bass1.g_rule_check where rule_code in ('R257') order by time_id desc



BASS1_EXP_G_S_05001_MONTH	BASS1_G_S_05001_MONTH.tcl	   

insert into app.sch_control_before
values ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
;


delete from  app.sch_control_before
where control_code = 'BASS1_G_S_05001_MONTH.tcl'
and before_control_code = 'BASS1_G_I_02032_MONTH.tcl'





insert into app.sch_control_before
values ('BASS1_EXP_G_S_05001_MONTH','BASS1_G_I_02032_MONTH.tcl')
;



select control—



CONTROL_CODE
BASS1_G_A_02052_MONTH.tcl
BASS1_G_I_06031_DAY.tcl
BASS1_G_S_22040_MONTH.tcl
BASS1_G_S_22042_MONTH.tcl
BASS1_G_S_22052_MONTH.tcl
BASS1_G_S_22055_MONTH.tcl
BASS1_G_S_22057_MONTH.tcl
BASS1_G_S_22072_MONTH.tcl
BASS1_G_S_22101_MONTH.tcl
BASS1_G_S_22103_MONTH.tcl
BASS1_G_S_22105_MONTH.tcl
BASS1_G_S_22106_MONTH.tcl


insert into app.sch_control_map
select 2 module, substr(control_code,7) program_name , control_code
from (
select control_code from app.sch_control_task where control_code like 'BASS1_G%' and cc_flag = 1       
except
select control_code from app.sch_control_map
) t

select control_code from app.sch_control_task where control_code like 'BASS1_%CHECK%' and cc_flag = 1       
except
select control_code from app.sch_control_map


MODULE	PROGRAM_NAME	CONTROL_CODE	   
2	G_A_02052_MONTH.tcl	BASS1_G_A_02052_MONTH.tcl	   
2	G_I_06031_DAY.tcl	BASS1_G_I_06031_DAY.tcl	   
2	G_S_22040_MONTH.tcl	BASS1_G_S_22040_MONTH.tcl	   
2	G_S_22042_MONTH.tcl	BASS1_G_S_22042_MONTH.tcl	   
2	G_S_22052_MONTH.tcl	BASS1_G_S_22052_MONTH.tcl	   
2	G_S_22055_MONTH.tcl	BASS1_G_S_22055_MONTH.tcl	   
2	G_S_22057_MONTH.tcl	BASS1_G_S_22057_MONTH.tcl	   
2	G_S_22072_MONTH.tcl	BASS1_G_S_22072_MONTH.tcl	   
2	G_S_22101_MONTH.tcl	BASS1_G_S_22101_MONTH.tcl	   
2	G_S_22103_MONTH.tcl	BASS1_G_S_22103_MONTH.tcl	   
2	G_S_22105_MONTH.tcl	BASS1_G_S_22105_MONTH.tcl	   
2	G_S_22106_MONTH.tcl	BASS1_G_S_22106_MONTH.tcl	   
			
            
            


22057

            select control_code from app.sch_control_map
where control_code like '%22057%'




select * 



select * from app.sch_control_task where control_code like 'BASS1_G%MONTH%' and cc_flag = 1       
AND CMD_LINE NOT LIKE '%`%'


update app.SCH_CONTROL_TASK
set cmd_line = 'int -s INT_CHECK_IMPORTSERV_MONTH.tcl>/bassapp/bass1/tcl/log/INT_CHECK_IMPORTSERV_MONTH.out.r`date +%Y%m`'
where CONTROL_CODE LIKE '%INT_CHECK_IMPORTSERV_MONTH%'




update app.sch_control_task 
set cmd_line = trim(cmd_line) ||'>/bassapp/bass1/tcl/log/'||replace(control_code,'BASS1_','')||'.out.r`date +%Y%m`'
where control_code in (
select control_code  from   app.sch_control_runlog
where control_code like 'BASS1%INT%MONTH.tcl'
AND DATE(BEGINTIME) >= '2011-07-01'
) 


select *
from (
select control_code from app.sch_control_task 
 where control_code like  'BASS1%INT%CHECK%MONTH%' and cc_flag = 1       
except
select control_code from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 2 days
and control_code like 'BASS1%INT%MONTH%'
) t , app.sch_control_runlog  b 
where t.control_code = b.CONTROL_CODE
and date(b.BEGINTIME) > date('2012-03-31')


select count(0) from 
bass2.dw_channel_info_201203


select count(0) from 
bass2.dw_channel_info_201202




select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 



select * from bass2.dw_product_20120404
where 
select * from app.sch_control_task where control_code = 'BASS1_G_I_02032_MONTH.tcl'



/bassapp/bass2/ngbass2/tcl



select * from app.sch_control_task where APP_DIR like '%ngbass2%' 


update(
select * from app.sch_control_task where control_code = 'BASS1_G_I_02032_MONTH.tcl'
) t 
set  PRIORITY_VAL = 10000




insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201203


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201203



select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 






select 
         time_id
         ,target2 新增客户数
from bass1.g_rule_check
where 
   rule_code='R159_1'
   and time_id / 100 = 201203
order by 1 desc 




	update       (select * from  g_i_06001_month where time_id = 201203 )
	set      stud_cnt = '7000'
	where      SCHOOL_ID = '89189400000001'
	



	update       (select * from  g_i_06001_month where time_id = 201203 )
	set      stud_cnt = '3000'
	where      SCHOOL_ID = '89189100000001'
	
    
    SCHOOL_ID
89189400000001      
89189100000001      



STUD_CNT	SCHOOL_ID	CNT	   
5000  	89189400000001      	5102	   
2400  	89189100000001      	2539	   
			
select count(0) from (
select a.stud_cnt
,b.*
from (select * from  g_i_06001_month where time_id = 201203 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201203
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) t






insert into app.sch_control_map
select 2 module, substr(control_code,7) program_name , control_code
from (
select control_code from app.sch_control_task where control_code like 'BASS1_G%' and cc_flag = 1       
except
select control_code from app.sch_control_map
) t

select control_code from app.sch_control_task where control_code like 'BASS1_%CHECK%' and cc_flag = 1       
except
select control_code from app.sch_control_map




syscat  



select * from syscat.tables where tabname like '%77780%'


select * from bass2.dim_prod_up_product_item where name like '%幸福卡%'


select * from BASS1.DIM_QW_QQT_PKGID


select * from bass1.ALL_DIM_LKP where bass1_tbid in ('BASS_STD1_0114','BASS_STD1_0115')

select offer_id , count(0)
from 
bass2.dw_product_ins_off_ins_prod_201203
where offer_id in 
(
111090001716
,111089150020
,111089250020
,111089350020
,111089450020
,111089550020
,111089650020
,111089750020
)
group by offer_id


111090001716

111090001716

111090001716	神州行幸福卡亲情号码促销



select * from BASS1.G_I_02026_MONTH_LOAD

insert into  BASS1.G_I_02026_MONTH_LOAD
select 
'111090001716'
,'999914311440009001'
,'神州行幸福卡亲情号码促销'
from bass2.dual



insert into bass1.ALL_DIM_LKP
select 
         XZBAS_TBNAME
        ,'神州行幸福卡亲情号码促销' XZBAS_COLNAME
        ,'111090001716' XZBAS_VALUE
        ,'神州行幸福卡统一套餐编码' BASS1_TBN_DESC
        ,'-' BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
        from bass2.dual
        
        

select * from G_I_02026_MONTH where time_id = 201203
and pkg_id = '999914311440009001'
        
        

select * from G_I_02026_MONTH where time_id = 201203
and pkg_name like '%幸福卡%'

        
        
        

select * from syscat.triggers
where text like '%周期%'
        
        
        

select * from bass2.ETL_TASK_LOG
fetch first 10 rows only
        
        
select * from BASS2.ETL_SEND_MESSAGE
        
        

select * from G_I_02027_MONTH where time_id = 201203
and pkg_id = '999914311440009001'
        

        
        

89160001888926      


select user_id,cust_id,userstatus_id ,test_mark
from bass2.dw_product_20120406
where user_id = '89160001888926'


89160001208781

select * from bass2.dw_cust_20120406        
where cust_id = '89160001208781'


select *
from g_i_02020_month where time_id = 201203
fetch first 100 rows only

select * from bass1.mon_all_interface
where interface_code in ('06023','02022')

select count(0) ,count(distinct user_id)
from G_I_02022_DAY where time_id = 20120331

97100	97100	   

select count(0) ,count(distinct a.user_id)
from (select * from g_i_02020_month where time_id = 201203 ) a
, bass1.int_02004_02008_month_stage b 
where a.user_id = b.USER_ID and b.USERTYPE_ID   not in ('2010','2020','2030','9000') 
and base_prod_id like '9999%'


1	2	   
97100	97100	   
		
        
        
        

9999




select count(0) ,count(distinct user_id)
from G_I_02023_DAY where time_id = 20120331

1	2	   
15956	15596	   


select *
from g_i_02021_month where time_id = 201203
fetch first 100 rows only		
        
        
        select count(0) ,count(distinct a.user_id)
from (select * from g_i_02021_month where time_id = 201203 ) a
,bass1.int_02004_02008_month_stage b 
where a.user_id = b.USER_ID and b.USERTYPE_ID   not in ('2010','2020','2030','9000') 
and over_prod_id like '9999%' 
1	2	   
15956	15596	   
		
        
update (        
select * from bass1.mon_all_interface
where interface_code in ('02026','02027')
) t 
set upload_time = '每月10日前'


        
        
        
update (        
select * from bass1.mon_all_interface
where interface_code in ('02026','02027')
) t 
set deadline = 10


        
        


select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 




select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join (select * from bass1.mon_all_interface 
                where sts = 1 and type = 'm') c on substr(a.filename,16,5) = c.INTERFACE_CODE 
with ur
        
        


mret:

select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join (select * from bass1.mon_all_interface 
                where sts = 1 and type = 'm') c on substr(a.filename,16,5) = c.INTERFACE_CODE 
with ur
        
        
        

ROAMTYPE_ID

select count(0) from  bass2.CDR_GPRS_LOCAL_20120407 where ROAMTYPE_ID = 2

        
        

89160000191125      
89160000191802      

select *
from bass2.dw_product_20120408
where user_id in ('89160000191125','89160000191802')        

89103001574212

select * from bass2.dw_cust_20120408
where cust_id = '89103001574212'






select distinct substr(a.filename,16,5)
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 2 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 2 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 
except
select distinct substr(a.filename,16,5)
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 
except
select distinct substr(a.filename,16,5)
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 2 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 2 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 




select *
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 2 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 2 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 


G_S_22073_DAY

			select val1
						,val2-val3
						,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
			from
			 (select ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) )- 
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) val1
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val2
				from bass1.G_S_22073_DAY
				where time_id=$timestamp ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id=$last_day ) N
                


select time_id,( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) val1
,TEL_MOBILE_LOST_CNT
,TEL_NET_LOST_CNT
,TEL_FIX_LOST_CNT
from G_S_22073_DAY
where time_id /100 = 201204



                
select * from app.sch_control_runlog where control_code in (                
   select before_control_code  from  table( bass1.get_before('BASS2_Dw_channel_dealer_ds')) a 
                )
                
                
                
update (
select * from app.sch_control_task where control_code = 'BASS2_Dw_channel_dealer_ds.tcl'
) t
set PRIORITY_VAL = 50


0



 select char(count(distinct comp_product_no)) c41
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
  
  
  
  
 select char(count(distinct comp_product_no)) c41
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
  
  
  select char(count(distinct comp_product_no)) c51
from bass2.Dw_comp_cust_20120408
where comp_day_off_mark=1
 and comp_brand_id in (9,10,11)
 
 
 select *
 from (
 select distinct COMP_LAST_DATE,comp_product_no
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
 ) a , 
 
  (
  select distinct COMP_LAST_DATE,comp_product_no
from bass2.Dw_comp_cust_20120410
where comp_brand_id in (9,10,11)
 ) b where a.comp_product_no = b.comp_product_no
 
 COMP_PRODUCT_NO	COMP_PRODUCT_NO	   
15348974170	15348974170	   
13308953602	13308953602	   
		
        
        

select *
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)




                   
select * from bass1.g_rule_check where 
rule_code in ('R170','R172') 
AND TIME_ID / 100 = 201204
order by 1,2 desc


20120411	R172	2394.00000	553.00000	3.32910	0.00000	   
20120411	R170	2931.00000	1021.00000	1.87070	0.00000	   



select COMP_DAY_NEW_MARK,COMP_DAY_OFF_MARK,COMP_ACTIVE_MARK , count(0) 
--,  count(distinct COMP_DAY_NEW_MARK,COMP_DAY_OFF_MARK,COMP_ACTIVE_MARK ) 
from bass2.Dw_comp_cust_20120411 
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
group by  COMP_DAY_NEW_MARK,COMP_DAY_OFF_MARK,COMP_ACTIVE_MARK 
order by 1 


select
case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end call
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end sms1
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end sms3
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end sms3
,count(0)
from bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
group by 
case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end 
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end 






select TEL_MOBILE_ARRIVE_CNT,TEL_MOBILE_NEW_ADD_CNT,TEL_MOBILE_LOST_CNT ,TEL_133_NEW_ADD_CNT 
,TEL_189_NEW_ADD_CNT
from g_s_22073_day
where time_id/100 = 201204






CREATE TABLE "BASS1   "."G_S_22073_DAY_BAK20120412"  (
                  "TIME_ID" INTEGER , 
                  "BILL_TIME" CHAR(8) , 
                  "D_UNION_MOBILE_IN_CALL_CNT" CHAR(10) , 
                  "D_UNION_156_IN_CALL_CNT" CHAR(10) , 
                  "D_UNION_FIX_IN_CALL_CNT" CHAR(10) , 
                  "D_UNION_NET_IN_CALL_CNT" CHAR(10) , 
                  "D_UNION_MOBILE_OUT_CALL_CNT" CHAR(10) , 
                  "D_UNION_156_OUT_CALL_CNT" CHAR(10) , 
                  "D_UNION_FIX_OUT_CALL_CNT" CHAR(10) , 
                  "D_UNION_NET_OUT_CALL_CNT" CHAR(10) , 
                  "D_TEL_MOBILE_IN_CALL_CNT" CHAR(10) , 
                  "D_TEL_133_IN_CALL_CNT" CHAR(10) , 
                  "D_TEL_189_IN_CALL_CNT" CHAR(10) , 
                  "D_TEL_FIX_IN_CALL_CNT" CHAR(10) , 
                  "D_TEL_NET_IN_CALL_CNT" CHAR(10) , 
                  "D_TEL_MOBILE_OUT_CALL_CNT" CHAR(10) , 
                  "D_TEL_133_OUT_CALL_CNT" CHAR(10) , 
                  "D_TEL_189_OUT_CALL_CNT" CHAR(10) , 
                  "D_TEL_FIX_OUT_CALL_CNT" CHAR(10) , 
                  "D_TEL_NET_OUT_CALL_CNT" CHAR(10) , 
                  "M_UNION_MOBILE_CNT" CHAR(10) , 
                  "M_UNION_FIX_CNT" CHAR(10) , 
                  "M_UNION_NET_CNT" CHAR(10) , 
                  "M_TEL_MOBILE_CNT" CHAR(10) , 
                  "M_TEL_FIX_CNT" CHAR(10) , 
                  "M_TEL_NET_CNT" CHAR(10) , 
                  "UNION_MOBILE_NEW_ADD_CNT" CHAR(10) , 
                  "UNION_156_NEW_ADD_CNT" CHAR(10) , 
                  "UNION_186_NEW_ADD_CNT" CHAR(10) , 
                  "UNION_NET_NEW_ADD_CNT" CHAR(10) , 
                  "UNION_FIX_NEW_ADD_CNT" CHAR(10) , 
                  "UNION_MOBILE_ARRIVE_CNT" CHAR(10) , 
                  "UNION_156_ARRIVE_CNT" CHAR(10) , 
                  "UNION_186_ARRIVE_CNT" CHAR(10) , 
                  "UNION_NET_ARRIVE_CNT" CHAR(10) , 
                  "UNION_FIX_ARRIVE_CNT" CHAR(10) , 
                  "UNION_MOBILE_LOST_CNT" CHAR(10) , 
                  "UNION_156_LOST_CNT" CHAR(10) , 
                  "UNION_186_LOST_CNT" CHAR(10) , 
                  "UNION_NET_LOST_CNT" CHAR(10) , 
                  "UNION_FIX_LOST_CNT" CHAR(10) , 
                  "TEL_MOBILE_NEW_ADD_CNT" CHAR(10) , 
                  "TEL_133_NEW_ADD_CNT" CHAR(10) , 
                  "TEL_189_NEW_ADD_CNT" CHAR(10) , 
                  "TEL_NET_NEW_ADD_CNT" CHAR(10) , 
                  "TEL_FIX_NEW_ADD_CNT" CHAR(10) , 
                  "TEL_MOBILE_ARRIVE_CNT" CHAR(10) , 
                  "TEL_133_ARRIVE_CNT" CHAR(10) , 
                  "TEL_189_ARRIVE_CNT" CHAR(10) , 
                  "TEL_NET_ARRIVE_CNT" CHAR(10) , 
                  "TEL_FIX_ARRIVE_CNT" CHAR(10) , 
                  "TEL_MOBILE_LOST_CNT" CHAR(10) , 
                  "TEL_133_LOST_CNT" CHAR(10) , 
                  "TEL_189_LOST_CNT" CHAR(10) , 
                  "TEL_NET_LOST_CNT" CHAR(10) , 
                  "TEL_FIX_LOST_CNT" CHAR(10) )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 


3.329113924	
	
	
(x-553)/553 = 0.4	
	
774.2	

775

2394      
1619


INSERT INTO G_S_22073_DAY_BAK20120411
SELECT * FROM G_S_22073_DAY

update (
select * from G_S_22073_DAY where time_id = 20120411
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 1620)
;


update (
select * from G_S_22073_DAY where time_id = 20120411
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 1620)



select 
         time_id,TEL_MOBILE_NEW_ADD_CNT
        ,TEL_133_NEW_ADD_CNT
        ,TEL_189_NEW_ADD_CNT
        ,TEL_NET_NEW_ADD_CNT
        ,TEL_FIX_NEW_ADD_CNT
        ,TEL_MOBILE_ARRIVE_CNT
        ,TEL_133_ARRIVE_CNT
        ,TEL_189_ARRIVE_CNT
        ,TEL_NET_ARRIVE_CNT
        ,TEL_FIX_ARRIVE_CNT
        ,TEL_MOBILE_LOST_CNT
        ,TEL_133_LOST_CNT
        ,TEL_189_LOST_CNT
        ,TEL_NET_LOST_CNT
        ,TEL_FIX_LOST_CNT
        from G_S_22073_DAY
        where time_id/ 100 = 201204
        
        
        

select current date - 90  days from bass2.dual





  
  select *
from bass2.Dw_comp_cust_20120408
where comp_day_off_mark=1
 and comp_brand_id in (9,10,11)
 and length(comp_product_no) = 12
 
 
 
 
 
   
  select *
from bass2.Dw_comp_cust_20120408
where length(comp_product_no) = 12
 
 
 
 select * from bass1.g_rule_check where rule_code in ('R155','R156','R164','R166','R170','R172') 
 AND TIME_ID in( 20120412,20120411)
 order by 2 desc,1 desc


select * from G_S_22073_DAY where time_id in(20120412,20120411)




INSERT INTO G_S_22073_DAY_BAK20120412
SELECT * FROM G_S_22073_DAY

update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 1620)
;


update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 1620)


774.00000

values 774+0.4*774

1083
7996.00000
6913


update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 6913)
;


update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 6913)


update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 1620)


select 
m.*
,n.*
,m.TEL_MOBILE_ARRIVE_CNT - n.TEL_MOBILE_ARRIVE_CNT
--,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
from
(select ( int(TEL_MOBILE_NEW_ADD_CNT)- int(TEL_MOBILE_LOST_CNT) ) val1
,int(TEL_MOBILE_NEW_ADD_CNT) TEL_MOBILE_NEW_ADD_CNT, int(TEL_MOBILE_LOST_CNT) TEL_MOBILE_LOST_CNT
,int(TEL_MOBILE_ARRIVE_CNT)  TEL_MOBILE_ARRIVE_CNT
from bass1.G_S_22073_DAY
where time_id=20120412 ) M
,(select int(TEL_MOBILE_ARRIVE_CNT) TEL_MOBILE_ARRIVE_CNT
from bass1.G_S_22073_DAY
where time_id=20120411 ) N
                                
VAL1	TEL_MOBILE_NEW_ADD_CNT	TEL_MOBILE_LOST_CNT	TEL_MOBILE_ARRIVE_CNT	TEL_MOBILE_ARRIVE_CNT	6	   
644	1083	439	323735	321471	2264	   
						
values 2264-644


select 
m.*
,n.*
,arrive1 - arrive2
from
(select 
( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) )- 
( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) pureadd
,( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) ) add
,( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) lost
,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) arrive1
from bass1.G_S_22073_DAY
where time_id=20120412 ) M
,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) arrive2
from bass1.G_S_22073_DAY
where time_id=20120411 ) N

PUREADD	ADD	LOST	ARRIVE1	ARRIVE2	6	   
839	1657	818	553701	551242	2459	   
					

                    
                    


 select *
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
                   

                   
                   



 select  substr(COMP_PRODUCT_NO,1,3) seg
 ,COMP_CITY_ID
 ,COMP_COUNTY_ID
 ,COMP_BRAND_ID
 ,COMP_USERSTATUS_ID
 ,COMP_FIRST_OPEN_DATE
 ,COMP_LAST_DATE
 ,COMP_DAY_NEW_MARK
,case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end call
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end sms1
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end sms3
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end sms3
,count(0) cnt
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
group by 
 substr(COMP_PRODUCT_NO,1,3) 
 ,COMP_CITY_ID
 ,COMP_COUNTY_ID
 ,COMP_BRAND_ID
 ,COMP_USERSTATUS_ID
 ,COMP_FIRST_OPEN_DATE
 ,COMP_LAST_DATE
 ,COMP_DAY_NEW_MARK
,case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end 
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end 
                   
                   


select
case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end call
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end sms1
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end sms3
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end sms3
,count(0)
from bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
group by 
case when DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0 or DAY_MT_SMS_COUNTS > 0 then 1 else 0 end 
,case when DAY_MO_SMS_COUNTS > 0  then 1 else 0 end 
,case when DAY_MT_SMS_COUNTS > 0  then 1 else 0 end 



18089023311


select * from bass2.cdr_call_20120412
where OPP_NUMBER = '18089014449'



select * from bass2.cdr_call_20120411
where OPP_NUMBER = '18008988777'


18089022387   




select * from bass2.cdr_call_20120411
where OPP_NUMBER = '18008988777'




select * from bass2.cdr_sms_20120411
where OPP_NUMBER = '18089026019'

18089029992   

18008988777

select * from bass2.cdr_call_20120411
where OPP_NUMBER = '18008988777'
 select *
 from  bass2.Dw_comp_cust_20120411
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
  and comp_product_no like '180%'
  and not ( DAY_IN_CALL_COUNTS > 0 or DAY_OUT_CALL_COUNTS > 0 )
  
  
  
  select * from bass2.cdr_sms_20120411
where OPP_NUMBER = '18089037008'


---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc


