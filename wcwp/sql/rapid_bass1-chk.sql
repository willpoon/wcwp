---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc



select * from   table( bass1.chk_wave(20120324) ) a order by 2

select * from   table( bass1.chk_wave(0) ) a order by 2


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


    update bass1.g_s_22012_day set m_off_users='' 
    where time_id=int(replace(char(current date - 1 days),'-',''))
    
update bass1.g_s_22012_day set m_off_users='133' 
    where time_id=int(replace(char(current date - 1 days),'-',''))
    
    
   select * from  table( bass1.get_after('02054')) a 

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


select * from bass1.g_rule_check where rule_code in ('R109') order by time_id desc



select * from bass1.g_rule_check where rule_code in ('R107') order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc




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
                
                

/
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