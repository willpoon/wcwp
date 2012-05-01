---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc


BASS2_Dim_channel_info_ds.tcl

select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS2_Dim_channel_info_ds%'
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
   select * from  table( bass1.get_before('TR1_L_A98012')) a 

G_S_22062_MONTH

                      select user_id,count(*) cnt from bass1.g_a_02008_day
                      where time_id =20120322
                     group by user_id
                     having count(*)>1
 
select  substr(filename,18,5)  from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'


select * from APP.G_FILE_REPORT
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



15692631095 �����ָ�����µ���������һ��
������û���������,�����ݵ��� UNION_MOBILE_NEW_ADD_CNT +1 ,������ָ�겻Ӱ��
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
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
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
c.MO_GROUP_DESC,--ģ����
count( case when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then a.control_code end ) ,--δ�����
count( case when   b.flag=1  then a.control_code end ) ,--ִ����
count( case when   b.flag=-1 then a.control_code end ) , --ִ�г���
count( case when date(current timestamp)=date(b.begintime)    and  b.flag=0 then a.control_code end ) 	--�����
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
case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then 'δ���' 
      when   b.flag=1   then 'ִ����'
      when   b.flag=-1  then 'ִ�г���'
      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '���'
      else  'δ֪'
end,
b.*
,a.FUNCTION_DESC
from  APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1,3)
   and MO_GROUP_DESC like '%һ��%'
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
                  
                  

                  
				  

�ش� 22038,


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
where control_code in ('BASS1_INT_CHECK_R258_DAY.tcl')




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
1.ʵ������:rel=804008
2.��������:rel=804009
3.ֱ������:rel=804010

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
where code_name like '%������һ%'

select * from bass2.dim_cfg_static_data
where code_name like '%����%'
fetch first 10 rows only


select * from  bass2.dim_cfg_static_data
where code_value in ('4103','4468','','')


select * from bass2.dim_cfg_static_data
where code_name like '%��%'

SELECT * FROM bass2.DIM_ACCT_PAYTYPE
where paytype_name like '%��%'
 and paytype_name not like '%����%'


 SELECT * FROM bass2.DIM_ACCT_PAYTYPE
where paytype_name like '%��%'


select *
from 
 
 select * from 
 
 rename G_S_22093_DAY to G_S_22093_DAY_CANCEL_OLD;

CREATE TABLE G_S_22093_DAY (
        TIME_ID                INTEGER             --  ��¼�к�        
        ,CHRG_DT                CHAR(8)             --  �ɷ�����        
        ,CHRG_TM                CHAR(6)             --  �ɷ�ʱ��        
        ,MSISDN                 CHAR(15)            --  MSISDN          
        ,CHRG_TYPE              CHAR(1)             --  �ɷ�����        
        ,CHRG_AMT               CHAR(8)             --  �ɷѽ��    
 ) DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX PARTITIONING KEY( TIME_ID,MSISDN ) USING HASHING;



select * from syscat.tables where tabname like '%DIM_CHANNEL_INFO%'



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
GJFY1	19186	GJFY1	�����ʻ��ɷ�	1	   
4103	3427	4103	���д��շ�	1	   

2.

3.
4115	29	4115	���սɷ�	1	   


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
                  "CHNL_PHONE" CHAR(15) , --������ϵ�绰                  
                  "GEO_ID" CHAR(1) , 
                  "AREA_TYPE" CHAR(1) , 
                  "CHANNEL_BASE_TYPE" CHAR(1) , 
                  "IF_EX" CHAR(1) , 
                  "IF_MOB_SALEHALL" CHAR(1) , 
                  "IF_PICKUP" CHAR(1) , --�Ƿ��ṩ����ȡ��
                  "IF_SRV_VIP" CHAR(1) , --�Ƿ��ṩVIP����
                  "IF_SALE_TERM" CHAR(1) , --�Ƿ�֧���ն�����
                  "IF_SRV_ACROSS" CHAR(1) , --�Ƿ��ṩ��������                  
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
                  "JINZHEN_CHNLTYPE" CHAR(2) NOT NULL , --����������������
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



06040	���г�ֵ�������Ϣ	ÿ��ȫ��	ÿ��11��ǰ	1	

select * from mon_all_interface 
d	22420	�������ź�����	ÿ��ȫ��	ÿ��13��ǰ	13	1	1	�������ŷֲ� v1.0 start from 20110815 	   
insert into mon_all_interface
select 
'd'
,'06040'
,'���г�ֵ�������Ϣ'
,'ÿ��ȫ��'
,'ÿ��11��ǰ'
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
        , '���г�ֵ�������Ϣ' INTERFACE_NAME
        ,'ÿ��ȫ��' COARSE_TYPE
        ,'ÿ��11��ǰ' UPLOAD_TIME
        ,11 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        


22093	���нɷѼ�¼	ÿ������	ÿ��13��ǰ
22094	�����ɷѼ�¼	ÿ������	ÿ��13��ǰ

insert into mon_all_interface
select 
       'd'  TYPE
        ,'22094' INTERFACE_CODE
        , '�����ɷѼ�¼' INTERFACE_NAME
        ,'ÿ������' COARSE_TYPE
        ,'ÿ��13��ǰ' UPLOAD_TIME
        ,13 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        
        
insert into mon_all_interface
select 
       'd'  TYPE
        ,'22093' INTERFACE_CODE
        , '���нɷѼ�¼' INTERFACE_NAME
        ,'ÿ������' COARSE_TYPE
        ,'ÿ��13��ǰ' UPLOAD_TIME
        ,13 DEADLINE
        ,0 IF_EMPTY
        ,1 STS
        ,'1.7.9new add' REMARKS
from bass2.dual
        
        
        
02048	WLAN�û����ֻ�����IMEI��Ϣ	ÿ������	ÿ��15��ǰ


insert into mon_all_interface
select 
       'd'  TYPE
        ,'02048' INTERFACE_CODE
        , 'WLAN�û����ֻ�����IMEI��Ϣ' INTERFACE_NAME
        ,'ÿ������' COARSE_TYPE
        ,'ÿ��15��ǰ' UPLOAD_TIME
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
deadline  in (11)
and type = 'd'
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




select * from app.sch_control_alarm where content like '%׼ȷ��ָ��56�������ſ��˷�Χ%'


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
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS like '%����%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%ǰ̨�ɷѳ��%' 
                                or BUSI_NOTES  like '%���д��շѳ��%'  
                                or BUSI_NOTES  like '%Ӫ������û�Ԥ��ѳ��%'  
                                or BUSI_NOTES  like '%������˾���´��շѳ��%'  
                                then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-�������־��ֲ�%' 
                                or BUSI_NOTES  like '%��ҵ��-����%'
                                then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-���Ź���%' 
                                or BUSI_NOTES  like '%��ҵ��-139�ֻ�����%Ԫ��%'
                                or BUSI_NOTES  like '%�Ų��ܼ�%'
                                then RESULT else 0 end))) VAL_TYPE2_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%����������%' 
                                or BUSI_NOTES  like '%��������%'
                                or BUSI_NOTES  like '%12580�����%'
                                or BUSI_NOTES  like '%�����������ֲ�%'
                                or BUSI_NOTES  like '%��Ϣ�ܼ�%'
                                or BUSI_NOTES  like '%�ֻ��̽�%'
                                or BUSI_NOTES  like '%�ֻ�ҽ��%'
                                or BUSI_NOTES  like '%�ֻ���ͼ%'
                                or BUSI_NOTES  like '%�ֻ�����%'
                                or BUSI_NOTES  like '%��Ѷ%'
                                or BUSI_NOTES  like '%�ֻ���%'
                                or BUSI_NOTES  like '%�ֻ��Ķ�%'
                                or BUSI_NOTES  like '%�ֻ���Ƶ%'
                                or BUSI_NOTES  like '%�ֻ���Ϸ%'
                                or BUSI_NOTES  like '%�ֻ�����%'
                                then RESULT else 0 end))) VAL_TYPE3_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ƶ�Ӧ���̳�%'  then RESULT else 0 end))) VAL_DIANBO
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
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS like '%����%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ź�%' and MONTHS not like '%����%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%ǰ̨�ɷѳ��%' 
                                or BUSI_NOTES  like '%���д��շѳ��%'  
                                or BUSI_NOTES  like '%Ӫ������û�Ԥ��ѳ��%'  
                                or BUSI_NOTES  like '%������˾���´��շѳ��%'  
                                then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-�������־��ֲ�%' 
                                or BUSI_NOTES  like '%��ҵ��-����%'
                                then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%��ҵ��-���Ź���%' 
                                or BUSI_NOTES  like '%��ҵ��-139�ֻ�����%Ԫ��%'
                                or BUSI_NOTES  like '%�Ų��ܼ�%'
                                then RESULT else 0 end))) VAL_TYPE2_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%����������%' 
                                or BUSI_NOTES  like '%��������%'
                                or BUSI_NOTES  like '%12580�����%'
                                or BUSI_NOTES  like '%�����������ֲ�%'
                                or BUSI_NOTES  like '%��Ϣ�ܼ�%'
                                or BUSI_NOTES  like '%�ֻ��̽�%'
                                or BUSI_NOTES  like '%�ֻ�ҽ��%'
                                or BUSI_NOTES  like '%�ֻ���ͼ%'
                                or BUSI_NOTES  like '%�ֻ�����%'
                                or BUSI_NOTES  like '%��Ѷ%'
                                or BUSI_NOTES  like '%�ֻ���%'
                                or BUSI_NOTES  like '%�ֻ��Ķ�%'
                                or BUSI_NOTES  like '%�ֻ���Ƶ%'
                                or BUSI_NOTES  like '%�ֻ���Ϸ%'
                                or BUSI_NOTES  like '%�ֻ�����%'
                                then RESULT else 0 end))) VAL_TYPE3_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%�ƶ�Ӧ���̳�%'  then RESULT else 0 end))) VAL_DIANBO
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
                
                


select time�e_id , count(0) 
--,  count(distinct time�e_id ) 
from G_I_06021_MONTH 
group by  time�e_id 
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



select control��



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
         ,target2 �����ͻ���
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


select * from bass2.dim_prod_up_product_item where name like '%�Ҹ���%'


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

111090001716	�������Ҹ�������������



select * from BASS1.G_I_02026_MONTH_LOAD

insert into  BASS1.G_I_02026_MONTH_LOAD
select 
'111090001716'
,'999914311440009001'
,'�������Ҹ�������������'
from bass2.dual



insert into bass1.ALL_DIM_LKP
select 
         XZBAS_TBNAME
        ,'�������Ҹ�������������' XZBAS_COLNAME
        ,'111090001716' XZBAS_VALUE
        ,'�������Ҹ���ͳһ�ײͱ���' BASS1_TBN_DESC
        ,'-' BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
        from bass2.dual
        
        

select * from G_I_02026_MONTH where time_id = 201203
and pkg_id = '999914311440009001'
        
        

select * from G_I_02026_MONTH where time_id = 201203
and pkg_name like '%�Ҹ���%'

        
        
        

select * from syscat.triggers
where text like '%����%'
        
        
        

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
set upload_time = 'ÿ��10��ǰ'


        
        
        
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



select * from bass1.g_rule_check where rule_code in ('R155','R156','R164','R166','R170','R172') 
 AND TIME_ID in( 20120412,20120413)
 order by 2 desc,1 desc





--δ���
select       
case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then 'δ���' 
      when   b.flag=1   then 'ִ����'
      when   b.flag=-1  then 'ִ�г���'
      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '���'
      else  'δ֪'
end,
b.*
,a.FUNCTION_DESC
from  APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1,3)
   and MO_GROUP_DESC like '%һ��%'
   and (date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 )
order by c.sort_id,begintime asc
with ur



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
where time_id=20120413 ) M
,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) arrive2
from bass1.G_S_22073_DAY
where time_id=20120412 ) N

PUREADD	ADD	LOST	ARRIVE1	ARRIVE2	6	   
839	1657	818	553701	551242	2459	   
					

                    
                    


update (
select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 6913-1620)


select * from app.sch_control_runlog where control_code = 'TR1_L_A98012'



select count(0) from                           bass2.DWD_MR_OPER_CDR_20120412


delete from bass1.G_S_04015_DAY where time_id = 20120413
insert into bass1.G_S_04015_DAY
                           (
                             TIME_ID           ,
                             BILL_TYPE         ,
                             PRODUCT_NO        ,
                             PRTY_CODE         ,
                             SP_ID             ,
                             SERV_CODE         ,                             
                             CUSTOM_CHNL       ,                             
                             MEET_NUM          ,                             
                             PREZENT_BEGIN_TIME,
                             PREZENT_END_TIME  ,                             
                             FEE_AMT           ,                             
                             BUSI_TYPE         ,
                             DEAL_TIME         ,
                             CUST_CONTE         
                           )
                         select
                           20120413
                           ,char(FEETYPE_ID)  
                           ,ltrim(rtrim(PRODUCT_NO))
                           ,value(ltrim(rtrim(REV_PRODUCT_NO)),' ')
                           ,value(SP_CODE, ' ')
                           ,value(SP_SVC_CODE,' ')
                           ,case when customize_way_code in ('1','2','3','4','5') then customize_way_code
                            else '6' end customize_way_code
                           ,value(IN_PRODUCT_NO,' ')
                           ,value('20120413'||substr(BEGIN_TIME,9),' ')
                           ,value(END_TIME,' ')
                           ,value(char(FEE),'0')
                           ,value(char(BUSITYPE_ID),' ')
                           ,value(substr(replace(replace(char(CDR_TIMES+1 days),'-',''),'.',''),1,14),' ')
                           ,value(RING_ID,' ')                        
                        from 
                          bass2.DWD_MR_OPER_CDR_20120412
fetch first 5420 rows only 



select count(0) from bass2.DWD_MR_OPER_CDR_20120419






select * from syscat.tables where tabname like '%PROPERTY%'



DIM_PROPERTY_INFO

select * from bass2.DIM_PROPERTY_INFO


select * from bass1.report_key_index_month
order by 1 desc ,2 asc




update (         
select * from G_I_06023_MONTH
where time_id = 201203
and 
(
STORE_AREA = '' or STORE_AREA = '0'
)
and bigint(BUILD_AREA) > 2
and BUILD_AREA <> ''
) t 
set STORE_AREA = char(bigint(BUILD_AREA)-2)




select * from G_I_06023_MONTH
where time_id = 201203
and channel_id in 
(
'100003959'
,'100003064'
)

select  * from bass1.g_i_06021_month where time_id =201203 and channel_type='1'		

LONGITUDE
LATITUDE

select LONGITUDE,LATITUDE,count(0)
from bass1.g_i_06021_month  
where time_id =201203 and channel_type='1'	
group by LONGITUDE,LATITUDE
having count(0) > 1


select LONGITUDE,count(0)
from bass1.g_i_06021_month  
where time_id =201203 and channel_type='1'	
group by LONGITUDE
having count(0) > 1



select LATITUDE,count(0)
from bass1.g_i_06021_month  
where time_id =201203 and channel_type='1'	
group by LATITUDE
having count(0) > 1






select 
         time_id,
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end,
         target1,
         target2,
         target3,target3*100
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_4'
and time_id / 100 in (201203,201202)
and bigint(target1) > 500
order by 1 desc 








delete from bass1.G_S_04015_DAY where time_id = 20120419
insert into bass1.G_S_04015_DAY
                           (
                             TIME_ID           ,
                             BILL_TYPE         ,
                             PRODUCT_NO        ,
                             PRTY_CODE         ,
                             SP_ID             ,
                             SERV_CODE         ,                             
                             CUSTOM_CHNL       ,                             
                             MEET_NUM          ,                             
                             PREZENT_BEGIN_TIME,
                             PREZENT_END_TIME  ,                             
                             FEE_AMT           ,                             
                             BUSI_TYPE         ,
                             DEAL_TIME         ,
                             CUST_CONTE         
                           )
                         select
                           20120419
                           ,char(FEETYPE_ID)  
                           ,ltrim(rtrim(PRODUCT_NO))
                           ,value(ltrim(rtrim(REV_PRODUCT_NO)),' ')
                           ,value(SP_CODE, ' ')
                           ,value(SP_SVC_CODE,' ')
                           ,case when customize_way_code in ('1','2','3','4','5') then customize_way_code
                            else '6' end customize_way_code
                           ,value(IN_PRODUCT_NO,' ')
                           ,value('20120419'||substr(BEGIN_TIME,9),' ')
                           ,value(END_TIME,' ')
                           ,value(char(FEE),'0')
                           ,value(char(BUSITYPE_ID),' ')
                           ,value(substr(replace(replace(char(CDR_TIMES+1 days),'-',''),'.',''),1,14),' ')
                           ,value(RING_ID,' ')                        
                        from 
                          bass2.DWD_MR_OPER_CDR_20120418
fetch first 5320 rows only 

5303



select * from app.sch_control_runlog 
where control_code like '%04015%'





delete from bass1.G_S_04015_DAY where time_id = 20120420;
insert into bass1.G_S_04015_DAY
                           (
                             TIME_ID           ,
                             BILL_TYPE         ,
                             PRODUCT_NO        ,
                             PRTY_CODE         ,
                             SP_ID             ,
                             SERV_CODE         ,                             
                             CUSTOM_CHNL       ,                             
                             MEET_NUM          ,                             
                             PREZENT_BEGIN_TIME,
                             PREZENT_END_TIME  ,                             
                             FEE_AMT           ,                             
                             BUSI_TYPE         ,
                             DEAL_TIME         ,
                             CUST_CONTE         
                           )
                         select
                           20120420
                           ,char(FEETYPE_ID)  
                           ,ltrim(rtrim(PRODUCT_NO))
                           ,value(ltrim(rtrim(REV_PRODUCT_NO)),' ')
                           ,value(SP_CODE, ' ')
                           ,value(SP_SVC_CODE,' ')
                           ,case when customize_way_code in ('1','2','3','4','5') then customize_way_code
                            else '6' end customize_way_code
                           ,value(IN_PRODUCT_NO,' ')
                           ,value('20120420'||substr(BEGIN_TIME,9),' ')
                           ,value(END_TIME,' ')
                           ,value(char(FEE),'0')
                           ,value(char(BUSITYPE_ID),' ')
                           ,value(substr(replace(replace(char(CDR_TIMES+1 days),'-',''),'.',''),1,14),' ')
                           ,value(RING_ID,' ')                        
                        from 
                          bass2.DWD_MR_OPER_CDR_20120417
fetch first 6100 rows only 



select 
         time_id,
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end,
         target1,
         target2,
         target3,target3*100
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_1'
and time_id / 100 = 201202
order by 1 desc 





select TIME_ID 
,sum(bigint(FH_REWARD))
from G_S_22063_MONTH 
group by  TIME_ID 
order by 1 



����	79.8087	81.9608	87.6846	90.7335
201202	6701	907335	   
201201	6874	876846	   
201112	6914	819608	   
201111	6942	798087	   


CREATE TABLE "BASS1   "."G_S_22063_MONTH_B20120423"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "STATMONTH" CHAR(6) NOT NULL , 
                  "CHANNEL_ID" CHAR(40) NOT NULL , 
                  "FH_REWARD" CHAR(10) , 
                  "BASIC_REWARD" CHAR(10) , 
                  "INCR_REWARD" CHAR(10) , 
                  "INSPIRE_REWARD" CHAR(10) , 
                  "TERM_REWARD" CHAR(10) , 
                  "RENT_CHARGE" CHAR(8) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "STATMONTH",  
                 "CHANNEL_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

insert into G_S_22063_MONTH_B20120423
select * from G_S_22063_MONTH
where time_id = 201203


delete from G_S_22063_MONTH where time_id = 201203
insert into G_S_22063_MONTH
select * from G_S_22063_MONTH_B20120423


select * from 



                  SELECT
                           201203
                                ,'201203'
                                ,trim(char(a.CHANNEL_ID))
                                ,char(bigint( sum(case when t_index_id in (1,2,3,4,5,6) then result else 0 end )                 ))
                                ,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21,22,23) then result else 0 end )   ))
                                ,char(bigint( sum(case when t_index_id in (7,8,9) then result else 0 end )                      ))
                                ,'0'
                                ,'0'
                                ,'0'
                        FROM BASS2.DW_CHANNEL_INFO_201203 A
                        inner join bass2.stat_channel_reward_0002 b on a.channel_id=b.channel_id
                        WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102) 
                                                and b.op_time=201203
                                                AND B.result>0
                        group by trim(char(a.CHANNEL_ID))
                        
                        
                        

select * from app.sch_control_runlog where control_code like '%G_S_22063_MONTH%'
                        
                        
                       

select * from app.sch_control_task where control_code like '%G_S_22063_MONTH%'
                        
                        
                        
                        

select count(0) from BASS2.DWD_MR_OPER_CDR_20120421

                        
                        



select 
         time_id,
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end,
         target1,
         target2,
         target3,target3*100
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_1'
and time_id / 100 in (201203,201204)
order by 1 desc 

                        
                        

  bass1.g_a_02004_02008_stage 


89160002100360

select user_id,usertype_id,day_new_mark,test_mark from bass2.dw_product_20120423 where user_id = '89160002100360'

		select distinct user_id from bass1.g_a_02004_02008_stage
		where create_date = '20120423'
		  and test_flag='0'
          except
          select distinct user_id
                    from bass2.dw_product_20120423
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1

89160002100360 
                        
               select distinct user_id
                    from bass2.dw_product_20120423
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1                   
except
		select distinct user_id from bass1.g_a_02004_02008_stage
		where create_date = '20120423'
		  and test_flag='0'
          

                        
                        
		select count(distinct user_id) from bass1.g_a_02004_02008_stage
		where create_date = '20120423'
		  and test_flag='0'                        
          


		select distinct user_id from bass1.g_a_02004_02008_stage
		where create_date = '20120422'
		  and test_flag='0'
          except
          select distinct user_id
                    from bass2.dw_product_20120422
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1          
                     

USER_ID
user_id,usertype_id,day_new_mark,test_mark
select * from bass2.dw_product_20120423 
where user_id in (
 '89160002098648'
,'89160002098596' 
,'89160002096796' 
)
                     
                     

select * from g_a_02008_day 
where user_id in (
 '89160002098648'
,'89160002098596' 
,'89160002096796' 
)
              

select * from g_a_02004_02008_stage 
where user_id in (
 '89160002098648'
,'89160002098596' 
,'89160002096796' 
)





delete from bass1.G_S_04015_DAY where time_id = 20120421;
insert into bass1.G_S_04015_DAY
                           (
                             TIME_ID           ,
                             BILL_TYPE         ,
                             PRODUCT_NO        ,
                             PRTY_CODE         ,
                             SP_ID             ,
                             SERV_CODE         ,                             
                             CUSTOM_CHNL       ,                             
                             MEET_NUM          ,                             
                             PREZENT_BEGIN_TIME,
                             PREZENT_END_TIME  ,                             
                             FEE_AMT           ,                             
                             BUSI_TYPE         ,
                             DEAL_TIME         ,
                             CUST_CONTE         
                           )
                         select
                           20120421
                           ,char(FEETYPE_ID)  
                           ,ltrim(rtrim(PRODUCT_NO))
                           ,value(ltrim(rtrim(REV_PRODUCT_NO)),' ')
                           ,value(SP_CODE, ' ')
                           ,value(SP_SVC_CODE,' ')
                           ,case when customize_way_code in ('1','2','3','4','5') then customize_way_code
                            else '6' end customize_way_code
                           ,value(IN_PRODUCT_NO,' ')
                           ,value('20120421'||substr(BEGIN_TIME,9),' ')
                           ,value(END_TIME,' ')
                           ,value(char(FEE),'0')
                           ,value(char(BUSITYPE_ID),' ')
                           ,value(substr(replace(replace(char(CDR_TIMES+1 days),'-',''),'.',''),1,14),' ')
                           ,value(RING_ID,' ')                        
                        from 
                          bass2.DWD_MR_OPER_CDR_20120421



select * from bass1.g_rule_check 
where rule_code = 'R161_14'




        select *  from 
			(
			                select t.USER_ID,t.STATUS_ID
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id <= 20120423
			  ) a
			where rn = 1	and STATUS_ID = '1'
		   and exists (select 1 from bass2.dw_product_20120423  b where a.user_id = b.user_id 
											 and userstatus_id not in (1,2,3,6,8) )
                                            
                                            

					
							
        select  userstatus_id,count(0) from 
			(
			                select t.USER_ID,t.STATUS_ID
			                ,row_number()over(partition by t.USER_ID, t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
							where time_id <=20120423
			  ) a ,bass2.dw_product_20120423   b
			where rn = 1
			and a.user_id = b.user_id
			group by userstatus_id                                            
            
            

            


select 
         a.TIME_ID
        ,a.ENTERPRISE_ID
        ,a.USER_ID
        ,a.ENTERPRISE_BUSI_TYPE
        ,a.MANAGE_MODE
        ,a.PRODUCT_NO
        ,a.INDUSTRY_ID
        ,a.GPRS_TYPE
        ,a.DATA_SOURCE
        ,a.CREATE_DATE
        ,'2' STATUS_ID
from (
        select 
         20120423 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
        from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id,USER_ID order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
							where time_id <= 20120423
			  ) a
			where rn = 1	and STATUS_ID = '1'
     ) a , bass2.dw_product_20120423 b 
     where a.USER_ID = b.user_id 
     and b.userstatus_id not in (1,2,3,6,8)
           

           
           
           
--before
WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE control_code = 'BASS1_EXP_G_S_04005_DAY'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.control_code = n.before_control_code
             )
SELECT distinct n.*,d.flag FROM n
,app.sch_control_task c
,app.sch_control_runlog d 
where n.control_code = c.control_code
and c.control_code = d.CONTROL_CODE
and c.control_code like 'BASS1%'
--and c.deal_time = 1
--and n.before_control_code  not like '%INT_CHECK%' 
--and c.control_code not like 'OLAP_%'



select control_code,before_control_code,count(0) from app.sch_control_BEFORE
where control_code like 'BASS1%'
group by control_code,before_control_code having count(0) > 1



select * from     app.sch_control_BEFORE
where control_code = 'BASS1_EXP_G_S_02048_MONTH'       

select min(rowid) from app.sch_control_BEFORE


select STATUS_ID , count(0) 
--,  count(distinct STATUS_ID ) 
from G_A_02062_DAY 
where time_id = 20120423
group by  STATUS_ID 
order by 1 



select count(0) from G_A_02062_DAY
where time_id = 20120424


1148

STATUS_ID	2	   
1	744	   
2	404	   
		
       
       
       
USER_ID
89160001276532
89657332759081

		select * from bass1.g_a_02004_02008_stage
where user_id in ('89160001276532','89657332759081')

select * from g_a_02004_day
where user_id = '89757333590768'


		select distinct user_id from bass1.g_a_02004_02008_stage
		where create_date = '20120425'
		  and test_flag='0'
          except
          select distinct user_id
                    from bass2.dw_product_20120425
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1          
                     


89757333590768 


select usertype_id,day_off_mark,userstatus_id,test_mark,day_new_mark
                    from bass2.dw_product_20120424
                  where   user_id = '89757333590768'
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1
                     and user_id = '89757333590768'

                     
                     



select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120424
 where user_id = '89757333590768'
 
 
 
where user_id in (
 '89160002098648'
,'89160002098596' 
,'89160002096796' 
)
                     
                     
                     

select TIME_ID , count(0) 
,  sum(bigint(D_SMS_FEE) ) 
from G_S_22038_DAY 
group by  TIME_ID 
order by 1 





select * from g_a_02004_day
where user_id = '89757333590768'



select RECREATE_MARK,DAY_RECREATE_MARK,count(0)
from bass2.dw_product_20120421
group by RECREATE_MARK,DAY_RECREATE_MARK




select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
--select *
 from bass2.dw_product_20120426
 where user_id 
in
('89360002107657' 
,'89560002107656' 
,'89660002107654' 
,'89360002107231' 
,'89460002107655')





select * from bass2.dw_product_ins_off_ins_prod_ds
 where product_instance_id
 in
 ('89760002106195'
,'89760002106190'
,'89660002107234'
,'89660002107354'
,'89160002107017')



select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 
-- SELECT COUNT(0)
 from bass2.dw_product_20120425
where VALID_DATE = EXPIRE_DATE
and test_mark = 0
and day_off_mark = 0
and day_new_mark = 0
AND usertype_id in (1,2,9) 
AND SUBSTR(replace(char(VALID_DATE),'-',''),1,6) = '201203'


select * from BASS2.dw_product_20120425
where RECREATE_MARK = 1


SELECT COUNT(0)
 from bass2.dw_product_bass1_20120425
where VALID_DATE = EXPIRE_DATE
and test_mark = 0
AND usertype_id in (1,2,9) 





select user_id from   bass1.g_a_02004_02008_stage 
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120427
except
select user_id 
from bass2.dw_product_20120427
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1


select * from g_a_02004_day
where user_id = '89460002108847'


select * from g_a_02008_day
where user_id = '89460002108847'

select * from g_a_02008_day
    where user_id = '89460002108847'

select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
from bass2.dw_product_20120427
where user_id = '89460002108847'

select time_id,sum(cnt) from
(
select time_id,count(*) cnt from bass1.g_s_04004_day
where time_id>=20120401
  and mm_bill_type in ('00')
  and applcn_type='0'
group by time_id
union all
select time_id,count(*) cnt from bass1.g_s_04004_day
where time_id>=20120401
  and applcn_type in('1','2','3','4')
group by time_id  
 ) as a
group by time_id
 
 
 
 select count(0),count(distinct xzbas_value) from   BASS1.ALL_DIM_LKP
where  BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')



select count(0)
from bass1.int_02004_02008_month_stage b
where b.USERTYPE_ID   not in ('2010','2020','2030','9000') 



 select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
and xzbas_value not in ('82000038')




select time_id, ACCT_ITEM_ID , count(0) 
from G_S_03004_MONTH 
where  time_id in (201202,201203)
 and ACCT_ITEM_ID in
('0601'
,'0603'
,'0605'
,'0607'
,'0609'
,'0611'
,'0613'
,'0625'
,'0627')
group by time_id, ACCT_ITEM_ID 
order by 1 ,2




select time_id, ACCT_ITEM_ID , count(0) 
,sum(bigint(FEE_RECEIVABLE))
from G_S_03004_MONTH 
where  time_id in (201202,201203)
 and substr(ACCT_ITEM_ID,1,2) = '06' 
group by time_id, ACCT_ITEM_ID 
order by 1 ,2



SELECT *
 from bass2.dw_product_bass1_20120426
 where replace(char(create_date),'-','') = '20120426'
and  (recreate_mark=1)


PRIORITY_VAL
-1

select * from

update (
select * from app.sch_control_task
where control_code  = 'BASS1_G_I_02026_MONTH.tcl'
) set PRIORITY_VAL = 210


select * from app.sch_control_task
where control_code  like '%BASS1_G_S_02024_DAY%'
G_S_22082_DAY


select control_code,COUNT(0) from app.sch_control_alarm 
where control_code like 'BASS1%'
GROUP BY control_code



select




WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code  IN    ('BASS1_G_S_04002_DAY.tcl')        
			 UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct D.* FROM n,app.sch_control_task c,APP.SCH_CONTROL_RUNLOG D
where n.control_code = c.control_code
and c.deal_time = 1
AND C.CONTROL_CODE=D.CONTROL_CODE
and D.control_code like 'BASS1%DAY%'


select * from app.sch_control_before where control_code like '%02004_02008%'


update (
select * from 
app.sch_control_runlog
where control_code in (
'BASS1_EXP_G_A_02004_DAY'
,'BASS1_EXP_G_A_02008_DAY'
,'BASS1_EXP_G_A_02011_DAY'
,'BASS1_EXP_G_A_02059_DAY'
,'BASS1_EXP_G_A_02060_DAY'
,'BASS1_EXP_G_A_02061_DAY'
,'BASS1_EXP_G_A_02062_DAY'
,'BASS1_EXP_G_S_04002_DAY'
,'BASS1_EXP_G_S_04017_DAY'
,'BASS1_EXP_G_S_04018_DAY'
,'BASS1_EXP_G_S_22012_DAY'
,'BASS1_EXP_G_S_22091_DAY'
,'BASS1_EXP_G_S_22201_DAY'
,'BASS1_EXP_G_S_22202_DAY'
,'BASS1_EXP_G_S_22203_DAY'
,'BASS1_G_A_02004_DAY.tcl'
,'BASS1_G_A_02008_DAY.tcl'
,'BASS1_G_BUS_00000_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl'
,'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl'
,'BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_R023R024_DAY.tcl'
,'BASS1_INT_CHECK_R255_DAY.tcl'
,'BASS1_INT_CHECK_R177182_DAY.tcl'
,'BASS1_INT_CHECK_R178183_DAY.tcl'
,'BASS1_INT_CHECK_R179184_DAY.tcl'
,'BASS1_INT_CHECK_R180185_DAY.tcl'
,'BASS1_INT_CHECK_TD_DAY.tcl'
,'BASS1_INT_CHECK_Z345_DAY.tcl'
)
) t
set flag = -2


BASS1_INT_CHECK_INDEX_SAME_DAY.tcl	2012-04-27 05:49:51	2012-04-27 05:51:11	79	-1	   
BASS1_EXP_G_A_02004_DAY	2012-04-27 04:53:46	2012-04-27 04:54:33	46	-1	   
BASS1_INT_CHECK_R177182_DAY.tcl	2012-04-27 05:24:55	2012-04-27 05:25:16	21	-1	   
BASS1_INT_CHECK_R180185_DAY.tcl	2012-04-27 04:41:50	2012-04-27 04:43:54	123	-1	   



select count(0) from g_a_02004_day where time_id = 20120426





BASS1_G_A_02004_DAY.tcl




WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE control_code = 'BASS1_G_S_02024_DAY.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.control_code = n.before_control_code
             )
SELECT distinct n.* FROM n
,app.sch_control_task c
where n.control_code = c.control_code
and c.control_code like 'BASS1%'
and c.deal_time = 1




select * from bass2.dw_product_20120426
where user_id = '89360002107231'



select * from G_A_02004_DAY
where user_id = '89360002107231'



WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE control_code = 'BASS1_INT_CHECK_R258_DAY.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.control_code = n.before_control_code
             )
SELECT distinct c.*,d.* FROM n
,app.sch_control_runlog c
,app.sch_control_task d
where n.control_code = c.control_code
and c.CONTROL_CODE = d.CONTROL_CODE
and c.control_code like 'BASS1%'
--and c.deal_time = 1
--and n.before_control_code  not like '%INT_CHECK%' 
--and c.control_code not like 'OLAP_%'



select * from app.sch_control_runlog 
where control_code in 
(select before_control_code from app.sch_control_before where control_code ='BASS1_G_S_02024_DAY.tcl')



BASS1_G_S_02024_DAY.tcl



select * from app.sch_control_task
where control_code in ('BASS1_G_S_02024_DAY.tcl')

select count(0) from bass2.ODS_CHANNEL_INFO_20120426



select count(0) from  "BASS2   "."DIM_CHANNEL_INFO";

CREATE TABLE "BASS2   "."DIM_CHANNEL_INFO"  (
                  "CHANNEL_ID" BIGINT , 
                  "ORGANIZE_ID" BIGINT , 
                  "SCHEME_CHANNEL_ID" BIGINT , 
                  "CHANNEL_NAME" VARCHAR(40) , 
                  "CHANNEL_CODE" VARCHAR(10) , 
                  "REGION_CODE" VARCHAR(8) , 
                  "COUNTY_CODE" VARCHAR(8) , 
                  "THORPE_CODE" BIGINT , 
                  "BUSI_REGION_CODE" VARCHAR(20) , 
                  "COVER_BUSI_REGION_CODE" VARCHAR(20) , 
                  "CHANNEL_TYPE" INTEGER , 
                  "CHANNEL_KIND" INTEGER , 
                  "CHANNEL_LEVEL" INTEGER , 
                  "CHANNEL_STYLE" INTEGER , 
                  "GEOGRAPHY_PROPERTY" INTEGER , 
                  "GEOGRAPHY_TYPE" INTEGER , 
                  "CHANNEL_ADDRESS" VARCHAR(120) , 
                  "LONGITUDE" BIGINT , 
                  "LATITUDE" BIGINT , 
                  "IS_INTEGRAL" SMALLINT , 
                  "IS_CENTER" SMALLINT , 
                  "IS_EXCLUDE" SMALLINT , 
                  "INTERNET_FLAG" SMALLINT , 
                  "INTERNET_MODE" INTEGER , 
                  "PROPERTY_SRC_TYPE" INTEGER , 
                  "CHANNEL_STATE" SMALLINT , 
                  "OPEN_DATE" TIMESTAMP , 
                  "BUSI_BEGIN_TIME" VARCHAR(6) , 
                  "BUSI_END_TIME" VARCHAR(6) , 
                  "TEL_NUMBER" VARCHAR(20) , 
                  "FAX_NUMBER" VARCHAR(20) , 
                  "POST_CODE" VARCHAR(20) , 
                  "APPLY_ID" SMALLINT , 
                  "CREATE_DATE" TIMESTAMP , 
                  "END_REASON" VARCHAR(100) , 
                  "OTHER_INFO" VARCHAR(200) , 
                  "DONE_DATE" TIMESTAMP , 
                  "DONE_CODE" VARCHAR(100) , 
                  "ORG_ID" BIGINT , 
                  "OP_ID" BIGINT , 
                  "NOTES" VARCHAR(200) , 
                  "STATE" SMALLINT , 
                  "STREET_CODE" INTEGER , 
                  "CANCEL_REASON" VARCHAR(200) , 
                  "USER_STATE" SMALLINT , 
                  "CHANNEL_TYPE_CLASS" INTEGER , 
                  "CHANNEL_CHILD_TYPE" INTEGER , 
                  "AREA_CODE" INTEGER , 
                  "SHOP_NUMBER" VARCHAR(256) )   
                 DISTRIBUTE BY HASH("CHANNEL_ID")   
                   IN "TBS_3H" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

ALTER TABLE "BASS2   "."DIM_CHANNEL_INFO" LOCKSIZE TABLE;






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
deadline  in (9)
and type = 'd'
and sts = 1


select *from app.sch_control_runlog
fetch first 10 rows only



select A.CONTROL_CODE,A.BEGINTIME,A.ENDTIME,A.RUNTIME,char(a.RUNTIME/60)||'min' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%'
and a.RUNTIME/60 > 10
and control_code not like 'BASS1_EXP%'
ORDER BY RUNTIME DESC 

CONTROL_CODE	BEGINTIME	ENDTIME	RUNTIME	FLAG	6	7	   
BASS1_EXP_G_S_04002_DAY	2012-04-27 08:20:19	2012-04-27 09:09:30	2951	0	49         min	0          hr	   
BASS1_INT_CHECK_04004_DAY.tcl	2012-04-27 01:34:17	2012-04-27 02:03:32	1755	0	29         min	0          hr	   
BASS1_INT_CHECK_TD_DAY.tcl	2012-04-27 07:53:04	2012-04-27 08:19:32	1587	0	26         min	0          hr	   
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl	2012-04-27 03:54:24	2012-04-27 04:15:03	1239	0	20         min	0          hr	   
BASS1_EXP_G_S_04005_DAY	2012-04-27 06:03:11	2012-04-27 06:22:25	1154	0	19         min	0          hr	   
BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl	2012-04-27 07:46:28	2012-04-27 08:02:17	948	0	15         min	0          hr	   
BASS1_EXP_G_S_04004_DAY	2012-04-27 04:00:53	2012-04-27 04:16:27	933	0	15         min	0          hr	   
BASS1_INT_CHECK_A0L694B5_DAY.tcl	2012-04-27 01:34:17	2012-04-27 01:49:39	922	0	15         min	0          hr	   
BASS1_G_A_01008_DAY.tcl	2012-04-27 03:59:59	2012-04-27 04:14:24	865	0	14         min	0          hr	   
							



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04004_DAY 
group by  time_id 
order by 1 






select 
         time_id
         ,case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end index_name
         ,target1 bass2_val
         ,target2 bass1_val
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_1'
and time_id  >(int(replace(char(current date - 62 days),'-','')))
order by 1 desc 






select TIME_ID , count(0) 
--,  count(distinct TIME_ID ) 
from bass1.g_s_04018_day 
group by  TIME_ID 
order by 1 




select  a
from 
(
select 1  a from bass2.dual 
union all
select 2  a from bass2.dual 
union all
select 1  a from bass2.dual 
) t
union 
select  a
from 
(
select 1  a from bass2.dual 
union all
select 2  a from bass2.dual 
union all
select 1  a from bass2.dual 
) t






select M.val1
,N.val2
,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate 
from 
( select int(TD_CUSTOMER_CNT) val1
from bass1.G_S_22201_DAY
where time_id=20120426 
) M
,
( select count(distinct a.product_no) val2
from  
(       
select  product_no
from bass1.g_s_04017_day
where mns_type='1' and time_id=20120426
union
select  product_no
from bass1.g_s_04002_day
where mns_type='1' and time_id=20120426
union
select 
from bass1.g_s_04018_day
where mns_type='1' and time_id=20120426
) a
,session.int_check_td_day_tmp1 b 
where a.product_no = b.product_no 
and b.usertype_id  not IN ('2010','2020','2030','9000') 
and b.test_flag='0'
) N





INSERT INTO bass1.G_RULE_CHECK VALUES
                        (20120426 ,
                        'R107',
                        cast (24.98 as  DECIMAL(18, 5) ),
                        cast (24.98 as  DECIMAL(18, 5) ),
                        cast (0.00000 as  DECIMAL(18, 5) ),
                        0.05)
                        
                        


select time_id , count(0) 
--,  count(distinct time_id ) 
from BASS1.G_S_04009_DAY 
group by  time_id 
order by 1 

                        
                        
                        


select time_id , count(0) 
--,  count(distinct time_id ) 
from BASS1.G_S_04008_DAY 
group by  time_id 
order by 1 
                        
                        
                        

values decimal(1.00*(99-100)/100,9,4)                        



select A.CONTROL_CODE,A.BEGINTIME,A.ENDTIME,A.RUNTIME,char(a.RUNTIME/60)||'min' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%'
and a.RUNTIME/60 > 10
and control_code not like 'BASS1_EXP%'
ORDER BY RUNTIME DESC 

CONTROL_CODE	BEGINTIME	ENDTIME	RUNTIME	5	   
BASS1_INT_CHECK_TD_DAY.tcl	2012-04-28 05:48:09	2012-04-28 06:04:00	950	15         min	   
BASS1_G_A_02053_DAY.tcl	2012-04-28 04:01:59	2012-04-28 04:17:07	907	15         min	   
BASS1_G_S_04002_DAY.tcl	2012-04-28 03:05:43	2012-04-28 03:19:59	856	14         min	   
BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl	2012-04-28 04:42:18	2012-04-28 04:56:11	832	13         min	   
					
                    

select A.CONTROL_CODE,A.BEGINTIME,A.ENDTIME,A.RUNTIME,char(a.RUNTIME/60)||'min' from   app.sch_control_runlog A
where control_code in
('BASS1_INT_CHECK_04004_DAY.tcl'
,'BASS1_INT_CHECK_TD_DAY.tcl'
,'BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl'
,'BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_G_A_01008_DAY.tcl')
ORDER BY RUNTIME DESC 

BASS1_INT_CHECK_04004_DAY.tcl
BASS1_INT_CHECK_TD_DAY.tcl
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl
BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl
BASS1_INT_CHECK_A0L694B5_DAY.tcl
BASS1_G_A_01008_DAY.tcl





                    
                    
                                       

                                       
                                       
                                       
               select AC_ADDRESS
                       ,COALESCE(FN_CHANGE_DTOH(trim(AC_ADDRESS)),' ') ATTESTATION_AS_IP
                        from bass2.cdr_wlan_20120428
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
                                       
                                       
                                       

               
                                       
               select COALESCE(FN_CHANGE_DTOH2(trim(AC_ADDRESS)),' ') ATTESTATION_AS_IP
               ,AC_ADDRESS
                      from (                 
               select
                       distinct AC_ADDRESS
                        from bass2.cdr_wlan_20120429
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
                            ) t
                            
                                       
                                                               
                                                               

select * from syscat.functions where funcname = 'FN_CHANGE_DTOH'
                                                               
                                                               
                                                               

select
                       distinct AC_ADDRESS
                       ,RIGHT(CHAR(HEX(INT(SUBSTR(AC_ADDRESS,1,POSSTR(AC_ADDRESS,'.')-1)))),2)
                       ,AC_ADDRESS
                       --
                       ,RIGHT(CHAR(HEX(INT(SUBSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),1,POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')-1)))),2)
                       ,substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1)
                       --
                       ,RIGHT(CHAR(HEX(INT(SUBSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),1,POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')-1)))),2)
                       ,substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1)
                       --
                       ,RIGHT(CHAR(HEX(INT(substr(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')+1)))),2)

                       ,substr(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')+1)
                        from bass2.cdr_wlan_20120429
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
               
                                                               
                                                               
                                                               



select
                       distinct AC_ADDRESS
                       ,RIGHT(CHAR(HEX(INT(SUBSTR(AC_ADDRESS,1,POSSTR(AC_ADDRESS,'.')-1)))),2)
                      
                       ||RIGHT(CHAR(HEX(INT(SUBSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),1,POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')-1)))),2)
                      
                       ||RIGHT(CHAR(HEX(INT(SUBSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),1,POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')-1)))),2)
                       
                       ||RIGHT(CHAR(HEX(INT(substr(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')+1)))),2)
    
    from bass2.cdr_wlan_20120429
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
               

                                                               



CREATE FUNCTION FN_CHANGE_DTOH2(AS_IP VARCHAR(20)) 
RETURNS VARCHAR(20) DETERMINISTIC NO EXTERNAL ACTION 
LANGUAGE SQL
BEGIN ATOMIC DECLARE STRING VARCHAR(20); 
SET STRING =RIGHT(CHAR(HEX(INT(SUBSTR(AC_ADDRESS,1,POSSTR(AC_ADDRESS,'.')-1)))),2)||RIGHT(CHAR(HEX(INT(SUBSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),1,POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')-1)))),2)||RIGHT(CHAR(HEX(INT(SUBSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),1,POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')-1)))),2)||RIGHT(CHAR(HEX(INT(substr(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),POSSTR(substr(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),POSSTR(substr(AC_ADDRESS,POSSTR(AC_ADDRESS,'.')+1),'.')+1),'.')+1)))),2); 
RETURN STRING; 
END



                                                               
                                                               


CREATE FUNCTION FN_CHANGE_DTOH2(AS_IP VARCHAR(20)) 
RETURNS VARCHAR(20) DETERMINISTIC NO EXTERNAL ACTION 
LANGUAGE SQL
BEGIN ATOMIC DECLARE STRING VARCHAR(20); 
SET STRING =RIGHT(CHAR(HEX(INT(SUBSTR(AS_IP,1,POSSTR(AS_IP,'.')-1)))),2)||RIGHT(CHAR(HEX(INT(SUBSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),1,POSSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),'.')-1)))),2)||RIGHT(CHAR(HEX(INT(SUBSTR(substr(substr(AS_IP,POSSTR(AS_IP,'.')+1),POSSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),'.')+1),1,POSSTR(substr(substr(AS_IP,POSSTR(AS_IP,'.')+1),POSSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),'.')+1),'.')-1)))),2)||RIGHT(CHAR(HEX(INT(substr(substr(substr(AS_IP,POSSTR(AS_IP,'.')+1),POSSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),'.')+1),POSSTR(substr(substr(AS_IP,POSSTR(AS_IP,'.')+1),POSSTR(substr(AS_IP,POSSTR(AS_IP,'.')+1),'.')+1),'.')+1)))),2); 
RETURN STRING; 
END



FN_CHANGE_DTOH2



                                                               


                                       
               select AC_ADDRESS
                       ,COALESCE(FN_CHANGE_DTOH(trim(AC_ADDRESS)),' ') ATTESTATION_AS_IP
                       ,COALESCE(FN_CHANGE_DTOH2(trim(AC_ADDRESS)),' ') ATTESTATION_AS_IP
                        from bass2.cdr_wlan_20120428
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
                                       
                                                               
                                                               

select count(0) from G_S_04003_DAY where time_id = 20120429
                                                               
                                                              
                                                              
                                                              
                                                              

   select
                        20120429 
                        ,PRODUCT_NO 
                        ,IMSI
                        ,value(char(PROVINCE_ID),'891')
                        ,value(char(ROAM_PROVINCE_ID),'891')
                        ,'1'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAMTYPE_ID)),'500') 
                        ,substr(char(START_TIME),1,4)||substr(char(START_TIME),6,2)||substr(char(START_TIME),9,2)
                        ,substr(char(START_TIME),12,2)||substr(char(START_TIME),15,2)||substr(char(START_TIME),18,2)
                        ,substr(char(STOP_TIME),1,4)||substr(char(STOP_TIME),6,2)||substr(char(STOP_TIME),9,2)
                        ,substr(char(STOP_TIME),12,2)||substr(char(STOP_TIME),15,2)||substr(char(STOP_TIME),18,2)
                        ,char(DURATION)
                        ,char(DATA_FLOW_UP)
                        ,char(DATA_FLOW_DOWN)
                        ,'03'
                        ,'01'   SERVICE_CODE /*1.7.9��������δʵ�֣��޷����֣�����ԭ�ھ�*/
                        ,case when AUTH_TYPE=1 THEN '01' else '02' end WLAN_ATTESTATION_CODE
                                                ,'01' WLAN_ATTESTATION_TYPE --��֤ƽ̨���� /*1.7.9��������δʵ�֣��޷�����,�� 01������ͳһ��֤ƽ̨���� �ϱ�*/
                        ,HOSTSPOT_ID
                        ,COALESCE(FN_CHANGE_DTOH2(AS_ADDRESS),' ') AS_IP
                        ,COALESCE(FN_CHANGE_DTOH2(AC_ADDRESS),' ') ATTESTATION_AS_IP
                                                ,'' USER_TERM_TYPE /*�û��ն����� ������δʵ�֣��޷�����;����*/
                                                ,'' USER_MAC_ADDR       --�û�MAC��ַ/* ������δʵ�֣��޷���ȡ;����*/
                                                ,'' AP_MAC      --AP��MAC��ַ/* ������δʵ�֣��޷���ȡ;����*/
                        ,char(CHARGE1/10 + CHARGE4/10) 
                        ,char(CHARGE4/10)
                        ,''
                        ,''
                        ,substr(char(bigint(IMSI)),1,5)
                        ,substr(char(bigint(IMSI)),1,5)
                        ,''
                        from bass2.cdr_wlan_20120429
                        where IMSI is not null and DATA_FLOW_DOWN >= 0
                                                              
                                                              

WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE control_code in ( 
             'BASS1_EXP_G_A_02004_DAY'
             ,'BASS1_EXP_G_A_02008_DAY'
             ,'BASS1_EXP_G_A_01002_DAY'
             ,'BASS1_EXP_G_A_01004_DAY'
             ,'BASS1_EXP_G_A_02008_DAY'
             ,'BASS1_EXP_G_A_02053_DAY'
             ,'BASS1_EXP_G_I_06032_DAY'
             ,'BASS1_EXP_G_I_06031_DAY'
             )
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.control_code = n.before_control_code
             )
SELECT distinct n.before_control_code FROM n
,app.sch_control_task c
where n.control_code = c.control_code
and c.control_code like 'BASS1%'
                                                              
                                                              

INTERFACE_CODE	COARSE_TYPE
01002	ÿ������
01004	ÿ������
02008	ÿ������
02004	ÿ������
06032	ÿ��ȫ��
02011	ÿ������
02053	ÿ������
06031	ÿ��ȫ��
	
                                                              
                                                              
                                                              
                                                              

select * from app.sch_control_task where control_code 
in 
('BASS2_Dw_enterprise_account_ds.tcl'
,'BASS2_Dw_enterprise_member_ds.tcl'
,'BASS2_Dw_enterprise_membersub_ds.tcl'
,'BASS2_Dw_enterprise_sub_ds.tcl'
,'BASS2_Dw_i_user_radius_order_ds.tcl'
,'BASS2_Dw_product_bass1_ds.tcl'
,'BASS2_Dw_product_ds.tcl'
,'BASS2_Dw_product_regsp_ds.tcl'
,'BASS2_Dwd_cust_msg_ds.tcl'
,'BASS2_Dwd_enterprise_manager_rela_ds.tcl'
,'BASS2_Dwd_enterprise_msg_ds.tcl'
,'BASS2_Dwd_enterprise_msg_his_ds.tcl'
,'BASS2_Dwd_product_func_ds.tcl'
,'BASS2_Dwd_product_regsp_ds.tcl'
,'BASS2_Dwd_ps_net_number_ds.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'TR1_L_08036')


select * from app.sch_control_before where control_code like '%02026%' or control_code like '%02027%'
update (
select * from app.sch_control_task where control_code = 'BASS1_G_I_02026_MONTH.tcl'
) set TIME_VALUE = 210


PRIORITY_VAL	TIME_VALUE
210	-1


	
update (
select * from app.sch_control_task where control_code LIKE 'BASS1_EXP%DAY'
    and cc_flag =1 
    ) set priority_val = 10000
    where priority_val < 10000
    


---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code = 'TR1_L_11072'
order by alarmtime desc
    
    

SELECT * FROM APP.SCH_CONTROL_TASK WHERE CONTROL_CODE = 'TR1_L_11072'
    

select count(0) from 
ODS_UP_RES_TIEM_201203
    