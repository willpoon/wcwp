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
    
update bass1.g_s_22012_day set m_off_users='101' 
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



