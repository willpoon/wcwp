select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabschema = 'BASS1'
and a.tabname like 'G%'
select * from dw_product_scheme_promo_ds



select * from 
STAT_ENTERPRISE_SNAPSHOT_201012

SQL0668N

enterprise_snapshot


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2)*1024 as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'STAT_ENTERPRISE_SNAPSHOT_201012'

select * from app.sch_control_task where 
upper(cmd_line) like  '%ENTERPRISE_SNAPSHOT%'
 
 select * from app.sch_control_runlog where control_code = 'TR1_VGOP_D_14303'
 

select * from STAT_ENTERPRISE_SNAPSHOT_201012

select * from STAT_ENTERPRISE_SNAPSHOT_201101


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110225'||'000000') 
--and flag = -1
order by alarmtime desc 

select count(0) from stat_enterprise_0053_detail



select  * from APP.G_FILE_REPORT
where filename like '%20100727%' and err_code='00'


select * from
DW_APP_CTT_USERINFO_20110220


select * from DW_APP_CTT_USERINFO_20110220
    
INSERT INTO BASS2.DW_APP_CTT_USERINFO_20110220 
(
        USER_ID
        ,PRODUCT_NO
        ,CUST_NAME
        ,CITY_ID
        ,COUNTY_ID
        ,AREA_ID
        ,DEPT_ID
        ,CHANNEL_ID
        ,BRAND_ID1
        ,BRAND_ID
        ,PLAN_ID
        ,USER_TYPE
        ,GROUP_KEY_MARK
        ,USERSTATUS_ID
        ,STATUS_DATE
        ,PREPAY_MARK
        ,BALANCE_FEE
        ,REC1M_CLT_COUNTS
        ,LAST_CALL_DATE
        ,JWGROUP_EXCEPT_MARK
                         )
                         select trim(a.user_id)
                         ,trim(a.PRODUCT_NO)
                         ,b.CUST_NAME
                         ,a.CITY_ID
                         ,a.COUNTY_ID
                         ,a.REGION_ID AREA_ID
                         ,'0' DEPT_ID  --暂无法提取
                         ,a.CHANNEL_ID
                         ,char(a.CRM_BRAND_ID1) BRAND_ID1
                         ,char(a.CRM_BRAND_ID2) BRAND_ID
                         ,a.PLAN_ID
                         ,char(a.USERTYPE_ID) USER_TYPE
                         ,case when c.RCUST_ID is not null then 1 else 0 end GROUP_KEY_MARK 
                         ,char(a.USERSTATUS_ID)
                         ,date(a.STS_DATE) STATUS_DATE
                         ,case when a.CRM_BRAND_ID1 = 1 then 0 else 1 end PREPAY_MARK
                         ,a.balance_fee
                         ,value(d.REC1M_CLT_COUNTS,0) REC1M_CLT_COUNTS
                         ,date('1900-01-01') LAST_CALL_DATE --暂无法提取
                         ,0 JWGROUP_EXCEPT_MARK
        from bass2.dw_product_20110220 a 
        left join dw_cust_20110220 b on a.cust_id = b.cust_id
        left join (select distinct RCUST_ID from DWD_CUST_RELATION_20110220 where CUST_RELATION_ID = 4 ) c 
        on b.cust_id = c.RCUST_ID
        left join (select USER_ID,sum(COMPLAINT_CNT) REC1M_CLT_COUNTS 
                                                        from Dw_cs_complaintuserinfo_dm  
         where op_time >= date('2011-01-20') 
         group by USER_ID 
                ) d on a.user_id = d.user_id 



select * from app.sch_control_task 
where function_desc like '%关键人%'

select * from syscat.tables where 
tabname like '%DIM%RELATION%'
TABSCHEMA	TABNAME
BASS2   	DIM_ACCT_CATALOG_RELATION
BASS2   	DIM_CUST_RELATION
BASS2   	DIM_CUST_RELATION_ST
BASS2   	DIM_RELATION_TYPE
MMP     	DIM_CHANNEL_USER_RELATION
MMP     	DIM_DEPT_FLOW_RELATION
MPM     	BAK1210_DIM_CHANNEL_USER_RELATION
MPM     	DIM_CHANNEL_USER_RELATION
MPM     	DIM_DEPT_FLOW_RELATION

select * from DIM_CUST_RELATION

select 
select count(0),count(distinct CUST_ID ) from  DWD_CUST_RELATION_20110219

select * from 
DWD_CUST_RELATION_20110219
where CUST_RELATION_ID = 4

89202999757491
89201120476943

select * from
dw_cust_201101
where cust_id = '89202999757491'

select * from DW_APP_CTT_USERINFO_20110220

drop TABLE "BASS2   "."DW_APP_CTT_USERINFO_YYYYMMDD"
CREATE TABLE "BASS2   "."DW_APP_CTT_USERINFO_YYYYMMDD"  (
                  "USER_ID" VARCHAR(20) NOT NULL , 
                  "PRODUCT_NO" VARCHAR(20) NOT NULL , 
                  "CUST_NAME" VARCHAR(128) , 
                  "CITY_ID" VARCHAR(18) , 
                  "COUNTY_ID" VARCHAR(18) , 
                  "AREA_ID" VARCHAR(18) , 
                  "DEPT_ID" VARCHAR(18) , 
                  "CHANNEL_ID" INTEGER , 
                  "BRAND_ID1" VARCHAR(1) , 
                  "BRAND_ID" VARCHAR(5) , 
                  "PLAN_ID" BIGINT , 
                  "USER_TYPE" VARCHAR(5) , 
                  "GROUP_KEY_MARK" SMALLINT , 
                  "USERSTATUS_ID" VARCHAR(5) , 
                  "STATUS_DATE" DATE , 
                  "PREPAY_MARK" SMALLINT , 
                  "BALANCE_FEE" DECIMAL(13,2) , 
                  "REC1M_CLT_COUNTS" INTEGER , 
                  "LAST_CALL_DATE" DATE , 
                  "JWGROUP_EXCEPT_MARK" SMALLINT )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_CDR_DATA"  
		

select * from BASS2.DW_APP_CTT_USERINFO_20110220 
select * from dw_product_20110220

select length(CHANNEL_ID),count(0)
from dw_product_20110220 
group by length(CHANNEL_ID)
        drop table BASS2.DW_APP_CTT_USERINFO_20110220 
        

        create table BASS2.DW_APP_CTT_USERINFO_20110220 
        like DW_APP_CTT_USERINFO_YYYYMMDD
        DISTRIBUTE BY HASH(USER_ID)   
  IN TBS_CDR_DATA

INSERT INTO BASS2.DW_APP_CTT_USERINFO_20110220 
(
        USER_ID
        ,PRODUCT_NO
        ,CUST_NAME
        ,CITY_ID
        ,COUNTY_ID
        ,AREA_ID
        ,DEPT_ID
        ,CHANNEL_ID
        ,BRAND_ID1
        ,BRAND_ID
        ,PLAN_ID
        ,USER_TYPE
        ,GROUP_KEY_MARK
        ,USERSTATUS_ID
        ,STATUS_DATE
        ,PREPAY_MARK
        ,BALANCE_FEE
        ,REC1M_CLT_COUNTS
        ,LAST_CALL_DATE
        ,JWGROUP_EXCEPT_MARK
			 )
			 select trim(a.user_id)
			 ,trim(a.PRODUCT_NO)
			 ,b.CUST_NAME
			 ,a.CITY_ID
			 ,a.COUNTY_ID
			 ,'0' AREA_ID
			 ,'0' DEPT_ID
			 ,a.CHANNEL_ID
			 ,char(a.CRM_BRAND_ID1) BRAND_ID1
			 ,char(a.CRM_BRAND_ID2) BRAND_ID
			 ,a.PLAN_ID
			 ,char(a.USERTYPE_ID) USER_TYPE
			 ,0 GROUP_KEY_MARK
			 ,char(a.USERSTATUS_ID)
			 ,date(a.STS_DATE) STATUS_DATE
			 ,case when a.CRM_BRAND_ID1 = 1 then 0 else 1 end PREPAY_MARK
			 ,a.balance_fee
			 ,value(d.REC1M_CLT_COUNTS,0) REC1M_CLT_COUNTS
			 ,date('1900-01-01') LAST_CALL_DATE
			 ,0 JWGROUP_EXCEPT_MARK
	from bass2.dw_product_20110220 a 
	left join dw_cust_20110220 b on a.cust_id = b.cust_id
	left join (select USER_ID,sum(COMPLAINT_CNT) REC1M_CLT_COUNTS 
	 from Dw_cs_complaintuserinfo_dm  
	 where op_time >= date('2011-01-20') 
	 group by USER_ID 
		) d on a.user_id = d.user_id 
        
115000000301
select * from dw_product_20110220
where PLAN_ID
= 115000000301


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'DW_PRODUCT_20110220'



select * from syscat.tables where tabname like '%%'
select  date('2011-01-20') from dual


select a.user_id
,a.PRODUCT_NO
,b.CUST_NAME
,a.CITY_ID
,a.COUNTY_ID
--,AREA_ID
--,DEPT_ID
,a.CHANNEL_ID
,a.CRM_BRAND_ID1 BRAND_ID1
,a.CRM_BRAND_ID2 BRAND_ID
,a.PLAN_ID
,a.USERTYPE_ID USER_TYPE
--,GROUP_KEY_MARK
,a.USERSTATUS_ID
,date(a.STS_DATE) STATUS_DATE
,case when a.CRM_BRAND_ID1 = 1 then 0 else 1 end PREPAY_MARK
,a.balance_fee
,value(d.REC1M_CLT_COUNTS,0) REC1M_CLT_COUNTS
,date('1900-01-01') LAST_CALL_DATE
,0 JWGROUP_EXCEPT_MARK
from bass2.dw_product_20110220 a 
left join dw_cust_20110220 b on a.cust_id = b.cust_id
--left join map_pub_brand c on a.plan_id = c.plan_id
left join (select USER_ID,sum(COMPLAINT_CNT) REC1M_CLT_COUNTS from Dw_cs_complaintuserinfo_dm  
						where op_time >= date('2011-02-20') group by USER_ID ) d on a.user_id = d.user_id 
select a.user_id
,a.PRODUCT_NO
,b.CUST_NAME
,a.CITY_ID
,a.COUNTY_ID
--,AREA_ID
--,DEPT_ID
,a.CHANNEL_ID
,a.CRM_BRAND_ID1 BRAND_ID1
,a.CRM_BRAND_ID2 BRAND_ID
,a.PLAN_ID
,a.USERTYPE_ID USER_TYPE
--,GROUP_KEY_MARK
,a.USERSTATUS_ID
,date(a.STS_DATE) STATUS_DATE
,case when a.CRM_BRAND_ID1 = 1 then 0 else 1 end PREPAY_MARK
,a.balance_fee
,d.REC1M_CLT_COUNTS
from bass2.dw_product_20110220 a 
left join dw_cust_20110220 b on a.cust_id = b.cust_id
--left join map_pub_brand c on a.plan_id = c.plan_id
left join (select USER_ID,sum(COMPLAINT_CNT) REC1M_CLT_COUNTS from Dw_cs_complaintuserinfo_dm  
						where op_time >= date('2011-02-20') group by USER_ID ) d on a.user_id = d.user_id 
						

select * from app.sch_control_task where function_desc like '%最近%'

select * from DIM_WORK_ITEM_TYPE


select busi_type , count(0) 
--, count(distinct busi_type ) 
from dw_cs_busiwksht_dm 
group by  busi_type 
order by 1 


select * from syscat.tables where tabname like '%BUSI_TYPE%'

select * from DIM_CUST_BUSI_TYPE
select max(op_time) from Dw_cs_complaintuserinfo_dm


select * from dw_call_lastcell_201101

--23073352


select count(0) from dw_product_imei_rela_ds
where first_use_date = use_date

11623298

select * from dim_svc_item


select crm_brand_id1,crm_brand_name1 , count(0) 
from map_pub_brand 
group by  crm_brand_id1,crm_brand_name1
order by 1 


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110220'||'000000') 
--and flag = -1
order by alarmtime desc 

select * from app.sch_control_task where control_code = 'TR1_L_02039'

ODS_KF_SMS_CMD_RECEIVE_YYYYMMDD

select count(0) from ODS_KF_SMS_CMD_RECEIVE_20110220

select * from  syscat.tables where TABSCHEMA = 'BASS1'

select 
        BRAND_ID
        ,CRM_BRAND_ID1
        ,CRM_BRAND_ID2
        ,CRM_BRAND_ID3
from 
dw_product_20110219


select bill_mark , count(0)
from dw_product_20110219 
group by  bill_mark 
order by 1 

select * from  syscat.tables where tabname like 'G_%'
select * from ODS_KF_SMS_CMD_RECEIVE_20110219 where sms_id = 19656574

select count(0) from  dw_cust_20110220
3701261
select count(0) from  dwd_cust_msg_20110220
3701261

select * from app.sch_control_task where function_desc like '%关键%'

select date(state_date) from ODS_PRODUCT_INS_PROD_20110219


select * from map_pub_brand

select * from STS_DATE

select * from Dw_enterprise_keycust_msg_201101


select count(0) from  dw_cs_busiwksht_dm
select * 
select * from dw_cs_busiwksht_dm

select count( from dw_product_imei_rela_ds



select count(0) from Dw_custsvc_appeal_info_dm
select op_time,count(0) cnt 
select *  
from Dw_custsvc_appeal_info_dm
group by op_time

SELECT DISTINCT PLAN_ID 
FROM dw_product_201011


select A.plan_id,B.PLAN_NAME,count(0) cnt
from dw_product_201011 A
LEFT JOIN DIM_PUB_PLAN B ON A.PLAN_ID = B.PLAN_ID
group by A.plan_id ,B.PLAN_NAME
order by 1

select tabname from syscat.tables where tabname like '%DIM%PLAN%'

SELECT * FROM DIM_PUB_PLAN


sel

select * from term_market_user_imei_cnt
select date('2010-11-01') from dual
select * from term_market_structure

select count(0),count(distinct xx ) from 
select count(0) from term_market_structure_detail


select count(0) from term_user_info

select * from term_user_info

select * from Dw_custsvc_appeal_info_dm

select * from  app.sch_control_alarm where control_code = 'TR1_VGOP_D_14303'
select * from app.sch_control_runlog where control_code = 'TR1_VGOP_D_14303'
dddddddddddd
select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110215'||'000000') 
--and flag = -1
order by alarmtime desc 

select * from VGOP_15210_20110215
select * from VGOP_14303_20110213

select * from VGOP_15202_20110214

select * from  APP.SCH_CONTROL_RUNLOG where control_code = 'TR1_VGOP_D_15202'
TR1_VGOP_D_15210	load_vgop.sh	1	a_13100_20110215_VGOP-R1.6-15210_00_001.dat加载失败,加载记录数:,文本记录数:702,并更新日志为失败,并告警	2011-02-16 14:58:01.936095	[NULL]	-1	[NULL]
TR1_VGOP_D_15202	load_vgop.sh	1	a_13100_20110215_VGOP-R1.6-15202_00_001.dat加载失败,加载记录数:,文本记录数:2585,并更新日志为失败,并告警	2011-02-16 14:57:15.327752	[NULL]	-1	[NULL]
TR1_VGOP_D_15304	load_vgop.sh	1	i_13100_20110215_VGOP-R1.6-15304_00_001.dat加载失败,加载记录数:,文本记录数:2042803,并更新日志为失败,并告警	2011-02-16 14:54:45.040734	[NULL]	-1	[NULL]



insert into TERM_MARKET_USER_OFF_INSTANCE
select PRODUCT_INSTANCE_ID,OFFER_ID,name
from (
select PRODUCT_INSTANCE_ID,OFFER_ID,b.name,row_number()over(partition by PRODUCT_INSTANCE_ID order by a.create_date) rn 
--select count(0),count(distinct PRODUCT_INSTANCE_ID)
from DW_PRODUCT_INS_OFF_INS_PROD_DS a
join dim_prod_up_product_item  b on a.offer_id = b.product_item_id
where a.create_date <= timestamp('20101130'||'125959') and ( b.name like '%零%机%'
or b.name like  '%存%机%')
) t where rn = 1


CREATE TABLE "BASS2   "."TERM_MARKET_USER_OFF_INSTANCE"  (

         PRODUCT_INSTANCE_ID    VARCHAR(20)         
        ,OFFER_ID               BIGINT              
        ,NAME                   VARCHAR(200)  
         )   
                 DISTRIBUTE BY HASH(PRODUCT_INSTANCE_ID)   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  


select * from app.sch_control_task where function_desc like '%零元购机%'

select * from app.sch_control_task where cmd_line like '%PRODUCT_INS_OFF_INS_PROD%'

select * from ODS_PRODUCT_INS_OFF_INS_PROD_HIS_201012
CREATE TABLE "BASS2   "."TERM_MARKET_USER_OFF_INSTANCE"  (

         PRODUCT_INSTANCE_ID    VARCHAR(20)         
        ,OFFER_ID               BIGINT              
        ,NAME                   VARCHAR(200)  
         )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "CITY_ID",  
                 "BRAND_ID")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  
alarmtime >=  timestamp('20110130'||'170000') 

select distinct year(create_date),month(create_date) from DW_PRODUCT_INS_OFF_INS_PROD_DS
select current_timestamp from dual
CREATE TABLE "BASS2   "."TERM_MARKET_STRUCTURE"  (

         PRODUCT_INSTANCE_ID    VARCHAR(20)         
        ,OFFER_ID               BIGINT              
        ,NAME                   VARCHAR(200)  
         )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "CITY_ID",  
                 "BRAND_ID")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  
                   
select PRODUCT_INSTANCE_ID,OFFER_ID,b.name
--select count(0),count(distinct PRODUCT_INSTANCE_ID)
from DW_PRODUCT_INS_OFF_INS_PROD_DS a
join dim_prod_up_product_item  b on a.offer_id = b.product_item_id
where a.create_date <= timestamp('20101130'||'125959') and ( b.name like '%零%机%'
or b.name like  '%存%机%')
drop view t_v_prod_ins_off_ins_rela

create view t_v_prod_ins_off_ins_rela
as
select PRODUCT_INSTANCE_ID,OFFER_ID,name
from (
select PRODUCT_INSTANCE_ID,OFFER_ID,b.name,row_number()over(partition by PRODUCT_INSTANCE_ID order by a.create_date) rn 
--select count(0),count(distinct PRODUCT_INSTANCE_ID)
from DW_PRODUCT_INS_OFF_INS_PROD_DS a
join dim_prod_up_product_item  b on a.offer_id = b.product_item_id
where a.create_date <= timestamp('20101130'||'125959') and ( b.name like '%零%机%'
or b.name like  '%存%机%')
) t where rn = 1

select * from syscat.tables where tabname like '%DW%PRODUCT%PROMO%DS'
SELECT * FROM DW_PRODUCT_USER_PROMO_DS

DW_PRODUCT_INS_OFF_INS_PROD_DS

SELECT PROMO_NAME ,COUNT(0)
FROM DW_PRODUCT_USER_PROMO_DS
GROUP BY PROMO_NAME

select count(0) from DW_PRODUCT_USER_PROMO_DS
select * from DW_PRODUCT_USER_PROMO_DS

select OP_TIME , count(0)
from DW_PRODUCT_USER_PROMO_DS 
group by  OP_TIME 
order by 1 
select COND_NAME , count(0) , count(distinct COND_NAME ) 
from DW_PRODUCT_USER_PROMO_DS 
where COND_NAME like '%存%机%'
group by  COND_NAME 
order by 1 

select COND_NAME , count(0) , count(distinct COND_NAME ) 
from DW_PRODUCT_USER_PROMO_DS 
where COND_NAME like '%零%机%'
group by  COND_NAME 
order by 1 

select * from dim_prod_up_product_item

select b.name ,a.* from ODS_PRODUCT_INS_OFF_INS_PROD_20110210 a 
join dim_prod_up_product_item  b on a.offer_id = b.product_item_id
where name like '%零%机%'


select * from dim_prod_up_product_item where name like '%零%机%'
like '%存%机%'


select OBJECT_TYPE , count(0) , count(distinct OBJECT_TYPE ) 
from DW_PRODUCT_USER_PROMO_DS 
group by  OBJECT_TYPE 
order by 1 


select OBJECT_ID , count(0) , count(distinct OBJECT_ID ) 
from DW_PRODUCT_USER_PROMO_DS 
group by  OBJECT_ID 
order by 1 

ALTER TABLE term_user_info ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


insert into term_user_info     
select  op_time
,a.CITY_ID
,a.BRAND_ID
, term_brand_id
,a.term_model_id
,termprice_id
,term_func_id
,product_no
, custclass_id
, custstatus_id 
,CUSTTYPE_ID 
,ONLINE_ID 
,sex_id
,age_id
, income
,case when b.offer_id is not null then 1 else 0 end term_flag
,b.OFFER_ID product_item_id
, term_preference_id
, change_term_probability
from term_market_structure_detail a
left join TERM_MARKET_USER_OFF_INSTANCE b on a.user_id = b.PRODUCT_INSTANCE_ID




ALTER TABLE term_user_info ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select * from term_user_info


select * from term_user_info where product_item_id is not null
insert into term_user_info     
select  op_time
,a.CITY_ID
,a.BRAND_ID
, term_brand_id
,a.term_model_id
,termprice_id
,term_func_id
,product_no
, custclass_id
, custstatus_id 
,CUSTTYPE_ID 
,ONLINE_ID 
,sex_id
,age_id
, income
,term_flag
,b.OFFER_ID product_item_id
, term_preference_id
, change_term_probability
from term_market_structure_detail a
left join TERM_MARKET_USER_OFF_INSTANCE b on a.user_id = b.PRODUCT_INSTANCE_ID



alarmtime >=  timestamp('20110130'||'170000') 

select * from dim_prod_up_product_item


drop table "BASS2   "."TERM_MARKET_STRUCTURE"
CREATE TABLE "BASS2   "."TERM_MARKET_STRUCTURE"  (
                  "OP_TIME" DATE , 
                  "CITY_ID" VARCHAR(8) , 
                  "BRAND_ID" SMALLINT , 
                  "TERM_BRAND_ID" VARCHAR(40) , 
                  "TERM_MODEL_ID" SMALLINT , 
                  "TERM_FUNC_ID" SMALLINT , 
                  "TERMPRICE_ID" SMALLINT , 
                  "DEVICE_ID" VARCHAR(20) , 
                  "TERM_FLAG" SMALLINT , 
                  "TERM_COUNT" INTEGER , 
                  "TERM_MARKET_RATIO" DECIMAL(12,5) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "CITY_ID",  
                 "BRAND_ID")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  
ALTER TABLE term_market_structure ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select * from term_market_structure



insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)/2206140 term_market_ratio
from    term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG




insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value(((case when imei_cnt > 10 then 10 else imei_cnt end) -1)/10.1200 + (((case when imei_cnt > 100 then 100 else imei_cnt end)))*1.0000/1000,0.0000) change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 

drop table term_market_structure_detail 
create table term_market_structure_detail
(
         OP_TIME                DATE                
        ,CITY_ID                VARCHAR(7)          
        ,BRAND_ID               SMALLINT              
        ,PRODUCT_NO             VARCHAR(15)         
        ,USER_ID                VARCHAR(20)         
        ,IMEI                   VARCHAR(18)         
        ,TERM_BRAND_ID          VARCHAR(20)         
        ,TAC_NUM                VARCHAR(10)         
        ,TERM_MODEL_ID          SMALLINT            
        ,TERM_FUNC_ID           SMALLINT             
        ,DEVICE_ID              VARCHAR(16)         
        ,TERM_FLAG              SMALLINT      
        ,TERMPRICE_ID           SMALLINT                   
        ,CUSTCLASS_ID           SMALLINT            
        ,CUSTSTATUS_ID          SMALLINT            
        ,CUSTTYPE_ID            SMALLINT            
        ,ONLINE_ID              SMALLINT            
        ,SEX_ID                 SMALLINT            
        ,AGE_ID                 SMALLINT            
        ,INCOME                 DECIMAL(12,2)       
        ,PRODUCT_ITEM_ID        BIGINT              
        ,TERM_PREFERENCE_ID     VARCHAR(40)         
        ,CHANGE_TERM_PROBABILITY        DECIMAL(12,4)           
)        
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY      

      ALTER TABLE term_user_info ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 
select * from dim_cust_status             
insert into term_user_info     
select  op_time
,a.CITY_ID
,a.BRAND_ID
, term_brand_id
,a.term_model_id
,termprice_id
,term_func_id
,product_no
, custclass_id
, custstatus_id 
,CUSTTYPE_ID 
,ONLINE_ID 
,sex_id
,age_id
, income
,term_flag
, product_item_id
, term_preference_id
, change_term_probability
from term_market_structure_detail a

                   ALTER TABLE term_market_structure ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

                   
                   select  count(user_id||imei) term_count from        
(select a.*,row_number()over(partition by USER_ID,imei order by TERM_FUNC_ID desc ) rn 
 from term_market_structure_detail a 
 ) t where rn = 1 
select CUSTSTATUS_ID , count(0) , count(distinct CUSTSTATUS_ID ) 
from term_market_structure_detail 
group by  CUSTSTATUS_ID 
order by 1 


        insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/2206140 term_market_ratio
from    term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
           

ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select * from term_market_structure_detail


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value(((case when imei_cnt > 10 then 10 else imei_cnt end) -1)/10.1200 + (((case when imei_cnt > 100 then 100 else imei_cnt end)))*1.0000/1000,0.0000) change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 10 then 10 else imei_cnt end) -1)/10.1200 + (((case when imei_cnt > 100 then 100 else imei_cnt end)))*1.0000/1000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select CHANGE_TERM_PROBABILITY , count(0) , count(distinct CHANGE_TERM_PROBABILITY ) 
from term_market_structure_detail 
group by  CHANGE_TERM_PROBABILITY 
order by 1 


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 3 else imei_cnt end) -1)/5.1200 + (((case when imei_cnt > 100 then 100 else imei_cnt end)))*1.0000/1000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select max(CHANGE_TERM_PROBABILITY) from term_market_structure_detail
select CHANGE_TERM_PROBABILITY , count(0) , count(distinct CHANGE_TERM_PROBABILITY ) 
from term_market_structure_detail 
group by  CHANGE_TERM_PROBABILITY 
order by 1 

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 3 else imei_cnt end) -1)/5.1200 + (((case when imei_cnt > 100 then 100 else imei_cnt end)))/1000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


select count(0) from term_market_structure_detail where term_model_id is null


select * from  bass2.tac_info where 

select * from term_market_structure_detail

select CHANGE_TERM_PROBABILITY , count(0) , count(distinct CHANGE_TERM_PROBABILITY ) 
from term_market_structure_detail 
group by  CHANGE_TERM_PROBABILITY 
order by 1 

select CUSTTYPE_ID , count(0) , count(distinct CUSTTYPE_ID ) 
from term_market_structure_detail 
group by  CUSTTYPE_ID 
order by 1 


select CUSTSTATUS_ID , count(0) , count(distinct CUSTSTATUS_ID ) 
from term_market_structure_detail 
group by  CUSTSTATUS_ID 
order by 1 

select CUSTCLASS_ID , count(0) , count(distinct CUSTCLASS_ID ) 
from term_market_structure_detail 
group by  CUSTCLASS_ID 
order by 1 

select TERMPRICE_ID , count(0) , count(distinct TERMPRICE_ID ) 
from term_market_structure_detail 
group by  TERMPRICE_ID 
order by 1 
select TERMPRICE_ID , count(0) , count(distinct TERMPRICE_ID ) 
from term_market_structure_detail 
group by  TERMPRICE_ID 
order by 1 

select TERM_MODEL_ID , count(0) , count(distinct TERM_MODEL_ID ) 
from term_market_structure_detail 
group by  TERM_MODEL_ID 
order by 1 


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.WLAN_MARK = 1 then 5
	  when a.MMS_MARK = 1 then 4 
	  when a.GPRS_MARK = 1 then 3 
	  when a.JAVA_MARK = 1 then 2 
	  when a.WAP_MARK  = 1 then 1  else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 



select * from term_market_structure_detail


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(PRICE_RANGE_ID,case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end ) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


select PRICE_RANGE_ID , count(0) , count(distinct PRICE_RANGE_ID ) 
from term_market_price_range 
group by  PRICE_RANGE_ID 
order by 1 

select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case when e.term_model_id is not null then int(e.PRICE_RANGE) else 
 int(case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end)
end  termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case when e.term_model_id is not null then e.PRICE_RANGE else 
 case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end
end  termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,values(e.PRICE_RANGE, int(case 
						when substr(c.value_desc,1,4) >= '2008' then 5
						when substr(c.value_desc,1,4) >= '2006' then 4
						when substr(c.value_desc,1,4) >= '2004' then 3
						when substr(c.value_desc,1,4) >= '2002' then 2
						when substr(c.value_desc,1,4) <  '2002' then 1
						else 0 end)) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,value(e.PRICE_RANGE, case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end) termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
left join term_market_price_range e on a.TERMTYPE_NAME = e.term_model_id
where a.TAC_NUM is not null 

create table term_market_price_range
(
        term_model_id              VARCHAR(30)         
        ,PRICE_RANGE_ID         INTEGER             
        ,PRICE_RANGE            VARCHAR(20)    
)        
                 DISTRIBUTE BY HASH(term_model_id)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY      


create table term_market_price_range
(
        term_model_id              VARCHAR(30)         
        ,PRICE_RANGE_ID         INTEGER             
        ,PRICE_RANGE            VARCHAR(20)    
)        
                 DISTRIBUTE BY HASH("term_model_id")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY      
drop view t_v_price_range
create view t_v_price_range
as
insert into term_market_price_range
select  
mobile_no
,case when PRICE_RANGE = '3501以上' then 5
when PRICE_RANGE = '2501-3500元' then 4
when PRICE_RANGE = '1501-2500元' then 3
when PRICE_RANGE = '801-1500元' then 2
when PRICE_RANGE = '800元以下' then 1 end price_range_id
,price_range
from 
			(select mobile_no,price_range,row_number()over(partition by mobile_no order by price_range desc ) rn 
			from 
			(select distinct mobile_no,price_range from dim_imei_mobile) t1
) t2 where t2.rn = 1

select * from dim_term_price
select distinct price_range
from dim_imei_mobile

select * from dim_imei_mobile
select count(0),count(distinct mobile_no ) from 
(select mobile_no,price_range,row_number()over(partition by mobile_no order by price_range desc ) rn 
from 
(select distinct mobile_no,price_range from dim_imei_mobile) t1
) t2 where t2.rn = 1

select price_range from bass2.dw_product_mobilefunc_201011 a
join (select * from 
			(select mobile_no,price_range,row_number()over(partition by mobile_no order by price_range desc ) rn 
			from 
			(select distinct mobile_no,price_range from dim_imei_mobile) t1
) t2 where t2.rn = 1) t3 on a.termtype_name = t3.mobile_no 


select * from bass2.tac_info

select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
left join bass2.tac_info b on a.TAC_NUM = b.tac
left join (select * from BASS2.DIM_DEVICE_PROFILE where  property_id = '006004') c on a.device_id = c.device_id 
left join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null 




select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from term_market_structure 
group by  TERM_FUNC_ID 
order by 1 


select count(0)
from        
(select a.*,row_number()over(partition by USER_ID,imei order by TERM_FUNC_ID desc ) rn 
 from term_market_structure_detail a 
 ) t where rn = 1 
 
 1729834
 select count(0) from term_market_structure_detail
 
select count(0),count(user_id||imei) from term_market_structure_detail

delete from term_market_structure_detail where term_func_id = 0 
ALTER TABLE term_market_structure ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select sum(term_market_ratio) from term_market_structure
insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from        
(select a.*,row_number()over(partition by USER_ID,imei order by TERM_FUNC_ID desc ) rn 
 from term_market_structure_detail a 
 ) t where rn = 1 
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        
select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from term_market_structure_detail 
group by  TERM_FUNC_ID 
order by 1 

select * from bass2.tac_info


drop  TABLE "BASS2   "."TERM_USER_INFO" 
CREATE TABLE "BASS2   "."TERM_USER_INFO"  (
                  "OP_TIME" DATE , 
                  "CITY_ID" VARCHAR(7) , 
                  "BRAND_ID" SMALLINT , 
                  "TERM_BRAND_ID" VARCHAR(40) , 
                  "TERM_MODEL_ID" SMALLINT , 
                  "TERMPRICE_ID" SMALLINT , 
                  "TERM_FUNC_ID" SMALLINT , 
                  "CUST_NO" VARCHAR(15) , 
                  "CUSTCLASS_ID" SMALLINT , 
                  "CUSTSTATUS_ID" SMALLINT , 
                  "CUSTTYPE_ID" SMALLINT , 
                  "ONLINE_ID" SMALLINT , 
                  "SEX_ID" SMALLINT , 
                  "AGE_ID" SMALLINT , 
                  "INCOME" DECIMAL(12,2) , 
                  "TERM_FLAG" SMALLINT , 
                  "PRODUCT_ITEM_ID" BIGINT , 
                  "TERM_PREFERENCE_ID" VARCHAR(40) , 
                  "CHANGE_TERM_PROBABILITY" DECIMAL(12,4) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "CITY_ID",  
                 "CUST_NO")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  

ALTER TABLE term_user_info ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select * from dim_term_brand
select * from term_user_info

insert into term_user_info     
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700+((case when imei_cnt > 100 then 2 else imei_cnt end) -1)/1000.0000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'

                   

        
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,((case when imei_cnt > 5 then 2 else imei_cnt end) -1)/5.6700 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'



   
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,(imei_cnt-1)/10*1.0000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'




select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value((imei_cnt-1)/10*1.0000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'



select max(change_term_probability)
from (
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value((imei_cnt-1)/(select max(imei_cnt) from term_market_user_imei_cnt),0)*1.0000 change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'
) t
select d.imei_cnt
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select avg(imei_cnt)
from term_market_user_imei_cnt
group by imei_cnt


from bass2.dw_product_mobilefunc_201011 a  
join term_market_user_imei_cnt d on a.user_id = d.user_id 

select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value((imei_cnt-1)/(select max(imei_cnt) from term_market_user_imei_cnt),0) change_term_probability
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID product_item_id
,TERMBRAND_ID term_preference_id
,value((imei_cnt-1)/(select max(imei_cnt) from term_market_user_imei_cnt),0)
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
join term_market_user_imei_cnt d on a.user_id = d.user_id 
where a.TAC_NUM is not null and c.property_id = '006004'



select product_no,user_id from dw_product_20110213 where user_id = '89657333584882'
drop table term_market_user_imei_cnt 
create table term_market_user_imei_cnt
(
        USER_ID                VARCHAR(20)         
        ,IMEI_CNT               INTEGER  
)        
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY          

insert into term_market_user_imei_cnt
select user_id , count(distinct imei) IMEI_CNT
from BASS2.DW_PRODUCT_IMEI_RELA_DS
where FIRST_USE_DATE >= date('2009-11-01')
group by user_id 

select max(imei_cnt) from term_market_user_imei_cnt
        
select * from term_market_user_imei_cnt where imei_cnt = 1

select (select max(imei_cnt) from term_market_user_imei_cnt) from dual

drop         view t_v_user_imei_cnt
create view t_v_user_imei_cnt
as
insert into term_market_user_imei_cnt
select user_id,count(distinct imei) imei_cnt
from (
select user_id,imei from bass2.dw_product_mobilefunc_201011 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201010 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201009 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201008 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201007 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201006 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201005 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201004 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201003 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201002 a union all
select user_id,imei from bass2.dw_product_mobilefunc_201001 a union all
select user_id,imei from bass2.dw_product_mobilefunc_200911
) t
group by user_id
select max(imei_cnt) from term_market_user_imei_cnt

select DW_PRODUCT_MOBILEFUNC_201011

select * from syscat.tables where tabname like '%DW_PRODUCT_MOBILEFUNC%'
select * from DB2INST1.EXPLAIN_ARGUMENT

select count(0) from bass2.term_market_structure_detail a 

se 
select * from BASS2.DMRN_USER_IMEI_891_201012

select card,trim(a.tabschema)||'.'||a.tabname from syscat.tables a where tabname like '%DW_PRODUCT_IMEI_RELA%' 

select count(0) from BASS2.DW_PRODUCT_IMEI_RELA_201101

select  from BASS2.DW_PRODUCT_IMEI_RELA_201101


select * from BASS2.DW_PRODUCT_IMEI_RELA_DS where user_id = '89157332221448'


select count(0) from BASS2.DIM_IMEI_MOBILE
select distinct price_range from BASS2.DIM_IMEI_MOBILE
 BASS2.DIM_DEVICE_INFO
 select count(0),count(distinct imei ) from  BASS2.DIM_IMEI_MOBILE
 select * from BASS2.DIM_IMEI_MOBILE
select * from  BASS2.DIM_DEVICE_INFO
select a.* ,b.* from  BASS2.DIM_DEVICE_INFO a
join BASS2.DIM_IMEI_MOBILE b on a.DEVICE_NAME = b.mobile_no

select * from BASS2.DIM_IMEI_MOBILE
select count(0),count(distinct mobile_no ) from  (select distinct mobile_no,PRICE_RANGE from    BASS2.DIM_IMEI_MOBILE) t
PRICE_RANGE
#N/A
0
1501-2500元
2501-3500元
3501以上
800元以下
801-1500元
N
X

TERMPRICE_ID	TERMPRICE_NAME
0	未知
1	800元以下
2	801-1500元
3	1501-2500元
4	2501-3500元
5	3501以上

select TERM_BRAND_ID , count(0) , count(distinct TERM_BRAND_ID ) 
from term_market_structure_detail 
group by  TERM_BRAND_ID 
order by 1 
select * from dim_term_price

select 
select * from term_market_structure_detail where substr(imei,1,8)='35391201'

select * from term_market_structure_detail
where imei like  '%35565300%'

select count(0)from term_market_structure_detail a join DIM_IMEI_MOBILE b on substr(a.imei,1,8) = b.imei

select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID
,TERMBRAND_ID
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select * from BASS2.DIM_DEVICE_PROFILE 


insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG


 drop TABLE "BASS2   "."TERM_MARKET_STRUCTURE" 
CREATE TABLE "BASS2   "."TERM_MARKET_STRUCTURE"  (
                  "OP_TIME" DATE , 
                  "CITY_ID" VARCHAR(8) , 
                  "BRAND_ID" SMALLINT , 
                  "TERM_BRAND_ID" VARCHAR(40) , 
                  "TERM_MODEL_ID" SMALLINT , 
                  "TERM_FUNC_ID" SMALLINT , 
                  "TERMPRICE_ID" SMALLINT , 
                  "DEVICE_ID" VARCHAR(20) , 
                  "TERM_FLAG" SMALLINT , 
                  "TERM_COUNT" SMALLINT , 
                  "TERM_MARKET_RATIO" DECIMAL(12,5) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "CITY_ID",  
                 "BRAND_ID")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,case when (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 < 800 then 800 else (CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 end income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID
,TERMBRAND_ID
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,(CASE WHEN AGE = 0 OR AGE IS NULL THEN 20*100 ELSE  AGE*100 END)*(EDUCATION_ID+1)/4 income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID
,TERMBRAND_ID
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'



select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.TERMBRAND_ID term_brand_id
,b.term_model_id
,case 
	when substr(c.value_desc,1,4) >= '2008' then 5
	when substr(c.value_desc,1,4) >= '2006' then 4
	when substr(c.value_desc,1,4) >= '2004' then 3
	when substr(c.value_desc,1,4) >= '2002' then 2
	when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,CUST_ID CUST_NO
,CUSTCLASS_ID custclass_id
,USERSTATUS_ID custstatus_id 
,CUSTTYPE_ID 
,USER_ONLINE_ID 
,sex_id
,age_id
,AGE*100 income
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,PLAN_ID
,TERMBRAND_ID
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

        

select EDUCATION_ID , count(0) , count(distinct EDUCATION_ID ) 
from dw_product_mobilefunc_201011 
group by  EDUCATION_ID 
order by 1 
select * from dim_term_brand


select * from 
dim_cust_onlinelev

select USER_ONLINE,USER_ONLINE_ID from
dw_product_mobilefunc_201011
where 

select * from
dim_prod_up_product_item

select  count(user_id||imei) term_count from term_market_structure_detail
term_market_structure_detail
sele3ct * f
select count(0) from dim_pub_city


select * from dim_term_prod


select * from term_market_structure

insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

ALTER TABLE term_market_structure ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select count(0) from 
term_market_structure_detail

select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from term_market_structure_detail 
group by  TERM_FUNC_ID 
order by 1 
TERM_FUNC_ID	2
0	5322655
1	1466298
2	1545031
3	1757614
4	644413
5	2469

TERM_FUNC_ID	2
1	1466298
2	1545031
3	1757614
4	644413
5	2469

select wap_mark , count(0) , count(distinct wap_mark ) 
from bass2.dw_product_mobilefunc_201011 
group by  wap_mark 
order by 1 

select wlan_mark , count(0) , count(distinct wlan_mark ) 
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'
group by  wlan_mark 
order by 1 

ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select wlan_mark , count(0) , count(distinct wlan_mark ) 
from bass2.dw_product_mobilefunc_201011 
group by  wlan_mark 
order by 1 


select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from term_market_structure_detail 
group by  TERM_FUNC_ID 
order by 1 

delete from term_market_structure_detail 
where term_func_id = 0 and  user_id in  (select user_id from  term_market_structure_detail where term_func_id > 0 )


select count(0)
delete from term_market_structure_detail 
where term_func_id = 0 and  exists (select 1 from  term_market_structure_detail where term_func_id > 0 )
         
insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'





insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.gprs_mark = 1 then 2 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 1 then 3 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.java_mark = 1 then 4 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'



delete from term_market_structure_detail 
where term_func_id = 0 and  exists (select 1 from  term_market_structure_detail where term_func_id > 0 )


insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select wap_mark , count(0) , count(distinct wap_mark ) 
from bass2.dw_product_mobilefunc_201011 
group by  wap_mark 
order by 1 


select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from term_market_structure_detail 
group by  TERM_FUNC_ID 
order by 1 


5415825
select count(0)
from term_market_structure_detail 
where term_func_id = 0 and  exists (select 1 from  term_market_structure_detail where term_func_id > 0 )

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.gprs_mark = 1 then 2 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 1 then 3 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.java_mark = 1 then 4 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

ALTER TABLE term_market_structure_detail ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

select * from 
dim_term_func
order by 1



insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.gprs_mark = 1 then 2 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


drop table term_market_structure_detail
create table term_market_structure_detail
(
         OP_TIME                DATE                
        ,CITY_ID                VARCHAR(7)          
        ,BRAND_ID               BIGINT              
        ,PRODUCT_NO             VARCHAR(15)         
        ,USER_ID                VARCHAR(20)         
        ,IMEI                   VARCHAR(18)         
        ,TERM_BRAND_ID          VARCHAR(20)         
        ,TAC_NUM                VARCHAR(10)         
        ,TERM_MODEL_ID          SMALLINT            
        ,TERM_FUNC_ID           INTEGER             
        ,DEVICE_ID              VARCHAR(16)         
        ,TERM_FLAG              INTEGER             
        ,TERMPRICE_ID           INTEGER  
)        
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY       


select * from term_market_structure

select sum(TERM_MARKET_RATIO) from term_market_structure 
insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

5415825	1151856	1729834	1249605	1729834

select count(0),count(distinct product_no)
,count(distinct user_id)
,count(distinct imei)
,count(distinct user_id||imei)
from  bass2.term_market_structure_detail

select count(0),
select count(0),count(distinct imei||product_no),count(distinct imei) from  term_market_structure_detail

select TERM_FUNC_ID , count(0) , count(distinct TERM_FUNC_ID ) 
from bass2.term_market_structure_detail 
group by  TERM_FUNC_ID 
order by 1 

select CITY_ID , count(0) , count(distinct CITY_ID ) 
from term_market_structure_detail 
group by  CITY_ID 
order by 1 

select BRAND_ID , count(0) , count(distinct BRAND_ID ) 
from term_market_structure_detail 
group by  BRAND_ID 
order by 1 


select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/1729834 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

select count(0) from term_market_structure_detail


select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(distinct product_no) term_count
		,count(0)/1729834*100.00 term_market_ratio
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        

select count(distinct user_id) from term_market_structure_detail
1249605

select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(0) term_count
		,count(distinct imei) term_count
from         term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG

delete from term_market_structure_detail where term_func_id = 0 


select count(0) from term_market_structure_detail


insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wlan_mark = 1 then 5 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'




insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.java_mark = 1 then 4 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'



insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 1 then 3 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'




insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.gprs_mark = 1 then 2 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure_detail
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

insert into term_market_structure
select 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,count(user_id||imei) term_count
		,count(user_id||imei)*100.0000/2206140 term_market_ratio
from    term_market_structure_detail
group by 
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,TERM_BRAND_ID
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,TERMPRICE_ID
        ,DEVICE_ID
        ,TERM_FLAG


select * from term_market_structure

create table term_market_structure_detail
(
         OP_TIME                DATE                
        ,CITY_ID                VARCHAR(7)          
        ,BRAND_ID               BIGINT              
        ,PRODUCT_NO             VARCHAR(15)         
        ,USER_ID                VARCHAR(20)         
        ,IMEI                   VARCHAR(18)         
        ,TERM_BRAND_ID          VARCHAR(20)         
        ,TAC_NUM                VARCHAR(10)         
        ,TERM_MODEL_ID          SMALLINT            
        ,TERM_FUNC_ID           INTEGER             
        ,DEVICE_ID              VARCHAR(16)         
        ,TERM_FLAG              INTEGER             
        ,TERMPRICE_ID           INTEGER  
)        
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY     

drop view t_v_term_market_structure
create table t_v_term_market_structure(
         OP_TIME
        ,CITY_ID
        ,BRAND_ID
        ,PRODUCT_NO
        ,USER_ID
        ,IMEI
        ,TERM_BRAND_ID
        ,TAC_NUM
        ,TERM_MODEL_ID
        ,TERM_FUNC_ID
        ,DEVICE_ID
        ,TERM_FLAG
        ,TERMPRICE_ID
        ) 
as
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,a.device_id
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select case when value(dist_date,'0') like 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID term_brand_id
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,a.device_id
--,c.value_desc dist_date
,case when USERTYPE_ID = 1 then 0 else 1 end term_flag
,case 
when substr(c.value_desc,1,4) >= '2008' then 5
when substr(c.value_desc,1,4) >= '2006' then 4
when substr(c.value_desc,1,4) >= '2004' then 3
when substr(c.value_desc,1,4) >= '2002' then 2
when substr(c.value_desc,1,4) <  '2002' then 1
else 0 end termprice_id
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

select * from dim_term_price

select c.value_desc,count(0)
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'
group by c.value_desc
select count(0)
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_PROFILE c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'


select USERTYPE_ID,count(0)
from dw_product_mobilefunc_201011
group by USERTYPE_ID


select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
where a.TAC_NUM is not null 

select * from  bass2.tac_info

select * from dim_term_model

select CHANNEL_ID,CHANNELTYPE_ID from dw_product_mobilefunc_201011 

select a.channel_name from dim_pub_channel a
join dw_product_mobilefunc_201011 b on a.CHANNEL_ID = b.CHANNEL_ID

select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,b.term_model_id
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,c.value_desc
,0 term_flag
from bass2.dw_product_mobilefunc_201011 a  
join bass2.tac_info b on a.TAC_NUM = b.tac
join BASS2.DIM_DEVICE_INFO c on a.device_id = c.device_id 
where a.TAC_NUM is not null and c.property_id = '006004'

select * from dim_pub_termbrand
select * from dim_term_model

SELECT * FROM BASS2.DIM_DEVICE_INFO


SELECT count(0) FROM BASS2.DIM_DEVICE_INFO
SELECT * FROM BASS2.DIM_DEVICE_INFO_200903
SELECT * FROM BASS2.DIM_DEVICE_INFO_200912
SELECT * FROM BASS2.DIM_DEVICE_INFO_201003
SELECT * FROM BASS2.DIM_DEVICE_INFO_201005OLD
SELECT * FROM BASS2.DIM_DEVICE_PROFILE where value_desc is not null and value_desc like '%2009%'
select * from BASS2.DIM_DEVICE_PROFILE where property_id = '006004'
SELECT * FROM BASS2.DIM_DEVICE_PROFILE_200903
SELECT * FROM BASS2.DIM_DEVICE_PROFILE_200912
SELECT * FROM BASS2.DIM_DEVICE_PROFILE_201003
SELECT * FROM BASS2.DIM_DEVICE_PROFILE_201005OLD
SELECT * FROM BASS2.DIM_DEVICEPRICE_INFO
SELECT * FROM BASS2.DIM_PUB_CHANNEL_DEVELOP_QUALITY_DETAIL_TYPE_ST
SELECT * FROM BASS2.DIM_TACNUM_DEVID
SELECT * FROM BASS2.DIM_TACNUM_DEVID_200903
SELECT * FROM BASS2.DIM_TACNUM_DEVID_200912
SELECT * FROM BASS2.DIM_TACNUM_DEVID_201003
SELECT * FROM BASS2.DIM_TACNUM_DEVID_201005OLD
SELECT count(0) FROM NUTS.DIM_DEVICE_INFO


select * from BASS2.DIM_APP_NB_SINGLEPRICE
select * from BASS2.DIM_CL_PRICE_LVL_ST
select * from BASS2.DIM_DEVICEPRICE_INFO
select * from BASS2.DIM_TERM_PRICE
select * from NUTS.DIM_CRING_PRICELEV
select * from REPORT.DIM_TERM_PRICE



select * from BASS2.DIM_TERM_APP_STS
select * from BASS2.DIM_TERM_BRAND
select * from BASS2.DIM_TERM_FLAG
select * from BASS2.DIM_TERM_FUNC
select * from BASS2.DIM_TERM_FUNCTION
select * from BASS2.DIM_TERM_FUNCTION_COM
select * from BASS2.DIM_TERM_MODEL
select * from BASS2.DIM_TERM_NUMBER_STS
select * from BASS2.DIM_TERM_PRICE
select * from BASS2.DIM_TERM_PROD
select * from BASS2.DIM_TERM_PRODIVER
select * from BASS2.DIM_TERM_STATE
select * from BASS2.DIM_TERM_SUBSIDY_TYPE
select * from BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_20100928BAK
select * from BASS2.DIM_TERM_TAC_20101029BAK
select * from BASS2.DIM_TERM_TAC_20101129BAK
select * from BASS2.DIM_TERM_TAC_20101229BAK
select * from BASS2.DIM_TERM_TAC_20110129BAK
select * from BASS2.DIM_TERM_TYPE
select * from BASS2.DIM_TERM_TYPE_ST
select * from BASS2.DIM_TERMBRAND
select * from BASS2.DIM_TERMSUP_APPTYPE_PARA
select * from BASS2.DIM_TERMSUP_INDUSTRYTYPE_PARA
select * from BASS2.DIM_TERMSUP_TERMTYPE_PARA
select * from BASS2.DIM_VAOCATION_APP_TERM_ST
select * from NGBASS20.DIM_G3_TERM
select * from NGBASS20.DIM_TERM_PROFILE
select * from NGBASS20.DIM_TERMINAL_REASON
select * from NUTS.DIM_TERMINAL_BRAND
select * from NUTS.DIM_TERMINAL_TYPE
select * from REPORT.DIM_TERM_BRAND
select * from REPORT.DIM_TERM_MODEL
select * from REPORT.DIM_TERM_PACK_TYPE
select * from REPORT.DIM_TERM_PRICE
select * from REPORT.DIM_TERM_PRODUCT
select * from REPORT.DIM_TERM_SERV_PLATFORM
select * from REPORT.DIM_TERM_SERV_TYPE



select * from BASS2.DIM_APP_NB_TERMBRAND
select * from BASS2.DIM_APP_NB_TERMFACTORY
select * from BASS2.DIM_APP_NB_TERMTYPE

select * from BASS2.DIM_NG_TERM_CYCLE
select * from BASS2.DIM_NG1_TERM_FUNC
select * from BASS2.DIM_NG1_TERM_FUNC_CFG
select * from BASS2.DIM_PUB_TERMBRAND


select * from BASS2.DIM_TERM_APP_STS 
select * from BASS2.DIM_TERM_BRAND   
select * from BASS2.DIM_TERM_FLAG    
select * from BASS2.DIM_TERM_FUNC    
select * from BASS2.DIM_TERM_FUNCTION
select * from BASS2.DIM_TERM_FUNCTION
select * from BASS2.DIM_TERM_MODEL   
select * from BASS2.DIM_TERM_NUMBER_S
select * from BASS2.DIM_TERM_PRICE   
select * from BASS2.DIM_TERM_PROD    
select * from BASS2.DIM_TERM_PRODIVER
select * from BASS2.DIM_TERM_STATE   
select * from BASS2.DIM_TERM_SUBSIDY_
select * from BASS2.DIM_TERM_TAC     
select * from BASS2.DIM_TERM_TAC_2010
select * from BASS2.DIM_TERM_TAC_2010
select * from BASS2.DIM_TERM_TAC_2010
select * from BASS2.DIM_TERM_TAC_2010
select * from BASS2.DIM_TERM_TAC_2011
select * from BASS2.DIM_TERM_TYPE    
select * from BASS2.DIM_TERM_TYPE_ST 
select * from BASS2.DIM_TERMBRAND    
select * from BASS2.DIM_TERMSUP_APPTY
BASS2.DIM_TERMSUP_INDUSTRYTYPE_PARA  
BASS2.DIM_TERMSUP_TERMTYPE_PARA      
BASS2.DIM_VAOCATION_APP_TERM_ST      
NGBASS20.DIM_G3_TERM                 
NGBASS20.DIM_TERM_PROFILE            
NGBASS20.DIM_TERMINAL_REASON         
NUTS.DIM_TERMINAL_BRAND              
NUTS.DIM_TERMINAL_TYPE               
REPORT.DIM_TERM_BRAND                
REPORT.DIM_TERM_MODEL                
REPORT.DIM_TERM_PACK_TYPE            
REPORT.DIM_TERM_PRICE                
REPORT.DIM_TERM_PRODUCT              
REPORT.DIM_TERM_SERV_PLATFORM        
REPORT.DIM_TERM_SERV_TYPE            

select * from DIM_DEVICE_INFO

create view t_v_term_001
as
select 
op_time
,city_id
,brand_id
,TERMBRAND_ID
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag
,count(0) term_count
,count(distinct imei) term_count
--,count(distinct imei||user_id) term_count
from 
(
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
) t 
group by 
op_time
,city_id
,brand_id
,TERMBRAND_ID
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag

select * from 


select * from dim_term_price

select count(0) from dw_product_mobilefunc_201011
create view t_v_term_001
as
select 
op_time
,city_id
,brand_id
,TERMBRAND_ID
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag
,count(0) term_count
,count(distinct imei) term_count
,count(distinct imei||user_id) term_count
from 
(
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
) t 
group by 
op_time
,city_id
,brand_id
,TERMBRAND_ID
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag

select 
op_time
,city_id
,brand_id
,term_brand_id
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag
,count(distinct imei) term_count
from 
(
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
) t 
group by 
op_time
,city_id
,brand_id
,term_brand_id
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag


select 
op_time
,city_id
,brand_id
,term_brand_id
--,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag
,count(distinct imei) term_count
from 
(
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01') op_time
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
) t 
group by 
op_time
,city_id
,brand_id
,term_brand_id
,term_model_id
,term_func_id
,termprice_id
,device_id
,term_flag



select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a where a.TAC_NUM is not null 
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a  where a.TAC_NUM is not null 





select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wap_mark = 1 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.gprs_mark = 2 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.mms_mark = 3 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.java_mark = 4 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a
union all 
select date('2010-11-01')
,a.CITY_ID
,a.BRAND_ID
,a.PRODUCT_NO
,a.USER_ID
,a.IMEI
,a.TERMBRAND_ID
,a.TAC_NUM
,case when a.wlan_mark = 5 then 1 else 0 end term_func_id
,' ' termprice_id
,a.device_id
,0 term_flag
from dw_product_mobilefunc_201011 a

select * from DIM_DEVICEPRICE_INFO


select * from dim_term_func

select * from dw_product_mobilefunc_20110211

select * from dim_term_func
select count(0) from ODS_RES_CTMS_EXCHG_20110211

select trim(a.tabschema)||'.'||a.tabname from syscat.tables a where tabname like '%RES_CTMS_EXCHG%' 

select * from DIM_APP_NB_TERMBRAND
select count(0) from BASS2.DW_RES_CTMS_EXCHG_20110101



select * from DIM_DEVICE_INFO
select count(0),count(distinct device_id ) from  DIM_DEVICE_INFO
select * from DW_RES_SALE_ORDER_201101

select * from report.DIM_TERM_PRICE


DW_RES_CTMS_EXCHG_201101

select * from DW_RES_CTMS_EXCHG_20110120


select * from ODS_RES_CTMS_EXCHG_20110228


select year_month('2011-01-01') from dual

select * from bass2.tac_info

select * from dim_term_price

select * from dim_term_prod
select * from dim_term_flag


select * from dim_term_func

insert into bass2.tac_info
select distinct a.tac_num ,a.TERMPROD_ID,b.TERM_MODEL_ID
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC
join dim_pub_termbrand c on a.TERMPROD_ID = c.TERMBRAND_ID

select distinct a.tac_num ,int(a.TERMPROD_ID),b.TERM_MODEL_ID
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC
join dim_pub_termbrand c on a.TERMPROD_ID = c.TERMBRAND_ID
select * from dim_pub_termbrand

select * from dim_property_value_range

select * from dim_term_price
select * from dim_term_brand
select * from DIM_TACNUM_DEVID
select distinct int(c.TERM_BRAND_ID)
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC
join dim_term_brand c on a.TERMPROD_NAME = c.TERM_BRAND_NAME

select * from DW_TDTERM_IMEI_INFO_201004

select * from BASS2.DIM_TERM_TAC


select a.*,b.TERM_MODEL_DESC

select * from DW_TDTERM_IMEI_INFO_201011

select * from ST_AI_CHL_TERM_SALE_RATE_DM

select * from DW_PRODUCT_MOBILEFUNC_201101

insert into bass2.tac_info
select distinct a.tac_num ,int(c.TERM_BRAND_ID),b.TERM_MODEL_ID
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC
join dim_term_brand c on a.TERMPROD_NAME = c.TERM_BRAND_NAME

select * from DW_NG1_ENT_TERM_USER_201101
select * from DWD_TERMSUP_USER_TERM_RELATION_20110210

select a.* from syscat.tables a where tabname like '%MOBILEFUNC%' 

select * from BASS2.DIM_TERM_TAC where TERM_MODEL like '%ollpad%'
select distinct a.TERM_MODEL ,a.TERMPROD_ID,b.TERM_MODEL_ID
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC

drop table "BASS2   "."TAC_INFO" 
CREATE TABLE "BASS2   "."TAC_INFO"  (
                  "TAC" VARCHAR(10) , 
                  "TERM_BRAND_ID" INTEGER , 
                  "TERM_MODEL_ID" SMALLINT )   
                 DISTRIBUTE BY HASH("TAC",  
                 "TERM_BRAND_ID")   
                   IN "TBS_REPORT" INDEX IN "TBS_INDEX"  

select count(0),count(distinct tac ) from  bass2.tac_info

insert into bass2.tac_info
select distinct a.tac_num ,int(a.TERMPROD_ID),b.TERM_MODEL_ID
from BASS2.DIM_TERM_TAC a 
join dim_term_model b on a.TERM_MODEL = b.TERM_MODEL_DESC


select * from BASS2.DIM_TERM_TAC
select * from bass2.tac_info
select * from dim_term_model
select * from dim_term_model where term_brand_id = '006010021'
select tac_num,
select * from bass2.term_market_structure
select * from dim_prod_up_product_item


select * from app.sch_control_task where control_code like '%0504%'

select * from etl_load_table_map where  
task_id like '%0504%'

select * from 
select count(0),count(distinct table_id ) from USYS_TABLE_MAINTAIN
select * from DW_CM_BUSI_RADIUS_201101
select * from dw_cm_busi_radius_201101

drop table DW_CM_BUSI_RADIUS_201101


DWD_CM_BUSI_RADIUS_

select * from DW_CM_BUSI_RADIUS_200912

select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_CM_BUSI_RADIUS where name not like '%BAK') a
join 
(select  *  from t_stat_DW_CM_BUSI_RADIUS where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


DROP TABLE "BASS2   "."DW_CM_BUSI_RADIUS_YYYYMM"
CREATE TABLE "BASS2   "."DW_CM_BUSI_RADIUS_YYYYMM"  (
                  "OP_TIME" DATE , 
                  "USER_ID" VARCHAR(20) , 
                  "BUSI_TYPE" VARCHAR(20) , 
                  "SP_CODE" VARCHAR(20) , 
                  "SERVICE_CODE" VARCHAR(32) , 
                  "CITY_ID" VARCHAR(7) , 
                  "AUTH_TYPE" SMALLINT , 
                  "AUTH_ID_TYPE" SMALLINT , 
                  "AUTH_ID_VALUE" VARCHAR(32) , 
                  "AUTH_PASSWORD" VARCHAR(32) , 
                  "APPLY_TYPE" SMALLINT , 
                  "USER_NAME" VARCHAR(64) , 
                  "USER_QUESTION" VARCHAR(64) , 
                  "USER_ANSWER" VARCHAR(64) , 
                  "PROPERTY" VARCHAR(20) , 
                  "CREATE_DATE" DATE , 
                  "ACT_TYPE" BIGINT , 
                  "VALID_DATE" TIMESTAMP , 
                  "EXPIRE_DATE" TIMESTAMP , 
                  "SO_NBR" VARCHAR(100) , 
                  "USER_LEVEL" VARCHAR(2) , 
                  "PASS_USE_TYPE" VARCHAR(2) , 
                  "PLAN_ID" VARCHAR(20) , 
                  "UPDATE_LOGIC" VARCHAR(2) , 
                  "REMARK" VARCHAR(255) , 
                  "THIRD_NUM" VARCHAR(16) , 
                  "BILL_FLAG" SMALLINT , 
                  "CHG_FLAG" SMALLINT , 
                  "PKG_SEQ" VARCHAR(20) , 
                  "SEQ" VARCHAR(20) , 
                  "RSLT" VARCHAR(4) , 
                  "SO_MODE" VARCHAR(5) , 
                  "EXT_HOLDS" VARCHAR(20) , 
                  "EXT_HOLDS1" VARCHAR(20) , 
                  "EXT_HOLDS2" VARCHAR(20) , 
                  "EXT_HOLDS3" VARCHAR(20) , 
                  "EXT_HOLDS4" VARCHAR(20) , 
                  "EXT_HOLDS5" VARCHAR(64) )   
                 DISTRIBUTE BY HASH(user_id,service_code,create_date,ext_holds2)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

select * from DW_CM_BUSI_RADIUS_200912


select * from DW_CM_BUSI_RADIUS_201005_BAK

select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_CM_BUSI_RADIUS where name not like '%BAK') a
join 
(select  *  from t_stat_DW_CM_BUSI_RADIUS where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1

drop table  DW_CM_BUSI_RADIUS_200806_BAK 


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_CM_BUSI_RADIUS where name not like '%BAK') a
join 
(select  *  from t_stat_DW_CM_BUSI_RADIUS where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1

insert into t_stat_DW_CM_BUSI_RADIUS select 'DW_CM_BUSI_RADIUS_200806', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_CM_BUSI_RADIUS_200806 group by nodenumber(USER_ID) 
insert into t_stat_DW_CM_BUSI_RADIUS select 'DW_CM_BUSI_RADIUS_200806_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_CM_BUSI_RADIUS_200806_BAK group by nodenumber(USER_ID) 



select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name not like '%BAK') a
join 
(select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1

delete from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name like 'DW_CM_BUSI_RADIUS%'
create table DW_CM_BUSI_RADIUS_200806 like DW_CM_BUSI_RADIUS_200806_BAK DISTRIBUTE BY HASH(user_id,service_code,create_date,ext_holds2) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
insert into DW_CM_BUSI_RADIUS_200806 select * from  DW_CM_BUSI_RADIUS_200806_BAK 
insert into t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM select 'DW_CM_BUSI_RADIUS_200806', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_CM_BUSI_RADIUS_200806 group by nodenumber(USER_ID) 
insert into t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM select 'DW_CM_BUSI_RADIUS_200806_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_CM_BUSI_RADIUS_200806_BAK group by nodenumber(USER_ID) 
rename DW_CM_BUSI_RADIUS_200806 to DW_CM_BUSI_RADIUS_200806_BAK

select * from DW_CM_BUSI_RADIUS_200806_BAK

create table DW_CM_BUSI_RADIUS_200806 like DW_CM_BUSI_RADIUS_200806_BAK DISTRIBUTE BY HASH(user_id,service_code,create_date,ext_holds2,DATA_ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 


create table t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing


select count(0) cnt from DW_CM_BUSI_RADIUS_200805 fetch first 100 rows only
select count(0) cnt from DW_CM_BUSI_RADIUS_200801 fetch first 100 rows only


select * from DW_CM_BUSI_RADIUS_201001


select * from DW_CM_BUSI_RADIUS_201011 fetch first 100 rows only 

select count(0) from DW_CM_BUSI_RADIUS_201012
select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'DW_CM_BUSI_RADIUS_201001'


select * from DW_CM_BUSI_RADIUS_201011


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name not like '%BAK') a
join 
(select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


select * from DW_KF_SEND_SMS_DATA_DM_200911

select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name not like '%BAK') a
join 
(select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1





select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name not like '%BAK') a
join 
(select  *  from t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1




select * from DW_KF_SEND_SMS_DATA_DM_200907


create table t_stat_DW_KF_SEND_SMS_DATA_DM_YYYYMM(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing



select op_time,count(0) from dw_kf_send_sms_data_dm_201101
group by op_time


drop TABLE "BASS2   "."DWD_KF_SEND_SMS_DATA_YYYYMMDD" 
CREATE TABLE "BASS2   "."DWD_KF_SEND_SMS_DATA_YYYYMMDD"  (
                  "DATA_ID" DECIMAL(9,0) , 
                  "SMS_ID" DECIMAL(9,0) , 
                  "OP_ID" BIGINT , 
                  "PHONE_ID" VARCHAR(20) , 
                  "SERVICE_CODE" VARCHAR(20) , 
                  "REGION_CODE" INTEGER , 
                  "COUNTY_CODE" INTEGER , 
                  "CREATE_DATE" TIMESTAMP , 
                  "STS" INTEGER , 
                  "SEND_COUNT" INTEGER , 
                  "CHARGE_TYPE" INTEGER , 
                  "CHARGE_MSISDN" VARCHAR(32) , 
                  "SERVICE_ID" VARCHAR(20) , 
                  "FEE_TYPE" VARCHAR(2) , 
                  "FEE_CODE" VARCHAR(6) , 
                  "MUTI_USER_FLAG" INTEGER , 
                  "REGISTER_DEL" INTEGER , 
                  "SEND_LEVEL" INTEGER , 
                  "FIRST_SEND_DT" TIMESTAMP , 
                  "SMS_TEXT" VARCHAR(1000) , 
                  "PKEY" INTEGER , 
                  "REMARK" VARCHAR(200) )   
                 DISTRIBUTE BY HASH("SMS_ID","DATA_ID")   
                   IN "TBS_CDR_DATA" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

drop table "BASS2   "."DWD_DSMP_SP_OPER_CODE_YYYYMM" 
CREATE TABLE "BASS2   "."DWD_DSMP_SP_OPER_CODE_YYYYMM"  (
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_CODE" VARCHAR(10) , 
                  "SP_OPER_NAME" VARCHAR(64) , 
                  "OPER_KIND_ID" INTEGER , 
                  "SERV_STATU_ID" CHAR(1) , 
                  "SERV_OPER_ID" INTEGER , 
                  "OPER_BEAR_ID" CHAR(1) , 
                  "ON_DEMANDFLAG" CHAR(1) , 
                  "SERV_ATTRIBUTE" CHAR(1) , 
                  "MISC_ID" VARCHAR(4) , 
                  "CONNECT_EPMENT_ID" VARCHAR(10) , 
                  "EPM_KIND_ID" INTEGER , 
                  "WAP_ABET_VER" CHAR(1) , 
                  "BILLING_TYPE_ID" VARCHAR(2) , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH(SP_OPER_CODE,SP_CODE)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
drop  TABLE "BASS2   "."DWD_DSMP_SP_OPER_CODE_MODIFY_DM_YYYYMM" 
CREATE TABLE "BASS2   "."DWD_DSMP_SP_OPER_CODE_MODIFY_DM_YYYYMM"  (
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_CODE" VARCHAR(10) , 
                  "MODIFY_TIME" TIMESTAMP , 
                  "SP_OPER_NAME" VARCHAR(64) , 
                  "OPER_KIND_ID" INTEGER , 
                  "SERV_STATU_ID" CHAR(1) , 
                  "SERV_OPER_ID" INTEGER , 
                  "OPER_BEAR_ID" CHAR(1) , 
                  "ON_DEMANDFLAG" CHAR(1) , 
                  "SERV_ATTRIBUTE" CHAR(1) , 
                  "MISC_ID" VARCHAR(4) , 
                  "EPMENT_ID" VARCHAR(10) , 
                  "EPM_KIND_ID" INTEGER , 
                  "WAP_ABET_VER" CHAR(1) , 
                  "BILLING_TYPE_ID" VARCHAR(2) , 
                  "USER_MODIFY_TYPE_ID" CHAR(1) , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH(SP_OPER_CODE,SP_CODE,BILL_DATE)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

drop  TABLE "BASS2   "."ODS_KF_SEND_SMS_DATA_YYYYMMDD"
CREATE TABLE "BASS2   "."ODS_KF_SEND_SMS_DATA_YYYYMMDD"  (
                  "DATA_ID" DECIMAL(9,0) , 
                  "SMS_ID" DECIMAL(9,0) , 
                  "OP_ID" DECIMAL(8,0) , 
                  "PHONE_ID" VARCHAR(20) , 
                  "SERVICE_CODE" VARCHAR(20) , 
                  "REGION_CODE" INTEGER , 
                  "COUNTY_CODE" INTEGER , 
                  "CREATE_DATE" TIMESTAMP , 
                  "STS" INTEGER , 
                  "SEND_COUNT" INTEGER , 
                  "CHARGE_TYPE" INTEGER , 
                  "CHARGE_MSISDN" VARCHAR(32) , 
                  "SERVICE_ID" VARCHAR(10) , 
                  "FEE_TYPE" VARCHAR(2) , 
                  "FEE_CODE" VARCHAR(6) , 
                  "MUTI_USER_FLAG" INTEGER , 
                  "REGISTER_DEL" INTEGER , 
                  "SEND_LEVEL" INTEGER , 
                  "FIRST_SEND_DT" TIMESTAMP , 
                  "SMS_TEXT" VARCHAR(1000) , 
                  "PKEY" INTEGER , 
                  "REMARK" VARCHAR(200) )   
                 DISTRIBUTE BY HASH("SMS_ID","DATA_ID")   
                   IN "TBS_CDR_DATA" INDEX IN "TBS_INDEX"  




drop TABLE "BASS2   "."DW_KF_SEND_SMS_DATA_DM_YYYYMM"
CREATE TABLE "BASS2   "."DW_KF_SEND_SMS_DATA_DM_YYYYMM"  (
                  "OP_TIME" DATE , 
                  "DATA_ID" DECIMAL(9,0) , 
                  "SMS_ID" DECIMAL(9,0) , 
                  "OP_ID" BIGINT , 
                  "PHONE_ID" VARCHAR(20) , 
                  "SERVICE_CODE" VARCHAR(20) , 
                  "REGION_CODE" INTEGER , 
                  "COUNTY_CODE" INTEGER , 
                  "CREATE_DATE" TIMESTAMP , 
                  "STS" INTEGER , 
                  "SEND_COUNT" INTEGER , 
                  "CHARGE_TYPE" INTEGER , 
                  "CHARGE_MSISDN" VARCHAR(32) , 
                  "SERVICE_ID" VARCHAR(20) , 
                  "FEE_TYPE" VARCHAR(2) , 
                  "FEE_CODE" VARCHAR(6) , 
                  "MUTI_USER_FLAG" INTEGER , 
                  "REGISTER_DEL" INTEGER , 
                  "SEND_LEVEL" INTEGER , 
                  "FIRST_SEND_DT" DATE , 
                  "SMS_TEXT" VARCHAR(1000) , 
                  "PKEY" INTEGER , 
                  "REMARK" VARCHAR(200) )   
                 DISTRIBUTE BY HASH("SMS_ID","DATA_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  


select * from Dwd_kf_send_sms_data_20110125

update USYS_TABLE_MAINTAIN set PARTITION_KEY = 'SMS_ID,DATA_ID' where table_id = 10136 
and table_name = 'ODS_KF_SEND_SMS_DATA'

select 'DW_KF_SEND_SMS_DATA_DM_201101_BAK', nodenumber(DATA_ID) ,count(*) as using_num from bass2.DW_KF_SEND_SMS_DATA_DM_201101_BAK group by nodenumber(DATA_ID) 


select * from DWD_KF_SEND_SMS_DATA_20101231



update USYS_TABLE_MAINTAIN set PARTITION_KEY = 'SP_OPER_CODE，SP_CODE' where table_id = 221 
and table_name = 'DWD_DSMP_SP_OPER_CODE'



update USYS_TABLE_MAINTAIN set PARTITION_KEY = 'SP_OPER_CODE,SP_CODE,BILL_DATE' where table_id = 203 
and table_name = 'DWD_DSMP_SP_OPER_CODE_MODIFY_DM'



select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_stat_DW_OW_LOADING_LOG_DS where name not like '%BAK') a
join 
(select  *  from t_stat_DW_OW_LOADING_LOG_DS where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


select * from DW_OW_LOADING_LOG_20101124


select * from DW_OW_LOADING_LOG_201012


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_OW_LOADING_LOG_stats where name not like '%BAK') a
join 
(select  *  from t_DW_OW_LOADING_LOG_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
where a.name = 'DW_OW_LOADING_LOG_201002'
order by 2,1

select count(0) from DW_OW_LOADING_LOG_20100117
select * from t_stat_DW_OW_LOADING_LOG_DS

create table t_stat_DW_OW_LOADING_LOG_DS(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing


select * from t_stat_DW_OW_LOADING_LOG_DS order by 1,2


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_OW_LOADING_LOG_stats where name not like '%BAK') a
join 
(select  *  from t_DW_OW_LOADING_LOG_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1

select count(0) from t_stat_DW_OW_LOADING_LOG_DS



select * from nuts.ODS_SP_BUSN_ACTION_201002

select * from nuts.ODS_SP_BUSN_ACTION_201012

select count(0) from DW_CM_BUSI_RADIUS_201012



select a.* from syscat.tables a where tabname like '%DW_CM_BUSI_RADIUS_%' and len

select * from DW_OW_LOADING_LOG_201002

select 'DWD_DSMP_SP_OPER_CODE_MODIFY_DM', nodenumber(SP_CODE) ,count(*) as using_num from bass2.DWD_DSMP_SP_OPER_CODE_MODIFY_DM_201011 group by nodenumber(SP_CODE) 

select * from sys

select * from DW_OW_LOADING_LOG_201001
drop table  DW_OW_LOADING_LOG_200801_BAK 
drop table  DW_OW_LOADINGo_200802_BAK 
drop table  DW_OW_LOADING_LOG_200805_BAK 
drop table  DW_OW_LOADING_LOG_200806_BAK 
drop table  DW_OW_LOADING_LOG_200807_BAK 
drop table  DW_OW_LOADING_LOG_200808_BAK 
drop table  DW_OW_LOADING_LOG_200809_BAK 
drop table  DW_OW_LOADING_LOG_200810_BAK 
drop table  DW_OW_LOADING_LOG_200811_BAK 
drop table  DW_OW_LOADING_LOG_200812_BAK 
drop table  DW_OW_LOADING_LOG_200901_BAK 
drop table  DW_OW_LOADING_LOG_200903_BAK 
drop table  DW_OW_LOADING_LOG_200904_BAK 
drop table  DW_OW_LOADING_LOG_200905_BAK 
drop table  DW_OW_LOADING_LOG_200906_BAK 
drop table  DW_OW_LOADING_LOG_200907_BAK 
drop table  DW_OW_LOADING_LOG_200908_BAK 
drop table  DW_OW_LOADING_LOG_200909_BAK 
drop table  DW_OW_LOADING_LOG_200910_BAK 
drop table  DW_OW_LOADING_LOG_200911_BAK 
drop table  DW_OW_LOADING_LOG_200912_BAK 
drop table  DW_OW_LOADING_LOG_201001_BAK 

select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_OW_LOADING_LOG_stats where name not like '%BAK') a
join 
(select  *  from t_DW_OW_LOADING_LOG_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


update  APP.SCH_CONTROL_RUNLOG set flag =0 where control_code = 'TR1_L_02031'


alter table t_DW_OW_LOADING_LOG_stats activate not logged initially with empty table


rename DW_OW_LOADING_LOG_200801 to DW_OW_LOADING_LOG_200801_bak
rename DW_OW_LOADING_LOG_200802 to DW_OW_LOADING_LOG_200802_bak
rename DW_OW_LOADING_LOG_200805 to DW_OW_LOADING_LOG_200805_bak
rename DW_OW_LOADING_LOG_200806 to DW_OW_LOADING_LOG_200806_bak
rename DW_OW_LOADING_LOG_200807 to DW_OW_LOADING_LOG_200807_bak
rename DW_OW_LOADING_LOG_200808 to DW_OW_LOADING_LOG_200808_bak
rename DW_OW_LOADING_LOG_200809 to DW_OW_LOADING_LOG_200809_bak
rename DW_OW_LOADING_LOG_200810 to DW_OW_LOADING_LOG_200810_bak
rename DW_OW_LOADING_LOG_200811 to DW_OW_LOADING_LOG_200811_bak
rename DW_OW_LOADING_LOG_200812 to DW_OW_LOADING_LOG_200812_bak
rename DW_OW_LOADING_LOG_200901 to DW_OW_LOADING_LOG_200901_bak
rename DW_OW_LOADING_LOG_200903 to DW_OW_LOADING_LOG_200903_bak
rename DW_OW_LOADING_LOG_200904 to DW_OW_LOADING_LOG_200904_bak
rename DW_OW_LOADING_LOG_200905 to DW_OW_LOADING_LOG_200905_bak
rename DW_OW_LOADING_LOG_200906 to DW_OW_LOADING_LOG_200906_bak
rename DW_OW_LOADING_LOG_200907 to DW_OW_LOADING_LOG_200907_bak
rename DW_OW_LOADING_LOG_200908 to DW_OW_LOADING_LOG_200908_bak
rename DW_OW_LOADING_LOG_200909 to DW_OW_LOADING_LOG_200909_bak
rename DW_OW_LOADING_LOG_200910 to DW_OW_LOADING_LOG_200910_bak
rename DW_OW_LOADING_LOG_200911 to DW_OW_LOADING_LOG_200911_bak
rename DW_OW_LOADING_LOG_200912 to DW_OW_LOADING_LOG_200912_bak
rename DW_OW_LOADING_LOG_201001 to DW_OW_LOADING_LOG_201001_bak


create table DW_OW_LOADING_LOG_200801 like DW_OW_LOADING_LOG_200801_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200802 like DW_OW_LOADING_LOG_200802_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200805 like DW_OW_LOADING_LOG_200805_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200806 like DW_OW_LOADING_LOG_200806_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200807 like DW_OW_LOADING_LOG_200807_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200808 like DW_OW_LOADING_LOG_200808_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200809 like DW_OW_LOADING_LOG_200809_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200810 like DW_OW_LOADING_LOG_200810_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200811 like DW_OW_LOADING_LOG_200811_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200812 like DW_OW_LOADING_LOG_200812_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200901 like DW_OW_LOADING_LOG_200901_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200903 like DW_OW_LOADING_LOG_200903_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200904 like DW_OW_LOADING_LOG_200904_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200905 like DW_OW_LOADING_LOG_200905_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200906 like DW_OW_LOADING_LOG_200906_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200907 like DW_OW_LOADING_LOG_200907_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200908 like DW_OW_LOADING_LOG_200908_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200909 like DW_OW_LOADING_LOG_200909_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200910 like DW_OW_LOADING_LOG_200910_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200911 like DW_OW_LOADING_LOG_200911_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_200912 like DW_OW_LOADING_LOG_200912_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_OW_LOADING_LOG_201001 like DW_OW_LOADING_LOG_201001_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 

insert into DW_OW_LOADING_LOG_200801 select * from  DW_OW_LOADING_LOG_200801_BAK 
insert into DW_OW_LOADING_LOG_200802 select * from  DW_OW_LOADING_LOG_200802_BAK 
insert into DW_OW_LOADING_LOG_200805 select * from  DW_OW_LOADING_LOG_200805_BAK 
insert into DW_OW_LOADING_LOG_200806 select * from  DW_OW_LOADING_LOG_200806_BAK 
insert into DW_OW_LOADING_LOG_200807 select * from  DW_OW_LOADING_LOG_200807_BAK 
insert into DW_OW_LOADING_LOG_200808 select * from  DW_OW_LOADING_LOG_200808_BAK 
insert into DW_OW_LOADING_LOG_200809 select * from  DW_OW_LOADING_LOG_200809_BAK 
insert into DW_OW_LOADING_LOG_200810 select * from  DW_OW_LOADING_LOG_200810_BAK 
insert into DW_OW_LOADING_LOG_200811 select * from  DW_OW_LOADING_LOG_200811_BAK 
insert into DW_OW_LOADING_LOG_200812 select * from  DW_OW_LOADING_LOG_200812_BAK 
insert into DW_OW_LOADING_LOG_200901 select * from  DW_OW_LOADING_LOG_200901_BAK 
insert into DW_OW_LOADING_LOG_200903 select * from  DW_OW_LOADING_LOG_200903_BAK 
insert into DW_OW_LOADING_LOG_200904 select * from  DW_OW_LOADING_LOG_200904_BAK 
insert into DW_OW_LOADING_LOG_200905 select * from  DW_OW_LOADING_LOG_200905_BAK 
insert into DW_OW_LOADING_LOG_200906 select * from  DW_OW_LOADING_LOG_200906_BAK 
insert into DW_OW_LOADING_LOG_200907 select * from  DW_OW_LOADING_LOG_200907_BAK 
insert into DW_OW_LOADING_LOG_200908 select * from  DW_OW_LOADING_LOG_200908_BAK 
insert into DW_OW_LOADING_LOG_200909 select * from  DW_OW_LOADING_LOG_200909_BAK 
insert into DW_OW_LOADING_LOG_200910 select * from  DW_OW_LOADING_LOG_200910_BAK 
insert into DW_OW_LOADING_LOG_200911 select * from  DW_OW_LOADING_LOG_200911_BAK 
insert into DW_OW_LOADING_LOG_200912 select * from  DW_OW_LOADING_LOG_200912_BAK 
insert into DW_OW_LOADING_LOG_201001 select * from  DW_OW_LOADING_LOG_201001_BAK 


insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200812', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200812 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200901', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200901 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200903', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200903 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200904', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200904 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200905', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200905 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200906', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200906 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200907', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200907 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200908', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200908 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200909', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200909 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200910', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200910 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_200911', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200911 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201002', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201002 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201003', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201003 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201004', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201004 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201005', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201005 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201006', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201006 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201007', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201007 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201008', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201008 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201009', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201009 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201010', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201010 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201011', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201011 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_WCC_USER_ACTION_201012', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201012 group by nodenumber(ID) 


rename DW_OW_LOADING_LOG_200801 to DW_OW_LOADING_LOG_200801_bak
rename DW_OW_LOADING_LOG_200802 to DW_OW_LOADING_LOG_200802_bak
rename DW_OW_LOADING_LOG_200805 to DW_OW_LOADING_LOG_200805_bak
rename DW_OW_LOADING_LOG_200806 to DW_OW_LOADING_LOG_200806_bak
rename DW_OW_LOADING_LOG_200807 to DW_OW_LOADING_LOG_200807_bak
rename DW_OW_LOADING_LOG_200808 to DW_OW_LOADING_LOG_200808_bak
rename DW_OW_LOADING_LOG_200809 to DW_OW_LOADING_LOG_200809_bak
rename DW_OW_LOADING_LOG_200810 to DW_OW_LOADING_LOG_200810_bak
rename DW_OW_LOADING_LOG_200811 to DW_OW_LOADING_LOG_200811_bak
rename DW_OW_LOADING_LOG_200812 to DW_OW_LOADING_LOG_200812_bak
rename DW_OW_LOADING_LOG_200901 to DW_OW_LOADING_LOG_200901_bak
rename DW_OW_LOADING_LOG_200903 to DW_OW_LOADING_LOG_200903_bak
rename DW_OW_LOADING_LOG_200904 to DW_OW_LOADING_LOG_200904_bak
rename DW_OW_LOADING_LOG_200905 to DW_OW_LOADING_LOG_200905_bak
rename DW_OW_LOADING_LOG_200906 to DW_OW_LOADING_LOG_200906_bak
rename DW_OW_LOADING_LOG_200907 to DW_OW_LOADING_LOG_200907_bak
rename DW_OW_LOADING_LOG_200908 to DW_OW_LOADING_LOG_200908_bak
rename DW_OW_LOADING_LOG_200909 to DW_OW_LOADING_LOG_200909_bak
rename DW_OW_LOADING_LOG_200910 to DW_OW_LOADING_LOG_200910_bak
rename DW_OW_LOADING_LOG_200911 to DW_OW_LOADING_LOG_200911_bak
rename DW_OW_LOADING_LOG_200912 to DW_OW_LOADING_LOG_200912_bak
rename DW_OW_LOADING_LOG_201001 to DW_OW_LOADING_LOG_201001_bak

select count(0),count(distinct id) from Dw_ow_loading_log_20110124
select count(0) from Dwd_ow_loading_log_20110124
select * from BASS2.DW_CHL_E_TOUCH_USER_DM_201012
select * from syscat.tables where tabname like '%201012%'

select * from ODS_SP_BUSN_ACTION_201011

select * from app.sch_control_before where before_control_code =  'BASS2_Dwd_kf_send_sms_data_ds.tcl'
BASS2_Dwd_ow_loading_log_ds.tcl
BASS2_Dw_ow_loading_log_ds.tcl


select * from syscat.tables where tabname like '%OW_LOADING_LOG%'
order by 2


drop table BASS2.ODS_PLAN_PROM_YYYYMMDD
CREATE TABLE "BASS2   "."ODS_PLAN_PROM_YYYYMMDD"  (
                  "PLAN_ID" BIGINT , 
                  "PROM_ID" DECIMAL(8,0) , 
                  "PRIORITY" SMALLINT , 
                  "EXPR_ID" DECIMAL(8,0) , 
                  "IS_GLOBAL" SMALLINT , 
                  "VALID_DATE" TIMESTAMP , 
                  "EXPIRE_DATE" TIMESTAMP , 
                  "SUBSCR_MODE" INTEGER , 
                  "EXPIRE_MODE" INTEGER , 
                  "MIN_NUM" DECIMAL(8,0) , 
                  "MAX_NUM" DECIMAL(8,0) , 
                  "DESCRIPTION" VARCHAR(128) , 
                  "VALID_CYCLE" INTEGER , 
                  "MANDATORY" SMALLINT , 
                  "CONTROL_DATE" TIMESTAMP )   
                 DISTRIBUTE BY HASH(PLAN_ID,PROM_ID)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX"  


update USYS_TABLE_MAINTAIN set PARTITION_KEY = 'PLAN_ID,PROM_ID' where table_id = 10103 
and table_name = 'ODS_PLAN_PROM'

select * from USYS_TABLE_MAINTAIN where table_name like '%KF_SEND_SMS_DATA%'

select * from app.sch_control_before where before_control_code LIKE 'BASS2_Dwd_plan_prom_ds.tcl'


select * from Dwd_plan_prom_20110124


 select 'dw_ow_loading_log_yyyymmdd', nodenumber(id) ,count(*) as using_num 
 from bass2.dw_ow_loading_log_201012 group by nodenumber(id) 

create table DWD_PLAN_PROM_20110124_bak
like DWD_PLAN_PROM_20110124 DISTRIBUTE BY HASH(PLAN_ID,prom_id)   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
insert into DWD_PLAN_PROM_20110124_bak select * from DWD_PLAN_PROM_20110124



select * from DWD_PLAN_PROM_20110124
select * from app.sch_control_task where upper(cmd_line) like '%KF_SEND_SMS_DATA%'
select count(0),count(distinct char(PROM_ID)||char(PLAN_ID) ) from  DWD_PLAN_PROM_20110124
select * from 
select * from etl_load_table_map where  
table_name_templet like 'ODS_OW_LOADING_LOG_%'
select * from USYS_TABLE_MAINTAIN where lower(table_name) like '%ods_plan_prom%'
update USYS_TABLE_MAINTAIN set PARTITION_KEY = 'ID' where table_id = 10149 
and table_name = 'ODS_OW_LOADING_LOG'




rename DW_WCC_USER_ACTION_200812 to DW_WCC_USER_ACTION_200812_bak
rename DW_WCC_USER_ACTION_200901 to DW_WCC_USER_ACTION_200901_bak
rename DW_WCC_USER_ACTION_200903 to DW_WCC_USER_ACTION_200903_bak
rename DW_WCC_USER_ACTION_200904 to DW_WCC_USER_ACTION_200904_bak
rename DW_WCC_USER_ACTION_200905 to DW_WCC_USER_ACTION_200905_bak
rename DW_WCC_USER_ACTION_200906 to DW_WCC_USER_ACTION_200906_bak
rename DW_WCC_USER_ACTION_200907 to DW_WCC_USER_ACTION_200907_bak
rename DW_WCC_USER_ACTION_200908 to DW_WCC_USER_ACTION_200908_bak
rename DW_WCC_USER_ACTION_200909 to DW_WCC_USER_ACTION_200909_bak
rename DW_WCC_USER_ACTION_200910 to DW_WCC_USER_ACTION_200910_bak
rename DW_WCC_USER_ACTION_200911 to DW_WCC_USER_ACTION_200911_bak
rename DW_WCC_USER_ACTION_201002 to DW_WCC_USER_ACTION_201002_bak
rename DW_WCC_USER_ACTION_201003 to DW_WCC_USER_ACTION_201003_bak
rename DW_WCC_USER_ACTION_201004 to DW_WCC_USER_ACTION_201004_bak
rename DW_WCC_USER_ACTION_201005 to DW_WCC_USER_ACTION_201005_bak
rename DW_WCC_USER_ACTION_201006 to DW_WCC_USER_ACTION_201006_bak
rename DW_WCC_USER_ACTION_201007 to DW_WCC_USER_ACTION_201007_bak
rename DW_WCC_USER_ACTION_201008 to DW_WCC_USER_ACTION_201008_bak
rename DW_WCC_USER_ACTION_201009 to DW_WCC_USER_ACTION_201009_bak
rename DW_WCC_USER_ACTION_201010 to DW_WCC_USER_ACTION_201010_bak
rename DW_WCC_USER_ACTION_201011 to DW_WCC_USER_ACTION_201011_bak
rename DW_WCC_USER_ACTION_201012 to DW_WCC_USER_ACTION_201012_bak



create table DW_WCC_USER_ACTION_200812 like DW_WCC_USER_ACTION_200812_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200901 like DW_WCC_USER_ACTION_200901_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200903 like DW_WCC_USER_ACTION_200903_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200904 like DW_WCC_USER_ACTION_200904_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200905 like DW_WCC_USER_ACTION_200905_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200906 like DW_WCC_USER_ACTION_200906_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200907 like DW_WCC_USER_ACTION_200907_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200908 like DW_WCC_USER_ACTION_200908_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200909 like DW_WCC_USER_ACTION_200909_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200910 like DW_WCC_USER_ACTION_200910_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_200911 like DW_WCC_USER_ACTION_200911_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201002 like DW_WCC_USER_ACTION_201002_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201003 like DW_WCC_USER_ACTION_201003_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201004 like DW_WCC_USER_ACTION_201004_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201005 like DW_WCC_USER_ACTION_201005_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201006 like DW_WCC_USER_ACTION_201006_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201007 like DW_WCC_USER_ACTION_201007_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201008 like DW_WCC_USER_ACTION_201008_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201009 like DW_WCC_USER_ACTION_201009_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201010 like DW_WCC_USER_ACTION_201010_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201011 like DW_WCC_USER_ACTION_201011_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 
create table DW_WCC_USER_ACTION_201012 like DW_WCC_USER_ACTION_201012_BAK DISTRIBUTE BY HASH(ID) IN TBS_ODS_OTHER INDEX IN TBS_INDEX NOT LOGGED INITIALLY 



insert into DW_WCC_USER_ACTION_200812 select * from  DW_WCC_USER_ACTION_200812_BAK 
insert into DW_WCC_USER_ACTION_200901 select * from  DW_WCC_USER_ACTION_200901_BAK 
insert into DW_WCC_USER_ACTION_200903 select * from  DW_WCC_USER_ACTION_200903_BAK 
insert into DW_WCC_USER_ACTION_200904 select * from  DW_WCC_USER_ACTION_200904_BAK 
insert into DW_WCC_USER_ACTION_200905 select * from  DW_WCC_USER_ACTION_200905_BAK 
insert into DW_WCC_USER_ACTION_200906 select * from  DW_WCC_USER_ACTION_200906_BAK 
insert into DW_WCC_USER_ACTION_200907 select * from  DW_WCC_USER_ACTION_200907_BAK 
insert into DW_WCC_USER_ACTION_200908 select * from  DW_WCC_USER_ACTION_200908_BAK 
insert into DW_WCC_USER_ACTION_200909 select * from  DW_WCC_USER_ACTION_200909_BAK 
insert into DW_WCC_USER_ACTION_200910 select * from  DW_WCC_USER_ACTION_200910_BAK 
insert into DW_WCC_USER_ACTION_200911 select * from  DW_WCC_USER_ACTION_200911_BAK 
insert into DW_WCC_USER_ACTION_201002 select * from  DW_WCC_USER_ACTION_201002_BAK 
insert into DW_WCC_USER_ACTION_201003 select * from  DW_WCC_USER_ACTION_201003_BAK 
insert into DW_WCC_USER_ACTION_201004 select * from  DW_WCC_USER_ACTION_201004_BAK 
insert into DW_WCC_USER_ACTION_201005 select * from  DW_WCC_USER_ACTION_201005_BAK 
insert into DW_WCC_USER_ACTION_201006 select * from  DW_WCC_USER_ACTION_201006_BAK 
insert into DW_WCC_USER_ACTION_201007 select * from  DW_WCC_USER_ACTION_201007_BAK 
insert into DW_WCC_USER_ACTION_201008 select * from  DW_WCC_USER_ACTION_201008_BAK 
insert into DW_WCC_USER_ACTION_201009 select * from  DW_WCC_USER_ACTION_201009_BAK 
insert into DW_WCC_USER_ACTION_201010 select * from  DW_WCC_USER_ACTION_201010_BAK 
insert into DW_WCC_USER_ACTION_201011 select * from  DW_WCC_USER_ACTION_201011_BAK 
insert into DW_WCC_USER_ACTION_201012 select * from  DW_WCC_USER_ACTION_201012_BAK 



insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200812', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200812 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200901', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200901 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200903', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200903 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200904', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200904 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200905', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200905 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200906', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200906 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200907', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200907 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200908', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200908 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200909', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200909 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200910', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200910 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200911', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200911 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201002', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201002 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201003', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201003 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201004', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201004 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201005', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201005 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201006', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201006 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201007', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201007 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201008', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201008 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201009', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201009 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201010', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201010 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201011', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201011 group by nodenumber(ID) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201012', nodenumber(ID) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201012 group by nodenumber(ID) 


insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200812_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200812_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200901_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200901_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200903_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200903_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200904_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200904_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200905_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200905_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200906_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200906_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200907_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200907_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200908_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200908_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200909_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200909_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200910_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200910_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_200911_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_200911_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201002_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201002_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201003_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201003_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201004_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201004_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201005_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201005_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201006_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201006_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201007_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201007_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201008_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201008_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201009_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201009_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201010_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201010_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201011_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201011_BAK group by nodenumber(LOGIN_PRODUCT_NO) 
insert into t_DW_WCC_USER_ACTION_stats select 'DW_WCC_USER_ACTION_201012_BAK', nodenumber(LOGIN_PRODUCT_NO) ,count(*) as using_num from bass2.DW_WCC_USER_ACTION_201012_BAK group by nodenumber(LOGIN_PRODUCT_NO) 

select * from t_DW_WCC_USER_ACTION_stats


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_WCC_USER_ACTION_stats where name  like '%BAK') a
left join 
 (select  *  from t_DW_WCC_USER_ACTION_stats where name not like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1



select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_WCC_USER_ACTION_stats where name not like '%BAK') a
join 
(select  *  from t_DW_WCC_USER_ACTION_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


select * from DW_WCC_USER_ACTION_201005


drop table  DW_WCC_USER_ACTION_200812_BAK 
drop table  DW_WCC_USER_ACTION_200901_BAK 
drop table  DW_WCC_USER_ACTION_200903_BAK 
drop table  DW_WCC_USER_ACTION_200904_BAK 
drop table  DW_WCC_USER_ACTION_200905_BAK 
drop table  DW_WCC_USER_ACTION_200906_BAK 
drop table  DW_WCC_USER_ACTION_200907_BAK 
drop table  DW_WCC_USER_ACTION_200908_BAK 
drop table  DW_WCC_USER_ACTION_200909_BAK 
drop table  DW_WCC_USER_ACTION_200910_BAK 
drop table  DW_WCC_USER_ACTION_200911_BAK 
drop table  DW_WCC_USER_ACTION_201002_BAK 
drop table  DW_WCC_USER_ACTION_201003_BAK 
drop table  DW_WCC_USER_ACTION_201004_BAK 
drop table  DW_WCC_USER_ACTION_201005_BAK 
drop table  DW_WCC_USER_ACTION_201006_BAK 
drop table  DW_WCC_USER_ACTION_201007_BAK 
drop table  DW_WCC_USER_ACTION_201008_BAK 
drop table  DW_WCC_USER_ACTION_201009_BAK 
drop table  DW_WCC_USER_ACTION_201010_BAK 
drop table  DW_WCC_USER_ACTION_201011_BAK 
drop table  DW_WCC_USER_ACTION_201012_BAK 



create table t_DW_WCC_USER_ACTION_stats(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing


drop table DWD_WCC_USER_ACTION_20110123_bak

create table DWD_WCC_USER_ACTION_20110123_bak like DWD_WCC_USER_ACTION_20110123   DISTRIBUTE BY HASH("ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
insert into DWD_WCC_USER_ACTION_20110123_bak select * from DWD_WCC_USER_ACTION_20110123

select 'DW_WCC_USER_ACTION_201010', nodenumber (LOGIN_PRODUCT_NO) ,count(*) as using_num 
from BASS2.DW_WCC_USER_ACTION_201010 group by  nodenumber (ID )
order by using_num 

drop ba

ODS_OW_LOADING_LOG_20110123

select ID,LOGIN_PRODUCT_NO from DWD_WCC_USER_ACTION_20110123
select * from ODS_OW_LOADING_LOG_20110123

dw_wcc_user_action_
select * from syscat.tables where tabname like 'DWD_PLAN_PROM%'
order by 2

select  from ODS_OW_LOADING_LOG_20110118 
select count(0),count(distinct id ) from ODS_OW_LOADING_LOG_20110118  
select * from DWD_NG2_P99934
select * from DWD_NG2_P99999

select * from DWD_NG2_M99914_201011



select * from app.sch_control_before where before_control_code = 'BASS2_Dwd_kf_send_sms_data_ds.tcl'



DWD_NG2_M99914_201011
DWD_NG2_M99916_201011
DWD_NG2_P99924
DWD_NG2_P99934
selec

select tabname from syscat.tables where tabname like 'DWD_NG2%'
AND tabname NOT LIKE '%YYYYMM%'
select * from DWD_NG2_I05002_20101101

--drop table DWD_NG2_I05002_20101101
 CREATE TABLE DWD_NG2_I05002_YYYYMMDD
 (
DRTYPE_ID                 INTEGER       ,--记录类型               
PDP_ID                    VARCHAR(4)    ,--PDP上下文的标志        PDP_TYPE
PRODUCT_NO                VARCHAR(15)   ,--手机号码               
IMSI                      VARCHAR(15)   ,--IMSI号码               
SGSN_ADDRESS              VARCHAR(128)  ,--SGSN地址               
ADDUP_RES                 INTEGER       ,--网络能力               
CELL_ID                   VARCHAR(20)   ,--小区标识               
RAC                       VARCHAR(5)    ,--路由区域               
LAC_ID                    VARCHAR(8)    ,--基站标识               
CHARGING_ID               VARCHAR(10)   ,--计费标识               
GGSN_ID                   VARCHAR(20)   ,--GGSN地址               
APN_NI                    VARCHAR(64)   ,--网络标识               
APN_OI                    VARCHAR(64)   ,--运营商标识             
PDP_TYPE                  VARCHAR(4)    ,--PDP类型                
PDP_ADDRESS               VARCHAR(32)   ,--PDP地址                
SGSN_CHANGE               SMALLINT      ,--SGSN改变标志           
CAUSE_CLOSE               SMALLINT      ,--终止原因               
COMBINE_RESULT            CHARACTER(1)  ,--合并结果标志           
CITY_ID                   VARCHAR(7)    ,--归属地                 
ROAM_CITY_ID              VARCHAR(7)    ,--漫游地                 
USER_PROPERTY             INTEGER       ,--计费用户类型           
BILL_FLAG                 SMALLINT      ,--费用类型               
ROAMTYPE_ID               SMALLINT      ,--漫游类型               
SERVICE_TYPE              VARCHAR(8)    ,--计费业务类型           
START_TIME                TIMESTAMP     ,--通话时间               
DURATION                  INTEGER       ,--通话时长               
TARIFF1                   CHARACTER(1)  ,--第1种费率级别          
DATA_FLOW_UP1             INTEGER       ,--级别1的上行的数据流量，
DATA_FLOW_DOWN1           INTEGER       ,--级别1的下行的数据流量，
DURATION1                 INTEGER       ,--级别1的通话时长        
--TARIFF2                   CHARACTER(1)  ,--第2种费率级别          
--DATA_FLOW_UP2             INTEGER       ,--级别2的上行的数据流量，
--DATA_FLOW_DOWN2           INTEGER       ,--级别2的下行的数据流量，
--DURATION2                 INTEGER       ,--级别2的通话时长        
--tariff3                   character(1)  ,--第3种费率级别          
--data_flow_up3             integer       ,--级别3的上行的数据流量，
--data_flow_down3           integer       ,--级别3的下行的数据流量，
--duration3                 integer       ,--级别3的通话时长        
--tariff4                   character(1)  ,--第4种费率级别          
--data_flow_up4             integer       ,--级别4的上行的数据流量，
--data_flow_down4           integer       ,--级别4的下行的数据流量，
--duration4                 integer       ,--级别4的通话时长        
--tariff5                   character(1)  ,--第5种费率级别          
--data_flow_up5             integer       ,--级别5的上行的数据流量，
--data_flow_down5           integer       ,--级别5的下行的数据流量，
--duration5                 integer       ,--级别5的通话时长        
--tariff6                   character(1)  ,--第6种费率级别          
--data_flow_up6             integer       ,--级别6的上行的数据流量，
--data_flow_down6           integer       ,--级别6的下行的数据流量，
--duration6                 integer       ,--级别6的通话时长        
BASE_FEE                  INTEGER       ,--基本通信费             CHARGE1
CHARGE1                   INTEGER       ,--费用1                  
CHARGE2                   INTEGER       ,--费用2                  
CHARGE3                   INTEGER       ,--费用3                  
total_charge              integer       ,--总费用                 
CHARGE1_DISC              INTEGER       ,--基本通信费优惠n        
CHARGE4_DISC              INTEGER       ,--其他费用优惠n          
data_flow_disc            integer       ,--优惠流量n              
--disc_type                 varchar(5)    ,--优惠类型n              
MNS_TYPE                  INTEGER        --承载网络类型           
 )
  DATA CAPTURE NONE
 IN TBS_CDR_VOICE
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (PRODUCT_NO
   ) USING HASHING



select * from DWD_NG2_I03013_20101101


create table DWD_NG2_A01001_20101101 like DWD_NG2_A01001_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( CUST_ID ) using hashing not logged initially
create table DWD_NG2_I01002_20101101 like DWD_NG2_I01002_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( GROUP_CUST_ID ) using hashing not logged initially
create table DWD_NG2_I01003_20101101 like DWD_NG2_I01003_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( GROUP_CUST_ID ) using hashing not logged initially
create table DWD_NG2_I01035_20101101 like DWD_NG2_I01035_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( SERVICE_INSTANCE_ID ) using hashing not logged initially
create table DWD_NG2_A02024_20101101 like DWD_NG2_A02024_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( ID ) using hashing not logged initially
create table DWD_NG2_A02025_20101101 like DWD_NG2_A02025_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( KEY_NUM,BEGIN_TIME ) using hashing not logged initially
create table DWD_NG2_A02026_20101101 like DWD_NG2_A02026_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( PHONE_ID,START_TIME ) using hashing not logged initially
create table DWD_NG2_A02027_20101101 like DWD_NG2_A02027_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( PHONE_ID,CREATE_DATE ) using hashing not logged initially
create table DWD_NG2_M03023_201011 like DWD_NG2_M03023_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( CHANNEL_ID ) using hashing not logged initially
create table DWD_NG2_I03028_20101101 like DWD_NG2_I03028_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( CHANNEL_ID ) using hashing not logged initially
create table DWD_NG2_M03021_201011 like DWD_NG2_M03021_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( DEALER_ID ) using hashing not logged initially
create table DWD_NG2_I03013_20101101 like DWD_NG2_I03013_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( CHANNEL_ID ) using hashing not logged initially
create table DWD_NG2_I03027_20101101 like DWD_NG2_I03027_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( OPER_ID ) using hashing not logged initially
create table DWD_NG2_A05042_20101101 like DWD_NG2_A05042_YYYYMMDD in TBS_CDR_DATA index in TBS_INDEX partitioning key ( USER_NUMBER ) using hashing not logged initially
create table DWD_NG2_I05002_20101101 like DWD_NG2_I05002_YYYYMMDD in TBS_CDR_VOICE index in TBS_INDEX partitioning key ( PRODUCT_NO ) using hashing not logged initially
create table DWD_NG2_I05037_20101101 like DWD_NG2_I05037_YYYYMMDD in TBS_CDR_DATA index in TBS_INDEX partitioning key ( PRODUCT_NO ) using hashing not logged initially
create table DWD_NG2_I06020_20101101 like DWD_NG2_I06020_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( SEQUENCE_NO ) using hashing not logged initially
create table DWD_NG2_I06024_20101101 like DWD_NG2_I06024_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( SEQUENCE_NO ) using hashing not logged initially
create table DWD_NG2_M06025_201011 like DWD_NG2_M06025_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SETTLE_INFO_ID,REGION_ID ) using hashing not logged initially
create table DWD_NG2_M06026_201011 like DWD_NG2_M06026_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SEQUENCE_NO ) using hashing not logged initially

select count(0) from DWD_NG2_A01001_YYYYMMDD 
select count(0) from DWD_NG2_I01002_YYYYMMDD 
select count(0) from DWD_NG2_I01003_YYYYMMDD 
select count(0) from DWD_NG2_I01035_YYYYMMDD 
select count(0) from DWD_NG2_A02024_YYYYMMDD 
select count(0) from DWD_NG2_A02025_YYYYMMDD 
select count(0) from DWD_NG2_A02026_YYYYMMDD 
select count(0) from DWD_NG2_A02027_YYYYMMDD 
select count(0) from DWD_NG2_M03023_YYYYMM 
select count(0) from DWD_NG2_I03028_YYYYMMDD 
select count(0) from DWD_NG2_M03021_YYYYMM 
select count(0) from DWD_NG2_I03013_YYYYMMDD 
select count(0) from DWD_NG2_I03027_YYYYMMDD 
select count(0) from DWD_NG2_A05042_YYYYMMDD 
select count(0) from DWD_NG2_I05002_YYYYMMDD 
select count(0) from DWD_NG2_I05037_YYYYMMDD 
select count(0) from DWD_NG2_I06020_YYYYMMDD 
select count(0) from DWD_NG2_I06024_YYYYMMDD 
select count(0) from DWD_NG2_M06025_YYYYMM 
select count(0) from DWD_NG2_M06026_YYYYMM 

create table DWD_NG2_I08059_20101101 like DWD_NG2_I08059_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( PRODUCT_ITEM_ID ) using hashing not logged initially
create table DWD_NG2_I08115_20101101 like DWD_NG2_I08115_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( RELA_ID ) using hashing not logged initially
create table DWD_NG2_I08117_20101101 like DWD_NG2_I08117_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( RELA_ID ) using hashing not logged initially
create table DWD_NG2_I08103_20101101 like DWD_NG2_I08103_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( DRTYPE_ID ) using hashing not logged initially
create table DWD_NG2_I08104_20101101 like DWD_NG2_I08104_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( CARRY_TYPE ) using hashing not logged initially
create table DWD_NG2_I08105_20101101 like DWD_NG2_I08105_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( CHARGE_TYPE ) using hashing not logged initially
create table DWD_NG2_I08106_20101101 like DWD_NG2_I08106_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( AGRMNT_TYPE ) using hashing not logged initially
create table DWD_NG2_I08108_20101101 like DWD_NG2_I08108_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( OPT_CODE ) using hashing not logged initially
create table DWD_NG2_I08109_20101101 like DWD_NG2_I08109_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( BUSI_TYPE ) using hashing not logged initially
create table DWD_NG2_I08110_20101101 like DWD_NG2_I08110_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( OPT_CODE ) using hashing not logged initially
create table DWD_NG2_I08111_20101101 like DWD_NG2_I08111_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( OPT_TYP_CODE ) using hashing not logged initially
create table DWD_NG2_I08112_20101101 like DWD_NG2_I08112_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( BUSI_CODE ) using hashing not logged initially

CREATE TABLE DWD_NG2_I08103_YYYYMMDD
(
DRTYPE_ID INTEGER,--话单类型代码
DRTYPE_NAME VARCHAR(20)--话单类型名称
)
IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (DRTYPE_ID
   ) USING HASHING

CREATE TABLE DWD_NG2_I08104_YYYYMMDD
(
CARRY_TYPE INTEGER,--承载类型编码
CARRY_TYPE_NAME VARCHAR(20)--承载类型名称
)
IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CARRY_TYPE
   ) USING HASHING

CREATE TABLE DWD_NG2_I08105_YYYYMMDD
(
CHARGE_TYPE INTEGER,--WAP信息计费类别编码
CHARGE_TYPE_NAME VARCHAR(20)--WAP信息计费类别名称
)
IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CHARGE_TYPE
   ) USING HASHING

CREATE TABLE DWD_NG2_I08106_YYYYMMDD
(
AGRMNT_TYPE INTEGER, --承载协议类型编码
AGRMNT_TYPE_NAME VARCHAR(20) --承载协议类型名称
)
IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (AGRMNT_TYPE
   ) USING HASHING



CREATE TABLE DWD_NG2_I08103_YYYYMMDD
(
DRTYPE_ID INTEGER,--话单类型代码
DRTYPE_NAME VARCHAR(20)--话单类型名称
)
IN TBS_DIM
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (DRTYPE_ID
   ) USING HASHING

CREATE TABLE DWD_NG2_I08104_YYYYMMDD
(
CARRY_TYPE INTEGER,--承载类型编码
CARRY_TYPE_NAME VARCHAR(20)--承载类型名称
)
IN TBS_DIM
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CARRY_TYPE
   ) USING HASHING

CREATE TABLE DWD_NG2_I08105_YYYYMMDD
(
CHARGE_TYPE INTEGER,--WAP信息计费类别编码
CHARGE_TYPE_NAME VARCHAR(20)--WAP信息计费类别名称
)
IN TBS_DIM
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CHARGE_TYPE
   ) USING HASHING

CREATE TABLE DWD_NG2_I08106_YYYYMMDD
(
AGRMNT_TYPE INTEGER, --承载协议类型编码
AGRMNT_TYPE_NAME VARCHAR(20) --承载协议类型名称
)
IN TBS_DIM
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (AGRMNT_TYPE
   ) USING HASHING




drop table DWD_NG2_I08103_YYYYMMDD
drop table DWD_NG2_I08104_YYYYMMDD
drop table DWD_NG2_I08105_YYYYMMDD
drop table DWD_NG2_I08106_YYYYMMDD


create table DWD_NG2_M03021_201011 like DWD_NG2_M03021_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( DEALER_ID ) using hashing not logged initially

 
--DROP TABLE DWD_NG2_M03021_YYYYMM					
CREATE TABLE DWD_NG2_M03021_yyyymm (					
        DEALER_ID           BIGINT              --渠道负责人标识
        ,DEALER_NAME        VARCHAR(60)         --渠道负责人姓名
        ,gender             SMALLINT            --负责人性别
        ,BIRTHDAY           TIMESTAMP           --负责人出生日期
        ,CERT_TYPE          SMALLINT            --负责人证件类型
        ,CERT_CODE          VARCHAR(40)         --负责人证件号码
        ,CONTACT_PHONE      VARCHAR(40)         --负责人联系电话
        ,CONTACT_ADDRESS    VARCHAR(120)        --负责人住址
        ,EMAIL              VARCHAR(120)        --负责人邮件
        ,CREATE_DATE        TIMESTAMP           --建档日期
        ,CREATE_OP_ID       BIGINT              --建档员工
        ,DONE_DATE          TIMESTAMP           --操作日期
        ,OP_ID              BIGINT              --操作员工
        ,STATE              SMALLINT                --状态
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( dealer_id )
 USING HASHING
        
 


select * from DWD_NG2_I03021_YYYYMM

select * from DWD_MR_RING_CASE_MEMB_INFO_20101101

select * from Dim_channel_info

select * from app.sch_control_before where control_code = 'BASS2_Dim_channel_info_ds.tcl'
select * from app.sch_control_task where control_code = '03021'
select * 
ODS_CHANNEL_INFO_YYYYMMDD


select * from BASS2.ODS_CHANNEL_INFO_20101201

select * from VGOP_16202_2030
select * from syscat.tables where tabname like 'DWD_NG2_%03021%'
order by 2

DROP TABLE DWD_NG2_M03021_YYYYMMDD


select * from VGOP_16202_2010123008

select * from DWD_NG2_M96002_201011

select * from VGOP_11202_201011

create table DWD_NG2_M96002_201011 like DWD_NG2_M96002_YYYYMM in TBS_CDR_DATA index in TBS_INDEX partitioning key ( ROW_NO ) using hashing not logged initially

--DROP TABLE DWD_NG2_M96002_YYYYMM
 CREATE TABLE DWD_NG2_M96002_YYYYMM (
        ROW_NO                 DECIMAL(12,0)       
        ,OP_TIME                VARCHAR(14)         
        ,PRODUCT_NO             VARCHAR(15)         
        ,SID                    VARCHAR(9)          
        ,CITY_ID                VARCHAR(5)          
        ,FRIEND_NO              VARCHAR(15)         
        ,FRIEND_SID             VARCHAR(9)          
        ,CANCEL_ID              VARCHAR(4)  
 )
 DATA CAPTURE NONE 
 IN TBS_CDR_DATA
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( ROW_NO )
 USING HASHING
 
         
select count(0) from VGOP_11202_201011
         
select * from etl_load_table_map_mtest where   TASK_ID = '%0504%'       

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'M96002','DWD_NG2_M96002_YYYYMM','飞信用户好友明细', 0,null)

select * from VGOP_11202_201012


select * from DWD_NG2_A96141_20101201


create table DWD_NG2_A96141_20101201 like DWD_NG2_A96141_YYYYMMDD in TBS_CDR_DATA index in TBS_INDEX partitioning key ( ROW_NO ) using hashing not logged initially


--DROP TABLE DWD_NG2_A96141_YYYYMMDD
 CREATE TABLE DWD_NG2_A96141_YYYYMMDD (
        ROW_NO                 DECIMAL(12,0)       
        ,OP_TIME                VARCHAR(8)          
        ,PRODUCT_NO             VARCHAR(15)         
        ,SID                    VARCHAR(9)          
        ,CITY_ID                VARCHAR(5)          
        ,FRIEND_NO              VARCHAR(15)         
        ,FRIEND_SID             VARCHAR(9)          
        ,COMMUNITYPE_ID         VARCHAR(6)          
        ,CALLTYPE_ID            VARCHAR(6)          
        ,CNT                    BIGINT   
 )
 DATA CAPTURE NONE 
 IN TBS_CDR_DATA
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( ROW_NO )
 USING HASHING


select * from etl_load_table_map_mtest where task_id = 'A96141'

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'A96141','DWD_NG2_A96141_YYYYMMDD','飞信用户交往明细', 0,null)
 

select * from VGOP_11203_20101201




select * from etl_load_table_map where task_id = 'I98013'
delete from etl_load_table_map where task_id = 'I98013'

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I98013','DWD_NG2_I98013_YYYYMMDD','铃音盒内铃音信息', 0,null)
 
create table DWD_NG2_I98013_20101101 like DWD_NG2_I98013_YYYYMMDD in TBS_CDR_DATA index in TBS_INDEX partitioning key ( RING_CASE_ID ) using hashing not logged initially

drop table DWD_NG2_I98015_YYYYMMDD
CREATE TABLE "BASS2   "."DWD_NG2_I98013_YYYYMMDD"  (
         RING_CASE_ID           VARCHAR(18)         
        ,RING_ID                VARCHAR(30)  
                  )   
                 DISTRIBUTE BY HASH("RING_CASE_ID")   
                   IN "TBS_CDR_DATA" INDEX IN "TBS_INDEX"  




drop table DWD_NG2_I99901_20101101
create table DWD_NG2_I99901_20101101 like DWD_NG2_I99901_YYYYMMDD in TBS_CDR_VOICE index in TBS_INDEX partitioning key ( USER_ID ) using hashing not logged initially


--drop table bass2.DWD_NG2_I99901_YYYYMMDD
create table bass2.DWD_NG2_I99901_YYYYMMDD
(
         USER_ID                VARCHAR(20)         
        ,CUST_ID                VARCHAR(20)         
        ,ACCT_ID                VARCHAR(20)         
        ,PRODUCT_NO             VARCHAR(15)         
        ,OPP_NUMBER             VARCHAR(30)         
        ,OPP_REGULAR_NUMBER     VARCHAR(30)         
        ,PRODUCT_ID             INTEGER             
        ,SERVICE_ID             BIGINT              
        ,DRTYPE_ID              INTEGER             
        ,PROVINCE_ID            VARCHAR(20)         
        ,CITY_ID                VARCHAR(7)          
        ,COUNTY_ID              VARCHAR(20)         
        ,ROAM_PROVINCE_ID       VARCHAR(7)          
        ,ROAM_CITY_ID           VARCHAR(7)          
        ,ROAM_COUNTY_ID         VARCHAR(20)         
        ,OPP_CITY_ID            VARCHAR(7)          
        ,OPP_ROAM_CITY_ID       VARCHAR(7)          
        ,BRAND_ID               BIGINT              
        ,PLAN_ID                BIGINT              
        ,BILL_MARK              SMALLINT            
        ,IP_MARK                SMALLINT            
        ,NETCALL_MARK           SMALLINT            
        ,SPESERVER_MARK         SMALLINT            
        ,CALLTYPE_ID            SMALLINT            
        ,TOLLTYPE_ID            SMALLINT            
        ,TOLLTYPE_ID2           INTEGER             
        ,ROAMTYPE_ID            SMALLINT            
        ,CALLFWTYPE_ID          SMALLINT            
        ,OPP_ACCESS_TYPE_ID     INTEGER             
        ,OPPOSITE_ID            INTEGER             
        ,CALLMOMENT_ID          SMALLINT            
        ,RATE_PROD_ID           VARCHAR(2048)       
        ,MSC_ID                 VARCHAR(10)         
        ,LAC_ID                 VARCHAR(8)          
        ,CELL_ID                VARCHAR(20)         
        ,IMEI                   VARCHAR(18)         
        ,START_TIME             TIMESTAMP           
        ,IMSI                   VARCHAR(15)         
        ,MSRN                   VARCHAR(15)         
        ,A_NUMBER               VARCHAR(24)         
        ,SCP_ID                 VARCHAR(3)          
        ,OUT_TRUNKID            VARCHAR(16)         
        ,IN_TRUNKID             VARCHAR(16)         
        ,STOP_CAUSE             INTEGER             
        ,MOC_ID                 VARCHAR(24)         
        ,MTC_ID                 VARCHAR(24)         
        ,ENTERPRISE_ID          VARCHAR(20)         
        ,SERVICE_TYPE           VARCHAR(4)          
        ,SERVICE_CODE           VARCHAR(20)         
        ,BILL_INDICATE          SMALLINT            
        ,SP_RELA_TYPE           SMALLINT            
        ,RATING_FLAG            SMALLINT            
        ,ITEM_CODE1             INTEGER             
        ,ITEM_CODE2             INTEGER             
        ,ITEM_CODE3             INTEGER             
        ,ITEM_CODE4             INTEGER             
        ,FREE_RES_CODE1         INTEGER             
        ,FREE_RES_CODE2         INTEGER             
        ,FREE_RES_CODE3         INTEGER             
        ,ORIGINAL_FILE          VARCHAR(64)         
        ,USERTYPE_ID            INTEGER             
        ,OPP_PLAN_ID            BIGINT              
        ,OPP_NOACCESS_NUMBER    VARCHAR(30)         
        ,VIDEO_TYPE             INTEGER             
        ,MNS_TYPE               INTEGER             
        ,USER_PROPERTY          INTEGER             
        ,OPP_PROPERTY           INTEGER             
        ,CALL_REFNUM            VARCHAR(64)         
        ,FCI_TYPE               VARCHAR(10)         
        ,INPUT_TIME             TIMESTAMP           
        ,RATING_RES             INTEGER             
        ,FREE_RES_VAL1          INTEGER             
        ,FREE_RES_VAL2          INTEGER             
        ,FREE_RES_VAL3          INTEGER             
        ,STD_UNIT               INTEGER             
        ,TOLL_STD_UNIT          INTEGER             
        ,ORI_BASIC_CHARGE       INTEGER             
        ,ORI_TOLL_CHARGE        INTEGER             
        ,ORI_OTHER_CHARGE       INTEGER             
        ,ADDUP_RES              INTEGER             
        ,CALL_DURATION          INTEGER             
        ,CALL_DURATION_M        INTEGER             
        ,CALL_DURATION_S        INTEGER             
        ,BILL_DURATION          INTEGER             
        ,BASECALL_FEE           DECIMAL(9,2)        
        ,TOLL_FEE               DECIMAL(9,2)        
        ,INFO_FEE               DECIMAL(9,2)        
        ,OTHER_FEE              DECIMAL(9,2)        
        ,CHARGE1                DECIMAL(9,2)        
        ,CHARGE1_DISC           DECIMAL(9,2)        
        ,CHARGE2                DECIMAL(9,2)        
        ,CHARGE2_DISC           DECIMAL(9,2)        
        ,CHARGE3                DECIMAL(9,2)        
        ,CHARGE3_DISC           DECIMAL(9,2)        
        ,CHARGE4                DECIMAL(9,2)        
        ,CHARGE4_DISC           DECIMAL(9,2)         
)
in tbs_cdr_voice
index in tbs_index
partitioning key (user_id) using hashing
not logged initially


create table DWD_NG2_I99901_20101101 like DWD_NG2_I99901_YYYYMMDD in TBS_CDR_VOICE index in TBS_INDEX partitioning key ( USER_ID ) using hashing not logged initially

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I99901','DWD_NG2_I99901_YYYYMMDD','语音清单', 0,null)
 

select 
         USER_ID
        ,CUST_ID
        ,ACCT_ID
        ,PRODUCT_NO
        ,OPP_NUMBER
        ,OPP_REGULAR_NUMBER
        ,PRODUCT_ID
        ,SERVICE_ID
        ,DRTYPE_ID
        ,PROVINCE_ID
        ,CITY_ID
        ,COUNTY_ID
        ,ROAM_PROVINCE_ID
        ,ROAM_CITY_ID
        ,ROAM_COUNTY_ID
        ,OPP_CITY_ID
        ,OPP_ROAM_CITY_ID
        ,BRAND_ID
        ,PLAN_ID
        ,BILL_MARK
        ,IP_MARK
        ,NETCALL_MARK
        ,SPESERVER_MARK
        ,CALLTYPE_ID
        ,TOLLTYPE_ID
        ,TOLLTYPE_ID2
        ,ROAMTYPE_ID
        ,CALLFWTYPE_ID
        ,OPP_ACCESS_TYPE_ID
        ,OPPOSITE_ID
        ,CALLMOMENT_ID
        ,RATE_PROD_ID
        ,MSC_ID
        ,LAC_ID
        ,CELL_ID
        ,IMEI
        ,START_TIME
        ,IMSI
        ,MSRN
        ,A_NUMBER
        ,SCP_ID
        ,OUT_TRUNKID
        ,IN_TRUNKID
        ,STOP_CAUSE
        ,MOC_ID
        ,MTC_ID
        ,ENTERPRISE_ID
        ,SERVICE_TYPE
        ,SERVICE_CODE
        ,BILL_INDICATE
        ,SP_RELA_TYPE
        ,RATING_FLAG
        ,ITEM_CODE1
        ,ITEM_CODE2
        ,ITEM_CODE3
        ,ITEM_CODE4
        ,FREE_RES_CODE1
        ,FREE_RES_CODE2
        ,FREE_RES_CODE3
        ,ORIGINAL_FILE
        ,USERTYPE_ID
        ,OPP_PLAN_ID
        ,OPP_NOACCESS_NUMBER
        ,INPUT_TIME
        ,RATING_RES
        ,FREE_RES_VAL1
        ,FREE_RES_VAL2
        ,FREE_RES_VAL3
        ,STD_UNIT
        ,TOLL_STD_UNIT
        ,ORI_BASIC_CHARGE
        ,ORI_TOLL_CHARGE
        ,ORI_OTHER_CHARGE
        ,ADDUP_RES
        ,CALL_DURATION
        ,CALL_DURATION_M
        ,CALL_DURATION_S
        ,BILL_DURATION
        ,BASECALL_FEE
        ,TOLL_FEE
        ,INFO_FEE
        ,OTHER_FEE
        ,CHARGE1
        ,CHARGE1_DISC
        ,CHARGE2
        ,CHARGE2_DISC
        ,CHARGE3
        ,CHARGE3_DISC
        ,CHARGE4
        ,CHARGE4_DISC
from cdr_call_dtl_20110118
        
--drop table bass2.DWD_NG2_I99901_YYYYMMDD
create table bass2.DWD_NG2_I99901_YYYYMMDD
(
       user_id              varchar(12),	--用户id
       cust_id              varchar(12),	--客户id
       acct_id              varchar(12),	--帐户id
       product_no           varchar(15),	--电话号码
       opp_number      	    varchar(30),	--对端号码
       opp_regular_number   varchar(30),	--对端规整号码
       product_id           integer,		--产品id
       service_id           integer,		--服务id
       drtype_id            integer,		--话单类型
       province_id          varchar(7),   --归属省
       city_id              varchar(7),	--城市代码
       county_id            varchar(7),	--县市代码
       roam_province_id     varchar(7),	--漫游省级代码
       roam_city_id         varchar(7),	--漫游城市代码
       roam_county_id       varchar(7),   --漫游归属县市
       opp_city_id          varchar(7),	--对端城市代码
       opp_roam_city_id     varchar(7),	--对端漫游城市代码
       brand_id             smallint,		--品牌
       plan_id              integer,		--计划
       bill_mark            smallint,		--收费标志
       ip_mark              smallint,		--ip标志
       netcall_mark         smallint,     --V网内通话标志
       speserver_mark       smallint,     --特服标志
       calltype_id          smallint,		--呼叫类型
       tolltype_id          smallint,		--长途类型
       tolltype_id2         integer,      --长话类型2
       roamtype_id          smallint,		--漫游类型
       callfwtype_id        smallint,		--呼转类型
       opp_access_type_id   integer,		--对端接入类型
       opposite_id          integer,		--对端类型
       callmoment_id        smallint,		--呼叫时段
       rate_prod_id         varchar(128),	--计费产品代码
       msc_id               varchar(10),	--交换机代码
       lac_id               varchar(8),	--位置代码
       cell_id              varchar(8),	--小区代码
       imei                 varchar(18),	--IMEI码
       start_time	          timestamp,		--通话起始时间
       imsi		             varchar(15),  --计费用户标识
       msrn                 varchar(15),  --被叫动态漫游号
       a_number             varchar(24),  --呼转的A号码
       scp_id               varchar(3),   --SCP代码
       out_trunkid          varchar(16),  --出路由
       in_trunkid	          varchar(16),  --入路由
       stop_cause           integer,      --终止原因
       moc_id		          varchar(24),  --主叫内部编号
       mtc_id 		          varchar(24),  --被叫内部编号
       enterprise_id        varchar(10),  --集团编号
       service_type         varchar(4),   --移动增值服务类型
       service_code         varchar(20),	--移动附加服务代码
       bill_indicate        smallint,     --指定计费方
       sp_rela_type         smallint,     --运营商关系类型
       rating_flag          smallint,     --批价标志
       item_code1           integer,      --科目代码1
       item_code2           integer,      --科目代码2
       item_code3           integer,      --科目代码3
       item_code4           integer,      --科目代码4
       free_res_code1       integer,      --免费资源类型1
       free_res_code2       integer,      --免费资源类型2
       free_res_code3       integer,      --免费资源类型3
       original_file        varchar(64),  --源文件
       usertype_id          integer,      --用户类型
       opp_plan_id          integer,      --对端PLAN_ID
       opp_noaccess_number  varchar(30),  --去IP头的对端号码
       input_time           timestamp,    --话单入库时间
       rating_res           integer,      --计费资源量
       free_res_val1        integer,      --免费资源量1
       free_res_val2        integer,      --免费资源量2
       free_res_val3        integer,      --免费资源量3
       std_unit             integer,      --本地标准计费单元量
       toll_std_unit        integer,      --长途标准计费单元量
       ori_basic_charge     integer,      --原始基本费
       ori_toll_charge      integer,      --原始长话费
       ori_other_charge     integer,      --其他费用
       addup_res            integer,      --累计资源量
       call_duration        integer,		--通话时长
       call_duration_m      integer,		--计费时长(分钟)
       call_duration_s      integer,		--长途计费时长(分钟)
       bill_duration        integer,		--收费通话时长
       basecall_fee         decimal(9,2),	--基本通话费
       toll_fee             decimal(9,2),	--长途费
       info_fee             decimal(9,2),	--信息费
       other_fee            decimal(9,2),	--其他费
       charge1              decimal(9,2),	--费用1
       charge1_disc         decimal(9,2),	--费用1优惠
       charge2              decimal(9,2),	--费用2
       charge2_disc         decimal(9,2),	--费用2优惠
       charge3              decimal(9,2),	--费用3
       charge3_disc         decimal(9,2),	--费用3优惠
       charge4              decimal(9,2),	--费用4
       charge4_disc         decimal(9,2)	--费用4优惠
)
in tbs_cdr_voice
index in tbs_index
partitioning key (user_id) using hashing
not logged initially

select * from syscat.tables where tabname like 'DWD_MR_RING_CASE_MEMB_INFO_%'
order by 2


select * from ODS_CDR_CALL_20110118 fetch first 10 rows only

select imei,OPP_PLAN_ID,RATING_FLAG,USERTYPE_ID from  bass2.cdr_call_dtl_20110118 fetch first 100 rows only

select * from cdr_call_20110118 fetch first 10 rows only




select count(0) from DWD_NG2_M99912_201011
select count(0) from DWD_NG2_M99903_201011
select count(0) from DWD_NG2_M99913_201011
select count(0) from DWD_NG2_M99904_201011
select count(0) from DWD_NG2_M99914_201011
select count(0) from DWD_NG2_M99905_201011
select count(0) from DWD_NG2_M99906_201011
select count(0) from DWD_NG2_M99916_201011

select * from dim_call_calltype




		SELECT 
         TIME_ID
        ,DEVICE_ID
        ,PROPERTY_ID
        ,VALUE
        ,VALUE_DESC
		FROM    bass2.DIM_DEVICE_PROFILE
		where value is not null and value_desc is not null and time_id is not null
		and time_id <= '201012'
        
        
select * from BASS2.DWD_NG2_SCH_CONTROL_ALARM

create table DWD_NG2_M99912_201011 like DWD_NG2_M99912_YYYYMM in TBS_ODS_OTHER index in TBS_INDEX partitioning key ( DEVICE_ID,PROPERTY_ID ) using hashing not logged initially

 
 CREATE TABLE "BASS2   "."DWD_NG2_M99912_YYYYMM"  (
         TIME_ID                VARCHAR(8)          --时间
        ,DEVICE_ID              VARCHAR(16)         --设备标识
        ,PROPERTY_ID            VARCHAR(16)         --属性标识
        ,VALUE                  VARCHAR(16)         --属性值
        ,VALUE_DESC             VARCHAR(1024)       --值描述
                  )   
                 DISTRIBUTE BY HASH("DEVICE_ID",  
                 "PROPERTY_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX"  




select * from DWD_NG2_M99912_YYYYMM

select * from APP.SCH_CONTROL_RUNLOG

select * from DWD_NG2_SCH_CONTROL_ALARM


CREATE TABLE "BASS2     "."DWD_NG2_SCH_CONTROL_ALARM"  (
                  "CONTROL_CODE" VARCHAR(50) NOT NULL , 
                  "CMD_LINE" VARCHAR(300) , 
                  "GRADE" INTEGER , 
                  "CONTENT" VARCHAR(600) , 
                  "ALARMTIME" TIMESTAMP , 
                  "USERID" VARCHAR(18) , 
                  "FLAG" INTEGER , 
                  "DEALTIME" TIMESTAMP )   
                 DISTRIBUTE BY HASH("CONTROL_CODE")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  


select * from APP.SCH_CONTROL_ALARM


select * from app.sch_control_alarm 

create table DWD_NG2_M99903_201011 like DWD_NG2_M99903_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SCHMARKER ) using hashing not logged initially
create table DWD_NG2_M99905_201011 like DWD_NG2_M99905_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( LACCODE,CELLCODE ) using hashing not logged initially
create table DWD_NG2_M99904_201011 like DWD_NG2_M99904_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( PRODUCT_NO,GROUPCODE ) using hashing not logged initially
create table DWD_NG2_M99906_201011 like DWD_NG2_M99906_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SCHMARKER ) using hashing not logged initially
create table DWD_NG2_M99914_201011 like DWD_NG2_M99914_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( PRODUCT_NO,GROUPCODE ) using hashing not logged initially
create table DWD_NG2_M99913_201011 like DWD_NG2_M99913_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SCHMARKER,PRODUCT_NO ) using hashing not logged initially
create table DWD_NG2_M99916_201011 like DWD_NG2_M99916_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SCHMARKER ) using hashing not logged initially


select * from DWD_NG2_M99906_201011

select * from DWD_NG2_M99905_201011

create table DWD_NG2_M99904_201011 like DWD_NG2_M99904_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( PRODUCT_NO,GROUPCODE ) using hashing not logged initially

select * from DWD_NG2_M99904_201011

select * from DWD_NG2_M99903_201011

create table DWD_NG2_M99914_201011 like DWD_NG2_M99914_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( PRODUCT_NO,GROUPCODE ) using hashing not logged initially

select * from db2inst1.ADVISE_INSTANCE


select a.user_id ,b.SCHMARKER ,max(b.groupcode) groupcode ,b.SCHNAME                        from (select a.user_id,lac_id,cell_id from  bass2.dw_call_cell_201011  a where a.plan_id                         in (89140007,89150010)) a                        join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE                        group by user_id,b.SCHMARKER  ,b.SCHNAME                        
    
    
    drop index "BASS2"."IDX_CC_201011"
    
    CREATE INDEX "BASS2"."IDX_CC_201011" ON "BASS2   "."DW_CALL_CELL_201011"
   ("CELL_ID" ASC, "LAC_ID" ASC, "USER_ID" ASC, "PLAN_ID"
   ASC)
   
   
select * from sysibmadm.long_running_sql fetch first 1000 rows only



select a.user_id,lac_id,cell_id from  bass2.dw_call_cell_201011  a where a.plan_id 
                        in (89140007,89150010)

select plan_id,count(0)
from bass2.dw_call_cell_201011
group by plan_id
                         

select * from  bass2.dw_call_cell_201011  a where a.plan_id 
                        in (89140007,89150010)
                        

create table DWD_NG2_M99906_201011 like DWD_NG2_M99906_YYYYMM in TBS_3H index in TBS_INDEX partitioning key ( SCHMARKER ) using hashing not logged initially

select * from syscat.tables where tabname like  '%M99902%'

select * from DWD_NG2_P99999    


db2 "load client from /bassapp/bass2/panzw2/P99999.txt \
of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000    \
replace into DWD_NG2_P99999"

--DROP TABLE DWD_NG2_P99999		
CREATE TABLE DWD_NG2_P99999 (		
organize_id			bigint
,organize_name		varchar(200)
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( organize_id )
 USING HASHING
 
 

select * from DWD_NG2_P99999

select SNAPSHOT_TIMESTAMP,
       substr(BP_NAME,1,15) BP_NAME,
       TOTAL_HIT_RATIO_PERCENT as TOTAL_HIT,
       DATA_HIT_RATIO_PERCENT as DATA_HIT,
       INDEX_HIT_RATIO_PERCENT as INDEX_HIT,
       DBPARTITIONNUM as par
  from SYSIBMADM.BP_HITRATIO
 where BP_NAME in ('IBMDEFAULTBP','POOL_32K')
 order by BP_NAME,DBPARTITIONNUM
 
select * from sysibmadm.log_utilization


select tbsp_id,
       substr(tbsp_name, 1, 20) tbsp_name,
       dbpartitionnum,
       accessible
  from SYSIBMADM.CONTAINER_UTILIZATION
 order by tbsp_id, dbpartitionnum
 


select * from SYSIBMADM.TBSP_UTILIZATION

select TBSP_ID ID,
       substr(TBSP_NAME,1,18) TBSP_NAME,
       substr(TBSP_TYPE,1,4) TYPE,
       -- TBSP_TOTAL_SIZE_KB / 1024 / 1024 as total_size,
       TBSP_USED_SIZE_KB / 1024 / 1024 as used,
       TBSP_FREE_SIZE_KB / 1024 / 1024 as free,
       TBSP_UTILIZATION_PERCENT as percent,
       substr(TBSP_STATE,1,12) STATE,
       DBPARTITIONNUM as par
  from SYSIBMADM.TBSP_UTILIZATION
 order by TBSP_ID,DBPARTITIONNUM

/**

insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200712', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200712 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200801', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200801 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200802', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200802 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200803', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200803 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200804', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200804 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200805', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200805 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200806', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200806 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200807', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200807 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200808', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200808 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200809', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200809 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200810', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200810 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200811', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200811 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200812', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200812 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200901', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200901 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200902', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200902 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200903', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200903 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200904', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200904 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200905', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200905 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200906', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200906 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200907', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200907 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200908', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200908 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200909_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200909_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200910_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200910_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200911_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200911_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200912_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200912_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201002_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201002_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201003_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201003_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201004_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201004_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201005_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201005_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201006_BAKNGGJ', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201006_BAKNGGJ group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201007', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201007 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201008', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201008 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201009', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201009 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201010', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201010 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201011', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201011 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201012', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201012 group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201101', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201101 group by nodenumber(USER_ID) 





insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200712_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200712_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200801_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200801_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200802_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200802_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200803_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200803_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200804_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200804_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200805_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200805_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200806_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200806_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200807_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200807_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200808_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200808_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200809_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200809_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200810_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200810_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200811_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200811_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200812_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200812_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200901_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200901_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200902_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200902_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200903_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200903_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200904_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200904_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200905_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200905_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200906_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200906_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200907_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200907_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200908_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200908_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200909_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200909_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200910_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200910_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200911_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200911_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_200912_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_200912_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201002_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201002_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201003_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201003_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201004_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201004_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201005_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201005_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201006_BAKNGGJ_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201006_BAKNGGJ_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201007_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201007_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201008_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201008_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201009_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201009_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201010_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201010_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201011_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201011_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201012_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201012_BAK group by nodenumber(USER_ID) 
insert into t_DW_PRODUCT_BUSI_SPROM_DM_stats select 'DW_PRODUCT_BUSI_SPROM_DM_201101_BAK', nodenumber(USER_ID) ,count(*) as using_num from bass2.DW_PRODUCT_BUSI_SPROM_DM_201101_BAK group by nodenumber(USER_ID) 




rename  DW_ENTERPRISE_UNIPAY_DM_200802				to DW_ENTERPRISE_UNIPAY_DM_200802_BAK                 


create table DW_ENTERPRISE_UNIPAY_DM_200802         LIKE      DW_ENTERPRISE_UNIPAY_DM_200802_BAK         DISTRIBUTE BY HASH( ENTERPRISE_ID,ACCT_ID ) IN  TBS_3H  INDEX IN  TBS_INDEX  NOT LOGGED INITIALLY

INSERT INTO DW_ENTERPRISE_UNIPAY_DM_200802 SELECT * FROM 				DW_ENTERPRISE_UNIPAY_DM_200802_BAK                

select * from DW_ENTERPRISE_UNIPAY_DM_200802


select 'DW_ENTERPRISE_UNIPAY_DM_200802', nodenumber(ENTERPRISE_ID) ,count(*) as using_num 
from bass2.DW_ENTERPRISE_UNIPAY_DM_200802 
group by nodenumber(ENTERPRISE_ID) order by 1,2


create table t_DW_ENTERPRISE_stats(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing

insert into t_DW_ENTERPRISE_stats
select 'DW_ENTERPRISE_UNIPAY_DM_200802_BAK', nodenumber(ENTERPRISE_ID) ,count(*) as using_num 
from bass2.DW_ENTERPRISE_UNIPAY_DM_200802_BAK 
group by nodenumber(ENTERPRISE_ID) 


select * from t_DW_ENTERPRISE_stats
order by 1,2

delete from t_DW_ENTERPRISE_stats
where name = 'DW_ENTERPRISE_UNIPAY_DM_200802_BAK'




create table t_DW_PRODUCT_BUSI_SPROM_DM_stats(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing
DW_PRODUCT_BUSI_SPROM_DM_201101



select * from t_DW_ENTERPRISE_stats

select * from t_DW_ENTERPRISE_stats where name like ''


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_ENTERPRISE_stats where name not like '%BAK') a
join 
(select  *  from t_DW_ENTERPRISE_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1





select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') a
join 
(select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') a
right join 
(select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1




select b.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') a
right join 
(select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1



select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') a
join 
(select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
where a.name like 'DW_PRODUCT_BUSI_SPROM%'
order by 2,1

select count(0) from t_DW_PRODUCT_BUSI_SPROM_DM_stats  where name not like '%BAK'



select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') a
left join 
 (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
where b.name like 'DW_PRODUCT_BUSI_SPROM%'
order by 2,1


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name  like '%BAK') a
left join 
 (select  *  from t_DW_PRODUCT_BUSI_SPROM_DM_stats where name not like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
where a.name like 'DW_PRODUCT_BUSI_SPROM%'
order by 2,1



select * from NGBASS20.ODS_PRODUCT_ENTPROD_201011

select * from NGBASS20.ODS_TERM_ORDER_HIS_201011

select * from NGBASS20.DIM_AREA_CHARACTER

select * from NGBASS20.DIM_AREA_CHARACTER

select * from NGBASS20.DIM_PUB_CELL

select * from NGBASS20.DIM_TERM_PROFILE


select  distinct name   from t_DW_ENTERPRISE_stats where name  like '%BAK'


select 'rename '||tabname ||' to '||tabname||'_bak' from syscat.tables where tabname like 'DW_ENTERPRISE_UNIPAY_DM%'
order by 1


select tabname from syscat.tables where tabname like 'DW_ENTERPRISE_UNIPAY_DM%'
and tabname not like '%BAK'
order by 2

rename DW_ENTERPRISE_UNIPAY_DM_200803 to DW_ENTERPRISE_UNIPAY_DM_200803_bak

select * from NGBASS20.ODS_PRODUCT_SCROECHG_HIS_201011

select * from NGBASS20.ODS_ENTERPRISE_ORDER_HIS_201011


select * from NGBASS20.ODS_PRODUCT_ORDER_HIS_201011

select * from NGBASS20.ODS_PRODUCT_SCROECHG_HIS_201011

select * from  ngbass20.dim_gift_code


select paytype_id,count(0) from   NGBASS20.ODS_PRODUCT_INDI_201011
group by paytype_id

select * from 

select * from NGBASS20.DIM_FEE_RATIO

select * from NGBASS20.DIM_PRODUCT_ITEM


select * from NGBASS20.ODS_ENTERPRISE_DETAIL_201011


select * from BASS2.DIM_GEOGRAPHY_TYPE

select * from bass2.dim_pub_region

select * from bass2.dim_pub_county

select * from bass2.dim_pub_city


select * from NGBASS20.DIM_PUB_CAMP_CHANNEL

select * from NGBASS20.DIM_PUB_ELEC_CHANNEL

select * from BASS2.VGOP_15210_20110111

BASS2_Dw_vgop_15210_dm.tcl

select * from app.sch_control_before where control_code = 'BASS2_Dw_vgop_15210_dm.tcl'
select * from app.sch_control_before where control_code = 'TR1_VGOP_D_15210'
select * from app.sch_control_task where control_code = 'TR1_VGOP_D_15210'

select * from app.sch_control_runlog where  control_code = 'BASS2_Dw_vgop_15210_dm.tcl'

select * from app.sch_control_task where control_code = 'BASS2_Dw_vgop_15210_dm.tcl'


TR1_VGOP_D_15210

select * from ngbass20.dim_pub_croptype

select * from bass2.dim_enterprise_level

select * from bass2.dim_entcstsig_type_st

select * from NGBASS20.ODS_CUST_GROUP_201011


select * from NGBASS20.ODS_ENTERPRISE_MSG_201011

select * from NGBASS20.DIM_PUB_IDENTYPE

ODS_CUST_INDV_YYYYMM

select * from NGBASS20.ODS_CUST_INDV_201011

select * from NGBASS20.DIM_PUB_CITY

select * from NGBASS20.DIM_PUB_INDUSTRY

select distinct PROVINCE_ID from NGBASS20.ODS_CUST_201011

select * from BASS2.DIM_PUB_CITY

select * from NGBASS20.ODS_CHL_PAYINFO_201011

select * from bass2.DIM_ACCT_PAYTYPE

select * from NGBASS20.DIM_CHL_STATUS_HIS

select * from NGBASS20.DIM_PUB_PHY_CHANNEL

select * from NGBASS20.ODS_DSMP_SP_COMPANY_CODE_201011

select * from bass2.DIM_STATU_TYPE_LKP


select * from bass2.DIM_SP_ATTRIBUTE_LKP

select * from bass2.dim_boss_staff


select * from syscat.tables where  tabschema = 'NGBASS20'
and tabname like '%CHL_PAYINFO%'

NGBASS20.DW_CHL_PAYINFO_YYYYMM
sele

create table DWD_NG2_I06020_20101231 like DWD_NG2_I06020_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( SEQUENCE_NO ) using hashing not logged initially

drop table DWD_NG2_I06020_20101231


create table DWD_NG2_A02025_20101231 like DWD_NG2_A02025_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( KEY_NUM,BEGIN_TIME ) using hashing not logged initially

create table DWD_NG2_A02025_20101231 like DWD_NG2_A02025_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( KEY_NUM,BEGIN_TIME ) using hashing not logged initially


create table DWD_NG2_A02025_20101231 like DWD_NG2_A02025_YYYYMMDD in TBS_3H index in TBS_INDEX partitioning key ( KEY_NUM,BEGIN_TIME ) using hashing not logged initially
drop table DWD_NG2_A02025_20101231

select * from DWD_NG2_A02025_YYYYMMDD

select * from syscat.tables where tabname like '%DWD_NG2_A02025_YYYYMMDD%'


select 'xxxxx', case when partkeyseq = 1 then  colname  else ','||colname end colname 
from syscat.columns 
where tabschema = 'BASS2' and tabname = 'DW_KF_SEND_SMS_DATA_DM_YYYYMM' and partkeyseq >= 1 
order by partkeyseq

DWD_NG2_A02025_YYYYMMDD

select * from syscat.columns where partkey

select count(0) from  syscat.columns 




select * FROM syscat.tables where   tabname like '%PART%'
--DROP TABLE DWD_NG2_I05046_YYYYMMDD
 CREATE TABLE DWD_NG2_I05046_YYYYMMDD (
         row_id                 integer             --记录标记        
        ,sequence_no            varchar(25)         --话单消息序列号  
        ,PLATFORM_ID            integer             --业务平台编码    
        ,busi_spec_id           INTEGER             --业务特性ID      
        ,share_as_id            BIGINT              --共享AS编号      
        ,drtype_id              integer             --话单类型        
        ,send_or_rec            integer             --是否发起/接收话单
        ,S_user_id              varchar(20)         --共享发起用户标识
        ,S_msisdn               varchar(15)         --共享发起用户MSISDN
        ,S_city_id              varchar(7)          --共享发起用户归属地
        ,S_roam_city_id         varchar(7)          --共享发起用户的漫游地
        ,S_IP_ADDR              varchar(15)         --发起共享用户终端IP地址
        ,S_TERM_TYPE            varchar(30)         --发起共享用户的终端类型
        ,R_user_id              varchar(20)         --共享接收用户标识
        ,R_msisdn               varchar(15)         --共享接收用户MSISDN
        ,R_PROVINCE_id          varchar(7)          --共享接收用户归属省
        ,R_IP_ADDR              varchar(15)         --接收共享用户终端IP地址
        ,R_TERM_TYPE            varchar(30)         --接收共享用户的终端类型
        ,start_time             TIMESTAMP           --共享开始时间    
        ,DURATION               integer             --共享持续时长    
        ,FLOW_CNT               INTEGER             --媒体数据流量    
        ,SUCCESS_FLAG           SMALLINT            --Video Share过程是否成功标识
        ,sp_code                VARCHAR(20)         --SP代码    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( sequence_no )
 USING HASHING


select * from DWD_NG2_I05044_20101202
select * from DWD_NG2_I05045_20110110

--DROP TABLE DWD_NG2_I05045_20110110
 CREATE TABLE DWD_NG2_I05045_20110110 (
        FLOW_NO                 VARCHAR(20)        --呼叫唯一标志                                             
        ,UCID                     VARCHAR(30)       --话单轨迹          
        ,CALLING                  VARCHAR(24)       --主叫号码          
        ,CALLED                   VARCHAR(24)       --被叫号码          
        ,WAIT_BEGIN_TIME          TIMESTAMP         --等待开始时间      
        ,WAIT_END_TIME            TIMESTAMP         --等待结束时间      
        ,ANS_BEGIN_TIME           TIMESTAMP         --应答开始时间      
        ,ANS_END_TIME             TIMESTAMP         --应答结束时间      
        ,CMM_BEGIN_TIME           TIMESTAMP         --通话开始时间      
        ,CMM_END_TIME             TIMESTAMP         --通话结束时间      
        ,OPERATION_TYPE           VARCHAR(8)        --业务类型号        
        ,DEV_TYPE_ID                  SMALLINT          --设备类型          
        ,DEV_CODE           VARCHAR(8)        --设备号            
        ,CALL_TYPE                   SMALLINT            --呼叫类型    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( FLOW_NO )
 USING HASHING



--可选

--DROP TABLE DWD_NG2_I05044_20101202
 CREATE TABLE DWD_NG2_I05044_20101202 (
        row_id                 INTEGER             --记录标记        
        ,sequence_no            VARCHAR(25)         --留言消息序列号  
        ,product_no             VARCHAR(15)         --计费用户号码    
        ,imsi                   VARCHAR(15)         --计费用户IMSI    
        ,province_id            VARCHAR(20)         --计费用户归属省  
        ,platform_id            BIGINT              --留言平台编码    
        ,opp_number             VARCHAR(30)         --留言用户号码    
        ,drtype_id              INTEGER             --话单类型        
        ,start_time             TIMESTAMP           --留言开始时间    
        ,duration               INTEGER             --留言持续时长    
        ,msg_size               INTEGER             --留言数据容量    
        ,msg_type               SMALLINT            --留言类型        
        ,msg_format             SMALLINT            --留言媒体格式    
        ,process_time           TIMESTAMP           --留言访问时间    
        ,msg_access_type        SMALLINT            --留言访问方式    
        ,sp_code                VARCHAR(20)         --SP代码          
        ,operator_code          VARCHAR(20)         --业务代码        
        ,service_code           VARCHAR(32)         --服务代码        
        ,bsc_fee                INTEGER             --通信费          
        ,info_fee               INTEGER             --信息费          
        ,info_fee_disc          INTEGER             --优惠后信息费    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( sequence_no )
 USING HASHING


--DROP TABLE DWD_NG2_I05046_20101202
 CREATE TABLE DWD_NG2_I05046_20101202 (
         row_id                 integer             --记录标记        
        ,sequence_no            varchar(25)         --话单消息序列号  
        ,PLATFORM_ID            integer             --业务平台编码    
        ,busi_spec_id           INTEGER             --业务特性ID      
        ,share_as_id            BIGINT              --共享AS编号      
        ,drtype_id              integer             --话单类型        
        ,send_or_rec            integer             --是否发起/接收话单
        ,S_user_id              varchar(20)         --共享发起用户标识
        ,S_msisdn               varchar(15)         --共享发起用户MSISDN
        ,S_city_id              varchar(7)          --共享发起用户归属地
        ,S_roam_city_id         varchar(7)          --共享发起用户的漫游地
        ,S_IP_ADDR              varchar(15)         --发起共享用户终端IP地址
        ,S_TERM_TYPE            varchar(30)         --发起共享用户的终端类型
        ,R_user_id              varchar(20)         --共享接收用户标识
        ,R_msisdn               varchar(15)         --共享接收用户MSISDN
        ,R_PROVINCE_id          varchar(7)          --共享接收用户归属省
        ,R_IP_ADDR              varchar(15)         --接收共享用户终端IP地址
        ,R_TERM_TYPE            varchar(30)         --接收共享用户的终端类型
        ,start_time             TIMESTAMP           --共享开始时间    
        ,DURATION               integer             --共享持续时长    
        ,FLOW_CNT               INTEGER             --媒体数据流量    
        ,SUCCESS_FLAG           SMALLINT            --Video Share过程是否成功标识
        ,sp_code                VARCHAR(20)         --SP代码    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( sequence_no )
 USING HASHING


视频留言业务清单
select * from etl_load_table_map_MTEST where task_id LIKE 'I0504%'

                        select 
                                 A.ROW_ID
                                ,A.SEQUENCE_NO
                                ,A.plan_id PLATFORM_ID
                                ,A.condition_id  BUSI_SPEC_ID
                                ,A.service_id  SHARE_AS_ID
                                ,DRTYPE_ID
                                ,int(rand(0)*2) SEND_OR_REC
                                ,a.user_id S_USER_ID
                                ,a.product_no S_MSISDN
                                ,A.city_id S_CITY_ID
                                ,A.ROAM_CITY_ID S_ROAM_CITY_ID
                                ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
                                ||'.'||char(int(10+rand(1)*255)),' ','') S_IP_ADDR
                                ,c.termbrand_name S_TERM_TYPE
                                --,D.user_id R_USER_ID
                                ,a.opp_number R_MSISDN
                                ,province_id R_PROVINCE_ID
                                ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
                                ||'.'||char(int(10+rand(1)*255)),' ','') R_IP_ADDR
                                ,b.termbrand_name R_TERM_TYPE
                                ,substr(replace(replace(char(a.start_time),'-',''),'.',''),1,14) start_time
                                ,int(stop_time-start_time) DURATION
                                ,message_length*1024 FLOW_CNT
                                ,late_link SUCCESS_FLAG
                                ,SP_CODE
                                from cdr_ismg_20101202 a
                                join (select product_no,max(termbrand_name||termtype_name) termbrand_name
                        from dw_product_imeifunc_201011 a where termbrand_name is not null
                        group by product_no) b on a.opp_number = b.product_no
                                join (select product_no,max(termbrand_name||termtype_name) termbrand_name
                        from dw_product_imeifunc_201011 a where termbrand_name is not null
                        group by product_no) c on a.product_no = c.product_no
                                JOIN dw_product_201011 D ON A.OPP_NUMBER = D.PRODUCT_NO
                        where a.product_no <> a.opp_number AND D.USERSTATUS_ID = 1 

select 'a'||
'b'
from dual

DELETE FROM etl_load_table_map_mtest WHERE TASK_ID = 'I05044'
INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I05044','DWD_NG2_I05044_YYYYMMDD','视频留言业务清单', 0,null)
 

                        select 
                                 A.ROW_ID
                                ,A.SEQUENCE_NO
                                ,A.plan_id PLATFORM_ID
                                ,A.condition_id  BUSI_SPEC_ID
                                ,A.service_id  SHARE_AS_ID
                                ,DRTYPE_ID
                                ,int(rand(0)*2) SEND_OR_REC
                                ,a.user_id S_USER_ID
                                ,a.product_no S_MSISDN
                                ,A.city_id S_CITY_ID
                                ,A.ROAM_CITY_ID S_ROAM_CITY_ID
                                ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
                        ,' ','') S_IP_ADDR
                                ,c.termbrand_name S_TERM_TYPE
                                ,D.user_id R_USER_ID
                                ,a.opp_number R_MSISDN
                                ,province_id R_PROVINCE_ID
                                ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
                        ,' ','') R_IP_ADDR
                                ,b.termbrand_name R_TERM_TYPE
                                ,substr(replace(replace(char(a.start_time),'-',''),'.',''),1,14) start_time
                                ,int(stop_time-start_time) DURATION
                                ,message_length*1024 FLOW_CNT
                                ,late_link SUCCESS_FLAG
                                ,SP_CODE
                                from cdr_ismg_20110102 a
                                join (select product_no,max(termbrand_name||termtype_name) termbrand_name
                        from dw_product_imeifunc_201012 a where termbrand_name is not null
                        group by product_no) b on a.opp_number = b.product_no
                                join (select product_no,max(termbrand_name||termtype_name) termbrand_name
                        from dw_product_imeifunc_201012 a where termbrand_name is not null
                        group by product_no) c on a.product_no = c.product_no
                                JOIN dw_product_201012 D ON A.OPP_NUMBER = D.PRODUCT_NO
                        where a.product_no <> a.opp_number AND D.USERSTATUS_ID = 1 
                        

select count(0) from 
cdr_ismg_20101101 where product_no <> opp_number


select 
         A.ROW_ID
        ,A.SEQUENCE_NO
        ,A.plan_id PLATFORM_ID
        ,A.condition_id  BUSI_SPEC_ID
        ,A.service_id  SHARE_AS_ID
        ,DRTYPE_ID
        ,int(rand(0)*2) SEND_OR_REC
        ,a.user_id S_USER_ID
        ,a.product_no S_MSISDN
        ,A.city_id S_CITY_ID
        ,A.ROAM_CITY_ID S_ROAM_CITY_ID
        ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
,' ','') S_IP_ADDR
        ,c.termbrand_name S_TERM_TYPE
        --,b.user_id R_USER_ID
        ,a.opp_number R_MSISDN
        ,province_id R_PROVINCE_ID
        ,replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
,' ','') R_IP_ADDR
        ,b.termbrand_name R_TERM_TYPE
        ,substr(replace(replace(char(a.start_time),'-',''),'.',''),1,14) start_time
        ,int(stop_time-start_time) DURATION
        ,message_length*1024 FLOW_CNT
        ,late_link SUCCESS_FLAG
        ,SP_CODE
        from cdr_ismg_20101101 a
        join (select product_no,max(termbrand_name||termtype_name) termbrand_name
from dw_product_imeifunc_201011 a where termbrand_name is not null
group by product_no) b on a.opp_number = b.product_no
        join (select product_no,max(termbrand_name||termtype_name) termbrand_name
from dw_product_imeifunc_201011 a where termbrand_name is not null
group by product_no) c on a.product_no = c.product_no
        JOIN dw_product_201012 D ON A.OPP_NUMBER = D.PRODUCT_NO
where a.product_no <> a.opp_number AND D.USERSTATUS_ID = 1 
        
        

        
select count(0) , count(distinct product_no) from 
(select product_no,user_id,max(value(termbrand_name,'0')) termbrand_name
from dw_product_imeifunc_201011 a where termbrand_name is not null
group by product_no,user_id
) t

select count(0) , count(distinct product_no) from dw_product_201012
where userstatus_id = 1


select count(0),userstatus_id from dwd_product_20101231
group by userstatus_id

select * from dim_pub_userstatus

select * from syscat.tables where tabname like '%DW_PRODUCT%'

select * from DIM_USER_STATUS


select * from dw_product_imeifunc_201011

select product_no,user_id,max(termbrand_name||termtype_name) termbrand_name
from dw_product_imeifunc_201011 a where termbrand_name is not null
group by product_no,user_id

select * from dim_property_value_range


select count(0) , count(distinct user_id||product_no||termbrand_name) from dw_product_imeifunc_201011
select * from VGOP_11204_20101231

select * from dwd_wcc_user_action_20101231
dw_product_imeifunc_yyyymm
select replace('172.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))||'.'||char(int(10+rand(1)*255))
,' ','') from dw_product_imeifunc_201012

select * from syscat.tables where tabname like '%11204%'

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I05046','DWD_NG2_I05046_YYYYMMDD','视频共享业务清单', 0,null)

--DROP TABLE DWD_NG2_I05046_YYYYMMDD
 CREATE TABLE DWD_NG2_I05046_YYYYMMDD (
         row_id                 integer             ----记录标记        
        ,sequence_no            varchar(25)         ----话单消息序列号  
        ,PLATFORM_ID            integer             ----业务平台编码    
        ,busi_spec_id           INTEGER             ----业务特性ID      
        ,share_as_id            BIGINT              ----共享AS编号      
        ,drtype_id              integer             ----话单类型        
        ,send_or_rec            integer             ----是否发起/接收话单
        ,S_user_id              varchar(12)         ----共享发起用户标识
        ,S_msisdn               varchar(15)         ----共享发起用户MSISDN
        ,S_city_id              varchar(7)          ----共享发起用户归属地
        ,S_roam_city_id         varchar(7)          ----共享发起用户的漫游地
        ,S_IP_ADDR              varchar(15)         ----发起共享用户终端IP地址
        ,S_TERM_TYPE            varchar(30)         ----发起共享用户的终端类型
        ,R_user_id              varchar(12)         ----共享接收用户标识
        ,R_msisdn               varchar(15)         ----共享接收用户MSISDN
        ,R_PROVINCE_id          varchar(7)          ----共享接收用户归属省
        ,R_IP_ADDR              varchar(15)         ----接收共享用户终端IP地址
        ,R_TERM_TYPE            varchar(30)         ----接收共享用户的终端类型
        ,start_time             TIMESTAMP           ----共享开始时间    
        ,DURATION               integer             ----共享持续时长    
        ,FLOW_CNT               INTEGER             ----媒体数据流量    
        ,SUCCESS_FLAG           SMALLINT            ----Video Share过程是否成功标识
        ,sp_code                VARCHAR(20)         ----SP代码    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( sequence_no )
 USING HASHING



select product_no,comp_product_no,opp_city_id from dw_call_opposite_20101101
where length(comp_product_no) = 11 and  substr(comp_product_no,1,3) between '134' and '139'
group by product_no,comp_product_no,opp_city_id



select condition_id,int(rand(0)*2) from cdr_ismg_20101101

select stop_time - start_time from cdr_ismg_20101101


--DROP TABLE DWD_NG2_I05044_YYYYMMDD
 CREATE TABLE DWD_NG2_I05044_YYYYMMDD (
        row_id                 INTEGER             ----记录标记        
        ,sequence_no            VARCHAR(25)         ----留言消息序列号  
        ,product_no             VARCHAR(15)         ----计费用户号码    
        ,imsi                   VARCHAR(15)         ----计费用户IMSI    
        ,province_id            VARCHAR(20)         ----计费用户归属省  
        ,platform_id            BIGINT              ----留言平台编码    
        ,opp_number             VARCHAR(30)         ----留言用户号码    
        ,drtype_id              INTEGER             ----话单类型        
        ,start_time             TIMESTAMP           ----留言开始时间    
        ,duration               INTEGER             ----留言持续时长    
        ,msg_size               INTEGER             ----留言数据容量    
        ,msg_type               SMALLINT            ----留言类型        
        ,msg_format             SMALLINT            ----留言媒体格式    
        ,process_time           TIMESTAMP           ----留言访问时间    
        ,msg_access_type        SMALLINT            ----留言访问方式    
        ,sp_code                VARCHAR(20)         ----SP代码          
        ,operator_code          VARCHAR(20)         ----业务代码        
        ,service_code           VARCHAR(32)         ----服务代码        
        ,bsc_fee                INTEGER             ----通信费          
        ,info_fee               INTEGER             ----信息费          
        ,info_fee_disc          INTEGER             ----优惠后信息费    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( sequence_no )
 USING HASHING
INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I05044','DWD_NG2_I05044_YYYYMMDD','视频IVR业务清单', 0,null)

select * from etl_load_table_map where task_id = 'I05044'
select   count(0),count(distinct sequence_no)
from cdr_ismg_20101231 
in 

89440008
89740002
89450001


('89410012','89510012','89440008','89740002','89450001')
group by plan_id
order by 2 desc 




select 
row_id
,sequence_no
,product_no
,imsi
,province_id
,plan_id
,opp_number
,drtype_id
,start_time
,int(stop_time-start_time)
,rating_res
,source_type
,bill_flag
,process_time
,record_type
,sp_code
,operator_code
,service_code
,charge1
,charge2
,charge1_disc
from cdr_ismg_20101231
where opp_number is not null 
and plan_id in (89410012,89510012,89440008,89740002,89450001)


select count(0)
from cdr_ismg_20101231
where opp_number is not null and charge1_disc is not null 


select * from cdr_ismg_20101231

select * from dw_newbusi_call_20110108

select * from syscat.tables where tabname like '%CALL%' and card > 0 
select * from DW_NEWBUSI_CALL_DT


select count(distinct bill_id||char(update_time)||group_id),count(0) from NGBASS20.ODS_FINANCE_GROUP_ACCT_201011
where bill_id is not null

select count(0),
count(DISTINCT BUSI_ID            ||
CHAR(BUSI_START_TIME)    )

from  NGBASS20.ODS_SJ_BUSI_201011
select count(0),count(distinct USER_ID||FECTION_ID||CITY_ID||LEVEL_ID)  from NGBASS20.ODS_SJ_FECTION_USER_POINT_20101101


select count(distinct CUST_CONTACT_ID ),count(0) from NGBASS20.ODS_SJ_COMPLAIN_201011 

select * from NGBASS20.ODS_SJ_NUMBER_USER_STATE_201011

select count(0),count(distinct flow_id) from NGBASS20.ODS_SJ_NUMBER_OPER_LOG_201011
select 
FLOW_ID
,USER_ID
,PROVINCE_ID
,CITY_ID
,START_TIME
,END_TIME
,OPER_TYPE
,USE_TYPE
,IS_UP_FLAG
,OPER_RESULT
,SYN_TYPE
,UP_FLOW
,DN_FLOW
,UP_CONTACT_NUMS
,DN_CONTACT_NUMS
,UP_DAY_NUMS
,DN_DAY_NUMS
--,SYN_TERMINAL_TYPE
,SYN_TERMINAL_ID
,SYN_REC_TYPE
from NGBASS20.ODS_SJ_NUMBER_OPER_LOG_201011
select * from NGBASS20.ODS_SJ_NUMBER_OPER_LOG_201011


select count(0),count(distinct flow_id) from NGBASS20.ODS_SJ_FECTION_OPER_LOG_201011


select * from NGBASS20.DIM_SEND_STATE
select * from NGBASS20.DIM_SEND_TYPE

select count(0),count(distinct FLOW_ID) from NGBASS20.ODS_SJ_139MAIL_UPMMS_LOG_201011

select NGBASS20.ODS_SJ_ORDER_201011
select count(0),count(distinct char(ORDER_ID)||USER_ID||char(CUST_CONTACT_ID)    ) from NGBASS20.ODS_SJ_ORDER_201011
select count(0),count(distinct SEND_DATE    ) from NGBASS20.ODS_SJ_139MAIL_UPMMS_LOG_201011

select  from NGBASS20.DIM_ACTION_TYPE

select count(0),count(distinct LIST_ID    ) from NGBASS20.ODS_SJ_139MAIL_SP_ACTION_201011

select IS_DIRECT_ANSWER,count(0) from NGBASS20.ODS_SJ_COMPLAIN_201011
group by IS_DIRECT_ANSWER

select * from BASS2.dim_busi_call_svctype

select count(0) from NGBASS20.ODS_SJ_WAP_GATEWAY_LOG_201011


select * from NGBASS20.ODS_SJ_BUSI_201011

select count(0),count(distinct BUSI_START_TIME    ) from NGBASS20.ODS_SJ_BUSI_201011

SELECT * FROM NGBASS20.ODS_SJ_BUSI_201011				

select 'select * from '||tabschema||'.'||tabname||''  from syscat.tables where tabschema = 'NGBASS20'
order by 1


select * from syscat.tables where tabname like '%SJ_BUSI%'

select * from USYS_TABLE_MAINTAIN where table_id = 10175

select * from syscat.tables where tabname like '%ODS_CDR_GPRS_GXZ%'
select * from ods_res_ctms_exchg_2010122


select * from syscat.tables where tabname like '%ODS_RES_CTMS_EXCHG%'
order by 2



select * from DWD_NG2_I08117_20101202


create table tmp_pzw_t_timeformat
(
col timestamp
)
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( col )
 USING HASHING


alter table DWD_MR_OPER_CDR_20101223 activate not logged initially with empty table

select * from ETL_TASK_RUNNING where task_id = 'A98012'
order by stime desc 

alter table DWD_MR_OPER_CDR_20101223 activate not logged initially with empty table


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110130'||'170000') 
order by alarmtime desc 

select distinct content from app.sch_control_alarm 
where content like '%SQL%N%'

delete from app.sch_control_alarm 
where control_code='TR1_L_02031'
and alarmtime >=  timestamp('20110124'||'000000') 


select * from dw_res_ctms_exchg_201011


select * from BASS2.DW_RES_CTMS_EXCHG_20101228


select * from syscat.tables where tabname like '%DW_RES_CTMS_EXCHG%'
order by 2

select * from DW_RES_CTMS_EXCHG_201012

select count(0),count(distinct product_no) from DWD_MR_OPER_CDR_20101223


select count(0) from DWD_MR_OPER_CDR_20101223


select * from syscat.tables where tabname like '%DWD_MR_OPER_CDR_20101223%'





select * from app.sch_control_task where control_code like '%TR1_L_A98012%'


select * from VGOP_11202_20101221

select * from DWD_NG2_I08117_YYYYMMDD

select * from VGOP_11202_2010122001


select * from VGOP_14203_2010122201

select * from VGOP_16202_2010122001

select * from VGOP_14201_2010121820

VGOP_14201_YYYYMMDD

select 'SELECT COUNT(0) FROM '||tabname ||'' from syscat.tables where tabname like '%DWD_NG2%201012%'
and card > 0

select * from DWD_NG2_I08117_20101202


VGOP_14201_2010121820

VGOP_14208_YYYYMMDD


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20101223'||'000000') 
order by alarmtime desc 


select * from VGOP_15304_20101220





select * from DWD_MR_RING_CASE_MEMB_INFO_20101220

select * from app.sch_control_task where control_code like '%98015%'
select * from ODS_CDR_CALL_YYYYMMDD

select * from DWD_NG2_M99912_yyyymm

alter table DWD_NG2_M99912_yyyymm drop column op_time 


select *   from DWD_NG2_M99904_201011 g

select *   from DWD_NG2_M99913_201011 g




select *   from DWD_NG2_M99913_201011 g



select a.*,int(rand(1)*18)+30 b from DWD_NG2_P99934 a where dept_id between 30 and 48

select *   from DWD_NG2_M99914_201011 g
select *   from DWD_NG2_M99904_201011 g


select * from DWD_NG2_P99924
                    
/**	2010-12-15 15:18	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99914_201011				
CREATE TABLE DWD_NG2_M99914_201011 (				
        SCHMARKER           VARCHAR(32)         --学校标识
        ,GROUPCODE	VARCHAR(32)
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,CUST_NAME          VARCHAR(200)        --教职人员姓名
        ,SEX_ID             SMALLINT                --性别
        ,WORK_DEPT          VARCHAR(120)            --部门
        ,POSITION           VARCHAR(120)                --职位
        ,PRODUCT_NO         VARCHAR(15)         --手机号码
        ,OPERATOR           SMALLINT            --运营商归属
        ,BRAND_ID           BIGINT             --品牌
        ,LINK_PHONE         VARCHAR(32)         --其它联系电话
        ,EMAIL              VARCHAR(64)         --邮箱地址
        ,op_time			BIGINT
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( PRODUCT_NO,GROUPCODE )
 USING HASHING
                    
                    
alter table DWD_NG2_M99914_YYYYMM alter  POSITION set data type VARCHAR 


/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99924				
CREATE TABLE DWD_NG2_P99924 (				
        pos_id              integer          --部门标识
        ,pos_name        VARCHAR(20)          --部门名称
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( pos_id )
 USING HASHING

alter  DWD_NG2_I05002_YYYYMMDD modify  SERVICE_TYPE varcar(8)

alter table DWD_NG2_M99914_YYYYMM modify  POSITION  VARCHAR(120) 



select * from DWD_NG2_P99934
select * from DWD_NG2_P99924

select * from DWD_NG2_M99914_201011



/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99934				
CREATE TABLE DWD_NG2_P99934 (				
        dept_group              VARCHAR(5)          --部门分组
        ,dept_id		  integer
        ,dept_name        VARCHAR(20)          --部门名称
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( dept_id )
 USING HASHING



/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99924				
CREATE TABLE DWD_NG2_P99924 (				
        pos_id              VARCHAR(8)          --部门标识
        ,pos_name        VARCHAR(20)          --部门名称
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( pos_id )
 USING HASHING



select * from DWD_NG2_M99914_201011



select * from DWD_NG2_P99924

/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99924				
CREATE TABLE DWD_NG2_P99924 (				
        dept_id              VARCHAR(8)          --地市标识
        ,dept_name        VARCHAR(20)          --区县标识
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( dept_id )
 USING HASHING




select * from DWD_NG2_M99914_201011

select * from DWD_NG2_M99906_201011



alter table DWD_NG2_M99903_201011 add column op_time bigint
alter table DWD_NG2_M99904_201011 add column op_time bigint
alter table DWD_NG2_M99905_201011 add column op_time bigint
alter table DWD_NG2_M99906_201011 add column op_time bigint
alter table DWD_NG2_M99912_201011 add column op_time bigint
alter table DWD_NG2_M99913_201011 add column op_time bigint
alter table DWD_NG2_M99914_201011 add column op_time bigint
alter table DWD_NG2_M99916_201011 add column op_time bigint
alter table DWD_NG2_M99999_201011 add column op_time bigint
	select 
	     REGION
        ,COUNTY_CODE
        ,SCHMARKER
        ,SALE_TEAM_ID
        ,'校园直销'||trim(char(SALE_TEAM_ID))||'队' SALE_TEAM_NAME
        ,t2.cust_name ADMINISTRATOR
        ,TEAM_SIZE
        ,t3.cust_name TEAM_LEADER
        ,LEADER_CONTACT
	from (
		select 
		         REGION
		        ,COUNTY_CODE
		        ,SCHMARKER
		        ,row_number()over( order by ORG_ID)  SALE_TEAM_ID
		        --,ORG_ID 
		        --,'校园直销队'||ORG_ID SALE_TEAM_NAME
		        --, ADMINISTRATOR
		        ,int(rand(8)*100) TEAM_SIZE
		        --,'--' TEAM_LEADER
		        , '13'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4+1))))||'89'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4567+3212)))) LEADER_CONTACT
		 from DWD_NG2_M99906_201011 a
		 ) t 
		 join ( select cust_name ,row_number()over( order by cust_name ) cust_id 
from (select distinct trim(cust_name) cust_name from  dw_cust_201005 ) t
fetch first 1000 rows only ) t2 on t.SALE_TEAM_ID + 10 = t2.cust_id 
		 join ( select cust_name ,row_number()over( order by cust_name ) cust_id 
from (select distinct trim(cust_name) cust_name from  dw_cust_201005 ) t
fetch first 1000 rows only ) t3 on t.SALE_TEAM_ID + 100= t3.cust_id 




( select cust_name ,row_number()over( order by cust_name ) cust_id 
from (select distinct trim(cust_name) cust_name from  dw_cust_201005 ) t
fetch first 100 rows only ) t2

	select 
	     REGION
        ,COUNTY_CODE
        ,SCHMARKER
        ,SALE_TEAM_ID
        ,'校园直销'||trim(char(SALE_TEAM_ID))||'队' SALE_TEAM_NAME
        ,ADMINISTRATOR
        ,TEAM_SIZE
        ,TEAM_LEADER
        ,LEADER_CONTACT
        ,OP_TIME 
	from (
		select 
		         REGION
		        ,COUNTY_CODE
		        ,SCHMARKER
		        ,row_number()over( order by ORG_ID)  SALE_TEAM_ID
		        --,ORG_ID 
		        --,'校园直销队'||ORG_ID SALE_TEAM_NAME
		        ,'--' ADMINISTRATOR
		        ,int(rand(5)*8)+3 TEAM_SIZE
		        ,'--' TEAM_LEADER
		        , '13'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4+1))))||'89'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4567+3212)))) LEADER_CONTACT
		 from DWD_NG2_M99906_201011 a
		 ) t 

		select 
		         REGION
		        ,COUNTY_CODE
		        ,SCHMARKER
		        ,row_number()over( order by ORG_ID)  SALE_TEAM_ID
		        --,ORG_ID 
		        ,'校园直销队'||ORG_ID SALE_TEAM_NAME
		        ,'--' ADMINISTRATOR
		        ,int(rand(8)*100) TEAM_SIZE
		        ,'--' TEAM_LEADER
		        , '13'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4+1))))||'89'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4567+3212)))) LEADER_CONTACT
		 from DWD_NG2_M99906_201011 a
         
select * from DWD_NG2_M99916_201011 order by 4



select 'xxxxxx',count(*) from bass2.DWD_NG2_I03036_20101020

Select 'xxxxxx',substr(control_code,7,5) from APP.SCH_CONTROL_RUNLOG where flag=0 and control_code like 'TR1_L_0%'


insert into ETL_LOAD_TABLE_MAP_MTEST
select distinct  * from ETL_LOAD_TABLE_MAP where upper(table_name_templet) like '%DWD_NG2%'

select * from ETL_LOAD_TABLE_MAP_MTEST

CREATE TABLE "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST"  (
                  "TASK_ID" VARCHAR(10) NOT NULL , 
                  "TABLE_NAME_TEMPLET" VARCHAR(100) NOT NULL , 
                  "TASK_NAME" VARCHAR(100) , 
                  "LOAD_METHOD" SMALLINT NOT NULL WITH DEFAULT 0 , 
                  "BOSS_TABLE_NAME" VARCHAR(100) )   
                 DISTRIBUTE BY HASH("TASK_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
                   
                   

select * from 
select * from DWD_NG2_M99916_201011

		select 
         SCHOOL_ID SCHMARKER
        ,SCHOOL_NAME SCHNAME
        ,CITY_ID REGION
        ,SCH_SIZE_STUS STUDENT_NUM
        ,SCHOOL_ADDRESS SCHADDRESS
        ,1 PARENT_SCHOOL_IND
        ,int(rand(1)*5) MARKETING_AREA
        ,ENTERPRISE_ID GROUPCODE
        ,int(rand(1)*5) ADMIN_DEPT
        ,SCH_SIZE_TEAS EMPLOYEE_NUM
        ,SCHOOL_TYPE RUN_TYPE
        ,0 OP_TIME
       from  xzbass2.Dim_xysc_maintenance_info 
       
       
select * from DWD_NG2_M99999

select * from DWD_NG2_M99914_201011

select SCHOOL_ID,max(SCH_SIZE_TEAS) cnt from  xzbass2.Dim_xysc_maintenance_info 
		        group by SCHOOL_ID
                
select count(0) from dwd_ng2_m99904_201011



alter table DWD_NG2_M99903_YYYYMM add column op_time bigint

select 'alter table '||tabname||' add column op_time bigint' from syscat.tables where lower(tabname) like '%dwd_ng2_%999%yyymm%'
drop  NICKNAME XZBASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT
FOR DB25.BASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT

db2 "export to /bassapp/bass2/panzw2/data/XZBASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT.TXT of del \
MODIFIED BY nochardel coldel$ \
select * from XZBASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT"

select  
a.SCHMARKER ,a.groupcode ,a.SCHNAME
,d.product_no,e.cust_name,e.sex_id,1 operator ,d.brand_id,1 work_dept
,1 grade , e.home_address , e.iden_nbr , e.education_id , year(d.valid_date) valid_date , 1 position , e.email
from   session.t_dwd_ng2_M99904_2 a
join XZBASS2.dw_product_201011 d 		on a.user_id = d.user_id 
join XZBASS2.dw_cust_201011 e       on d.cust_id = e.CUST_ID  



select roamtype_id , count(0) , count(distinct roamtype_id ) from xzbass2.dw_call_cell_201011 group by  roamtype_id order by 1 

select roamtype_id , count(0) , count(distinct roamtype_id ) from xzbass2.call_cell_201011 group by  roamtype_id order by 1 

select SCHOOL_ID,max(SCH_SIZE_teaS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID 

89210012
select roamtype_id , count(0) , count(distinct roamtype_id ) from xzbass2.dw_product_201011 group by  roamtype_id order by 1 


select roamtype_id , count(0) , count(distinct roamtype_id ) from xzbasss2.dw_product_201011 group by  roamtype_id order by 1         
 XZBASS2.dw_product_201011 
        
drop table tmp_pzw_M99904
create table tmp_pzw_M99904
(
        USER_ID                 VARCHAR(20)         
        ,SCHMARKER               VARCHAR(32)         
        ,GROUPCODE               VARCHAR(20)         
        ,SCHNAME                 VARCHAR(200)            
)
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   (USER_ID)				
 USING HASHING
insert into tmp_pzw_M99904
select a.user_id ,b.SCHMARKER ,max(b.groupcode) groupcode ,b.SCHNAME
        --, row_number()over(partition by b.SCHMARKER order by a.user_id ) rn
        from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
        join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
        group by user_id,b.SCHMARKER  ,b.SCHNAME
        --having count(0) >= 3



select count(0),count(distinct user_id) from tmp_pzw_M99904



select a.*,row_number()over(partition by a.SCHMARKER order by a.user_id ) rn
from tmp_pzw_M99904 a


select scho

  
select  
a.SCHMARKER ,a.groupcode ,a.SCHNAME
,d.product_no,e.cust_name,e.sex_id,1 operator ,d.brand_id,1 work_dept
,1 grade , e.home_address , e.iden_nbr , e.education_id , year(d.valid_date) valid_date , 1 position , e.email
--,a.user_id,a.lac_id,a.cell_id
from   XZBASS2.dw_product_201011 d 
join XZBASS2.dw_cust_201011 e      on d.cust_id = e.CUST_ID  
join (select a.*,row_number()over(partition by a.SCHMARKER order by a.user_id ) rn
from tmp_pzw_M99904 a) a on  d.user_id = a.user_id 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  a.SCHMARKER = c.SCHOOL_ID 
where  a.rn <= c.cnt  


select  
a.SCHMARKER ,c.cnt,count(0)
from   XZBASS2.dw_product_201011 d 
join XZBASS2.dw_cust_201011 e      on d.cust_id = e.CUST_ID  
join (select a.*,row_number()over(partition by a.SCHMARKER order by a.user_id ) rn
from tmp_pzw_M99904 a) a on  d.user_id = a.user_id 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  a.SCHMARKER = c.SCHOOL_ID 
where  a.rn <= c.cnt  
group by a.SCHMARKER ,c.cnt



drop table tmp_pzw_M99904
CREATE TABLE tmp_pzw_M99904(
   USER_ID VARCHAR(20),
   SCHMARKER VARCHAR(32),
   GROUPCODE VARCHAR(20),
   SCHNAME VARCHAR(200) 
) DATA CAPTURE NONE				 
 IN TBS_3H				 INDEX 
 IN TBS_INDEX				 PARTITIONING KEY(
    USER_ID
 )			 USING HASHING
insert into tmp_pzw_M99904
select a.user_id ,b.SCHMARKER ,max(b.groupcode) groupcode ,b.SCHNAME
        --, row_number()over(partition by b.SCHMARKER order by a.user_id ) rn
        from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.plan_id 
        in (89140007,89150010)) a
        join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
        group by user_id,b.SCHMARKER  ,b.SCHNAME


update  tmp_pzw_M99904 set school_id = '89189100000002'
where   school_id = '89189100000002'      

select count(0),count(distinct user_id)
from 
(
select t1.*,row_number()over(partition by t1.SCHMARKER order by user_id ) rn2
from 
    (
    select a.*,row_number()over(partition by user_id order by a.user_id ) rn1
    from tmp_pzw_M99904 a
    ) t1 where rn1 = 1 
) t2 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  t2.SCHMARKER = c.SCHOOL_ID 
where   t2.rn2 <= c.cnt       



select count(0) from tmp_pzw_M99904_2

        

select a.*,row_number()over(partition by a.SCHMARKER order by a.user_id ) rn1
from tmp_pzw_M99904 a 

select t2.*
from 
(
select t1.*,row_number()over(partition by t1.SCHMARKER order by user_id ) rn2
from 
    (
    select a.*,row_number()over(partition by user_id order by a.user_id ) rn1
    from tmp_pzw_M99904 a
    ) t1 where rn1 = 1 
) t2 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  t2.SCHMARKER = c.SCHOOL_ID 
where   t2.rn2 <= c.cnt       


select a.
(select a.*,row_number()over(partition by a.SCHMARKER order by a.user_id ) rn
from tmp_pzw_M99904 a) a on  d.user_id = a.user_id 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  a.SCHMARKER = c.SCHOOL_ID 
where  a.rn <= c.cnt  




select c.school_id,cnt,count(0) c2
from 
(
select t1.*,row_number()over(partition by t1.SCHMARKER order by user_id ) rn2
from 
    (
    select a.*,row_number()over(partition by user_id order by a.user_id ) rn1
    from tmp_pzw_M99904 a
    ) t1 where rn1 = 1 
) t2 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  t2.SCHMARKER = c.SCHOOL_ID 
where   t2.rn2 <= c.cnt       
group by c.school_id,cnt


  
select  
a.SCHMARKER ,a.groupcode ,a.SCHNAME
,d.product_no,e.cust_name,e.sex_id,1 operator ,d.brand_id,1 work_dept
,1 grade , e.home_address , e.iden_nbr , e.education_id , year(d.valid_date) valid_date , 1 position , e.email
,a.user_id,a.lac_id,a.cell_id
from   XZBASS2.dw_product_201011 d 
join XZBASS2.dw_cust_201011 e      on d.cust_id = e.CUST_ID  
join (
select a.user_id ,lac_id,cell_id,b.SCHMARKER ,b.groupcode,b.SCHNAME, row_number()over(partition by b.SCHMARKER order by a.user_id ) rn
from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
--group by user_id having count(0) >= 3
) a on  d.user_id = a.user_id 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  a.SCHMARKER = c.SCHOOL_ID 
where  a.rn <= c.cnt  



select count(0) from  XZBASS2.dw_cust_201011
select count(0) from XZBASS2.dw_product_201011

    select BRAND_ID,CRM_BRAND_ID1,CRM_BRAND_ID2,CRM_BRAND_ID3
    ,count(0)
    from dw_product_201011
    group by BRAND_ID,CRM_BRAND_ID1,CRM_BRAND_ID2,CRM_BRAND_ID3
    order by 1,2,3

		select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
		,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
		from (select user_id  , a.cell_id 
		from XZBASS2.dw_call_cell_201011 a 
		where a.lac_id = '8940' 
		and a.cell_id in ('27ED','27F7','2739')
		group by user_id ,a.cell_id  having count(0) >= 10) a 
		join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode 
		join DWD_NG2_M99906_201011 c on b.SCHMARKER = c.SCHMARKER 
		join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
		join XZBASS2.dw_cust_201011 e on d.cust_id = e.CUST_ID
        
SELECT COUNT(0),COUNT(DISTINCT SCHMARKER),COUNT(DISTINCT GROUPCODE) FROM DWD_NG2_M99906_201011
        
SELECT COUNT(DISTINCT CELLCODE) FROM DWD_NG2_M99905_201011
        
SELECT BRAND_ID , COUNT(0)
FROM bass
group by BRAND_ID
order by 1

select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
group by SCHOOL_ID


select roamtype_id ,count(0)
from XZBASS2.dw_call_cell_201011 a
where city_id = '891'
group by roamtype_id 
order by 1


create index ind

select count(0) 
from (
select user_id
from (select user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
group by user_id having count(0) >= 3
) a 

select t1.*
from 
(
select a.user_id , b.SCHMARKER , row_number()over(partition by a.user_id order by a.user_id ) rn
from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
--group by user_id having count(0) >= 3
) t1 
join (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
group by SCHOOL_ID
) t2 on t1.SCHMARKER = t2.SCHOOL_ID
where t1.rn <= t2.cnt 


select t2.school_id,t2.cnt,count(0)
from 
(
select a.user_id , b.SCHMARKER , row_number()over(partition by b.SCHMARKER order by a.user_id ) rn
from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
--group by user_id having count(0) >= 3
) t1 
join (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
group by SCHOOL_ID
) t2 on t1.SCHMARKER = t2.SCHOOL_ID
where t1.rn <= t2.cnt 
group by t2.school_id,t2.cnt

		select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
		,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
		from (select user_id  , a.cell_id 
		from XZBASS2.dw_call_cell_$yyyymm a 
		where a.lac_id = '8940' 
		and a.cell_id in ('27ED','27F7','2739')
		group by user_id ,a.cell_id  having count(0) >= 10) a 
		join DWD_NG2_M99905_$yyyymm b on a.cell_id = b.cellcode 
		join DWD_NG2_M99906_$yyyymm c on b.SCHMARKER = c.SCHMARKER 
		join XZBASS2.dw_product_$yyyymm d on a.user_id = d.user_id 
		join XZBASS2.dw_cust_$yyyymm e on d.cust_id = e.CUST_ID
        
select  
a.SCHMARKER ,a.groupcode ,a.SCHNAME
--,d.product_no,e.cust_name,e.sex_id,1 operator ,d.brand_id,1 work_dept
--,1 grade , e.home_address , e.iden_nbr , e.education_id , year(d.valid_date) valid_date , 1 position , e.email
--,a.user_id,a.lac_id,a.cell_id
from (
select a.user_id ,lac_id,cell_id,b.SCHMARKER ,b.groupcode,b.SCHNAME, row_number()over(partition by b.SCHMARKER order by a.user_id ) rn
from (select a.user_id,lac_id,cell_id from  XZBASS2.dw_call_cell_201011  a where a.roamtype_id = 1 ) a
join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode and a.lac_id = b.LACCODE
--group by user_id having count(0) >= 3
) a
, (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c 
--, XZBASS2.dw_product_201011 d 
--, XZBASS2.dw_cust_201011 e           
where
a.SCHMARKER = c.SCHOOL_ID 
and  a.rn <= c.cnt  
--and  a.user_id = d.user_id 
--and  d.cust_id = e.CUST_ID  
  


select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
group by SCHOOL_ID

select 

 --DROP TABLE DWD_NG2_M99905_201011				
CREATE TABLE DWD_NG2_M99905_201011 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称   
        ,GROUPCODE			varchar(20)     
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING
select * from DWD_NG2_P99999
select * from DWD_NG2_P99999

select count(0),school_id,school_name from xzbass2.Dim_xysc_maintenance_info
group by school_id,school_name

select count(0),school_id from xzbass2.Dim_xysc_maintenance_info
group by school_id

select  lac_id ,cell_id from xzbass2.Dim_xysc_maintenance_info


select count(0),count(distinct LACCODE||CELLCODE) from DWD_NG2_M99905_201011

select count(0) from XZBASS2.dw_call_cell_201011 


select count(0) from 
select  user_id
from XZBASS2.dw_call_cell_201011  a 
where exists (select 1 from  DWD_NG2_M99905_201011 b where  a.lac_id = b.laccode and a.cell_id = b.cellcode)
and roamtype_id = 0
group by user_id having count(0) >= 10


select decimal('27ED') from dual

select hex(cellcode) from DWD_NG2_M99905_201011

select * 
SELECT TIMESTAMP( DATE('2010-01-25'), TIME('17.12.30'))
FROM SYSIBM.SYSDUMMY1

substr(replace(replace(char(value(c.VALID_DATE,TIMESTAMP( DATE('2010-01-25'), TIME('17.12.30')))),'-',''),'.',''),1,14)
substr(replace(replace(char(value(c.EXPIRE_DATE,TIMESTAMP( DATE('2999-12-25'), TIME('17.12.30')))),'-',''),'.',''),1,14)


select * from DWD_NG2_M99905_201011

		select distinct  
				a.LAC_ID
				,a.CELL_ID
                ,hex(int(a.LAC_ID))
                                ,substr(char(hex(int(a.LAC_ID))),5)
		from  xzbass2.Dim_xysc_maintenance_info a 
		join  DWD_NG2_P99999 b on a.org_name = b.organize_name 
		join  xzbass2.ODS_DIM_SYS_ORGANIZE_20101215 c on b.organize_id = c.organize_id
order by 1,2
        
        
select * from app.sch_control_before where control_code like '%w_newbusi_taste_staff_config_ds%'
select * from syscat.tables where lower(tabname) like '%dw_newbusi_taste_staff_config%'
order by 2

select count(0) from DW_NEWBUSI_TASTE_STAFF_CONFIG_201011
select * from dw_newbusi_taste_staff_config_ds

14 - 265

select * from BASS2.DW_NEWBUSI_TASTE_STAFF_CONFIG_DS

error line:1002
error info:[IBM][CLI Driver][DB2/SUN64] SQL0601N  The name of the object to be created is identical to the existing name "BASS2.DW_NEWBUSI_TASTE_STAFF_CONFIG_DS" of type "TABLE".  SQLSTATE=42710

SELECT TIME_ID,COUNT(0) FROM DWD_40001_DM_201012 GROUP BY TIME_ID ORDER BY 1

SELECT * FROM APP.SCH_CONTROL_RUNLOG WHERE lower(CONTROL_CODE) LIKE '%w_newbusi_taste_staff_config_ds%'

select * from app.sch_control_task WHERE lower(CONTROL_CODE) LIKE '%w_newbusi_taste_staff_config_ds%'

select * from dw_newbusi_taste_staff_config_201011

 select * from app.sch_control_alarm
 where lower(control_code) like '%w_newbusi_taste_staff_config%' 

 select * from app.sch_control_alarm
 where control_code like '%w_newbusi_taste_staff_config_ds.tcl%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%I14108%'
 )
 and alarmtime >=  timestamp('20101201'||'000000') 
 order by alarmtime desc
 
SELECT DWD_NG2_M99906_
create nickname xzbass2.ODS_DIM_SYS_ORGANIZE_20101215 
for db25.bass2.ODS_DIM_SYS_ORGANIZE_20101215

SELECT count(distinct school_id),count(distinct enterprise_id) FROM  xzbass2.Dim_xysc_maintenance_info

SELECT distinct school_id,enterprise_id FROM  xzbass2.Dim_xysc_maintenance_info
select 0.5+round(rand(0)*0.5,2) from DWD_NG2_M99906_201011

select distinct  a.city_id 
		,c.COUNTY_ID
		,a.SCHOOL_ID
		,b.organize_id
		,a.ORG_TYPE
        ,value(a.enterprise_id,'未签约')
from  xzbass2.Dim_xysc_maintenance_info a 
join  DWD_NG2_P99999 b on a.org_name = b.organize_name 
join  xzbass2.ODS_DIM_SYS_ORGANIZE_20101215 c on b.organize_id = c.organize_id

select * from DWD_NG2_M99906_201011


select  a.*
from  xzbass2.Dim_xysc_maintenance_info a where a.org_name is not null and a.org_name not like '%测试%'
join DWD_NG2_P99999 b on a.org_name = b.organize_name 

SELECT * FROM DWD_NG2_P99999

--DROP TABLE DWD_NG2_P99999		
CREATE TABLE DWD_NG2_P99999 (		
organize_id			bigint
,organize_name		varchar(200)
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( organize_id )
 USING HASHING
select * from ods_dim_sys_organize_20101215 where organize_name like '%八一路指定专营店%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%东方%粮%%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%福源商%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%河坝林%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%隆旺%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%圣翔%警%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%手机维修%'
select * from ods_dim_sys_organize_20101215 where organize_name like '%小卖部%'

select * from ODS_DIM_SYS_ORGANIZE_20101201
select * from syscat.tables where tabname like '%ODS_DIM_SYS_ORGANIZE%'

select distinct organize_id,organize_name ,b.org_name from ODS_DIM_SYS_ORGANIZE_20101215 a ,
 (select distinct  org_name org_name from Dim_xysc_maintenance_info where org_name is not null) b
 where a.organize_name = b.org_name
 or organize_id in (91000777,91000788,10850052,91000985,91000525,91000715,91000437)
 

select distinct  org_name from Dim_xysc_maintenance_info where org_name is not null
except 
select organize_name from ODS_DIM_SYS_ORGANIZE_20101215 a 
where a.organize_name in 
 (select distinct  org_name from Dim_xysc_maintenance_info where org_name is not null) 

1



IP





select * from Dim_xysc_maintenance_info where org_name like '%东方粮行代理店%'

select * from ODS_DIM_SYS_ORGANIZE_20101215 where organize_name like '%八一路指定专营店%'

select * from Dim_xysc_lac_cell_info

select * from dim_xysc_org_type

select * from dim_xysc_school_type

select * from dim_xysc_school_level

select * from dim_xysc_cell_type

select * from dim_xysc_cell_nettype

select * from  bass2.Dim_xysc_lac_cell_info

select * from Dim_xysc_maintenance_info where org_name is not null


/**	2010-12-16 11:39	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99999_201011				
CREATE TABLE DWD_NG2_M99999_201011 (				
        SCHNAME             VARCHAR(200)        --学校名称
        ,ORG_ID             VARCHAR(20)         --渠道标识
        ,ORG_NAME           VARCHAR(200)        --渠道名称
        ,ORG_TYPE           VARCHAR(8)          --渠道类型-1:移动、2:电信、3:联通
        ,SCHMARKER          VARCHAR(32)         --学校标识-省代码+地市代码+8位序号
        ,REGION             VARCHAR(8)          --地市标识
        ,GROUPCODE          VARCHAR(32)         --集团编码
        ,SCHKIND            VARCHAR(8)          --学校类别-1:本科
        ,SCHLEVEL           VARCHAR(8)          --学校级别-1:国家教育部
        ,SCHADDRESS         VARCHAR(200)        --学校地址
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,SCHSCALE           VARCHAR(32)         --规模-学生人数、教职工人数
        ,LACNAME            VARCHAR(200)        --位置名称
        ,LACCODE            VARCHAR(64)         --位置代码
        ,CELLCODE           VARCHAR(64)         --小区代码
        ,CELLLAT            VARCHAR(32)         --小区纬度
        ,CELLLONG           VARCHAR(32)         --小区经度
        ,CELLNETTYPE        VARCHAR(32)         --小区网络类型
        ,ATTACHCORP         VARCHAR(200)        --附属分公司
        ,CELLTYPE           VARCHAR(32)         --小区类型-1:学校、2:农村、3:县城等
        ,STUDENT_NUM        INTEGER             --学生人数
        ,EMPLOYEE_NUM       INTEGER             --教职工人数
        ,COVERRATE               DECIMAL(10)   	--覆盖率     
        ,EFF_DATE                TIMESTAMP       --生效日期 
        ,EXP_DATE                TIMESTAMP       --失效日期  
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SCHMARKER )
 USING HASHING
select * from dw_cust_20101202 where cust_name like '%学院%'

89402999773637
89401560001792
89401560001792
select * from dw_cust_20101202 where cust_id = '89401560001792'


insert into etl_load_table_map values('M99999','DWD_NG2_M99999_YYYYMM','校园市场信息表',0,'SO.SCHOOL_MARKET_DATE')
select * from DWD_NM_BSC_CAPABILITY_20101210
select * from etl_load_table_map where task_id like '%999%'


select * from app.sch_control_task where function_desc like '%基站%'

select * from DWD_NG2_I08112_20101020

select * from DWD_NG2_M99905_201011


select 
         REGION
        ,COUNTY_CODE
        ,SCHMARKER
        ,ORG_ID SALE_TEAM_ID
        ,'校园直销队'||ORG_ID SALE_TEAM_NAME
        ,'--' ADMINISTRATOR
        ,int(rand(8)*100) TEAM_SIZE
        ,'--' TEAM_LEADER
        , '13'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4+1))))||'89'||rtrim(ltrim(char(int(rand(1)*4+6))))||rtrim(ltrim(char(int(rand(1)*4567+3212)))) LEADER_CONTACT
 from DWD_NG2_M99906_201011 a
 

/**	2010-12-16 9:18	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99916_201011				
CREATE TABLE DWD_NG2_M99916_yyyymm (				
        REGION              VARCHAR(8)          --地市标识
        ,COUNTY_CODE        VARCHAR(8)          --区县标识
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SALE_TEAM_ID       VARCHAR(200)        --校园直销队标识
        ,SALE_TEAM_NAME     VARCHAR(100)          --校园直销队名称
        ,ADMINISTRATOR      VARCHAR(32)         --校园直销队负责人
        ,TEAM_SIZE          INTEGER             --校园直销队团队规模
        ,TEAM_LEADER        VARCHAR(32)         --直销队队长姓名
        ,LEADER_CONTACT     VARCHAR(32)         --直销队队长联系方式
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SCHMARKER )
 USING HASHING
INSERT INTO etl_load_table_map 
VALUES(
   'M99916',
   'DWD_NG2_M99916_YYYYMM',
   '校园直销队信息表',
   0,
   'SO.SCHOOL_MARKET_DATE'
   )
select * from DWD_NG2_M99914_201011

insert into etl_load_table_map values('M99914','DWD_NG2_M99914_YYYYMM','教职工信息表',0,'SO.SCHOOL_MARKET_DATE')
select count(0) from DWD_NG2_M99904_201011
select * from DWD_NG2_M99914_201011



select * from DWD_NG2_M99913_201011


insert into etl_load_table_map values('M99913','DWD_NG2_M99913_YYYYMM','学校管理层信息表',0,'SO.SCHOOL_MARKET_DATE')
/**	2010-12-15 17:07	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_M99913_YYYYMM				
CREATE TABLE DWD_NG2_M99913_YYYYMM(
   SCHMARKER VARCHAR(32) --学校标识
   ,
   SCHNAME VARCHAR(200) --学校名称
   ,
   ADMIN_NAME VARCHAR(200) --学校管理层人员姓名
   ,
   PRODUCT_NO VARCHAR(15) --学校管理层人员手机号码
   ,
   OPERATOR SMALLINT --运营商归属及品牌
   ,
   POSITION SMALLINT --管理层人员级别
   ,
   ADMIN_DEPT VARCHAR(120) --主管部门名称
   ,
   NOTES VARCHAR(255) --说明
)			 DATA CAPTURE NONE				 
 IN TBS_3H				 INDEX 
 IN TBS_INDEX				 PARTITIONING KEY(
    SCHMARKER,
    PRODUCT_NO 
 )			 USING HASHING
SCHMARKER
        ,GROUPCODE
        ,SCHNAME
        ,CUST_NAME
        ,SEX_ID
        ,WORK_DEPT
        ,POSITION
        ,PRODUCT_NO
        ,OPERATOR
        ,BRAND_ID
        ,LINK_PHONE
        ,EMAIL
        
select *
from (        
select 
         SCHMARKER
        ,SCHNAME
        ,CUST_NAME ADMIN_NAME
        ,PRODUCT_NO
        ,OPERATOR
        ,POSITION
        ,WORK_DEPT ADMIN_DEPT
        ,'-' NOTES
        ,row_number()over(partition by SCHMARKER order by PRODUCT_NO ) rn 
from DWD_NG2_M99914_201011
) t where rn <= 5

		fetch first 20 rows only

SCHMARKER	VARCHAR(32)	
SCHNAME	VARCHAR(200)	
ADMIN_NAME	VARCHAR(200)	
PRODUCT_NO	VARCHAR(15)	
OPERATOR	smallint	
POSITION	SMALLINT	
ADMIN_DEPT	VARCHAR(120)	
NOTES	VARCHAR(255)	




select * from DWD_NG2_M99903_201011


insert into etl_load_table_map values('M99903','DWD_NG2_M99903_YYYYMM','学校基本信息表',0,'SO.SCHOOL_MARKET_DATE')
select * from DWD_NG2_M99903_201011

--DROP TABLE DWD_NG2_M99903_YYYYMM				
CREATE TABLE DWD_NG2_M99903_YYYYMM (				
        SCHMARKER           VARCHAR(32)         --学校标识
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,REGION             VARCHAR(8)          --归属地市
        ,STUDENT_NUM        INTEGER             --学生人数
        ,SCHADDRESS         VARCHAR(200)        --学校地址
        ,PARENT_SCHOOL_IND  INTEGER             --上级学校标识
        ,MARKETING_AREA     INTEGER             --归属营销区域
        ,GROUPCODE          VARCHAR(32)         --集团编码
        ,ADMIN_DEPT         VARCHAR(120)        --主管部门
        ,EMPLOYEE_NUM       INTEGER             --教职工人数
        ,RUN_TYPE           VARCHAR(8)          --办校属性
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SCHMARKER )
 USING HASHING
select * from DWD_NG2_M99904_201011


                select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
                ,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
                from (select user_id  , a.cell_id 
                from XZBASS2.dw_call_cell_201011 a 
                where a.lac_id = '8940' 
                and a.cell_id in ('27ED','27F7','2739')
                group by user_id ,a.cell_id  having count(0) >= 10) a 
                join DWD_NG2_M99905_201011 b on a.cell_id = b.cellcode 
                join DWD_NG2_M99906_201011 c on b.SCHMARKER = c.SCHMARKER 
                join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
                join XZBASS2.dw_cust_201011 e on d.cust_id = e.CUST_ID
                
SCHMARKER	VARCHAR(32)	
SCHNAME	VARCHAR(200)	
CUST_NAME	VARCHAR(200)	
SEX_ID	SMALLINT	
WORK_DEPT	VARCHAR(120)	
POSITION	SMALLINT	
PRODUCT_NO	VARCHAR(15)	
OPERATOR	smallint	
BRAND_ID	BIGINT(8)	
LINK_PHONE	VARCHAR(32)	
EMAIL	VARCHAR(64)	
CREATE TABLE DWD_NG2_M99914_yyyymm (				
        SCHMARKER           VARCHAR(32)         --学校标识
        ,GROUPCODE	VARCHAR(32)
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,CUST_NAME          VARCHAR(200)        --教职人员姓名
        ,SEX_ID             SMALLINT                --性别
        ,WORK_DEPT          VARCHAR(120)            --部门
        ,POSITION           SMALLINT                --职位
        ,PRODUCT_NO         VARCHAR(15)         --手机号码
        ,OPERATOR           SMALLINT            --运营商归属
        ,BRAND_ID           BIGINT             --品牌
        ,LINK_PHONE         VARCHAR(32)         --其它联系电话
        ,EMAIL              VARCHAR(64)         --邮箱地址
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( PRODUCT_NO,GROUPCODE )
 USING HASHING
--DROP TABLE DWD_NG2_M99914_YYYYMM				
CREATE TABLE DWD_NG2_M99914_YYYYMM(
   SCHMARKER VARCHAR(32) --学校标识
   ,
   SCHNAME VARCHAR(200) --学校名称
   ,
   CUST_NAME VARCHAR(200) --教职人员姓名
   ,
   SEX_ID SMALLINT --性别
   ,
   WORK_DEPT VARCHAR(120) --部门
   ,
   POSITION SMALLINT --职位
   ,
   PRODUCT_NO VARCHAR(15) --手机号码
   ,
   OPERATOR SMALLINT --运营商归属
   ,
   BRAND_ID BIGINT --品牌
   ,
   LINK_PHONE VARCHAR(32) --其它联系电话
   ,
   EMAIL VARCHAR(64) --邮箱地址
) DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    PRODUCT_NO 
 ) USING HASHING
select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,e.CUST_NAME,e.SEX_ID,1 WORK_DEPT
 ,1 POSITION,d.product_no,1 OPERATOR,d.BRAND_ID,e.LINK_PHONE,E.EMAIL
from (select user_id  , a.cell_id 
from XZBASS2.dw_call_cell_201011 a 
where a.lac_id = '8940' 
and a.cell_id in ('27ED','27F7')
group by user_id ,a.cell_id  having count(0) = 8) a 
join DWD_NG2_P99905_20101130 b on a.cell_id = b.cellcode 
join DWD_NG2_P99906_20101130 c on b.SCHMARKER = c.SCHMARKER 
join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
join XZBASS2.dw_cust_201011 e on d.cust_id = e.CUST_ID
select * from DWD_NG2_M99904_201011

delete from etl_load_table_map where task_id = 'M99904'

select int(rand(3)*10) from XZBASS2.dw_cust_201011 

select * from DWD_NG2_M99904_YYYYMM

--insert into etl_load_table_map values('M99904','DWD_NG2_M99904_YYYYMM','学生信息表',0,'SO.SCHOOL_MARKET_DATE')	
drop table DWD_NG2_M99904_yyyymm
CREATE TABLE DWD_NG2_M99904_yyyymm (				
        SCHMARKER           VARCHAR(32)         --学校标识
        ,GROUPCODE          VARCHAR(32)         --集团编码
        ,SCHNAME            VARCHAR(200)        --学校名称
        ,PRODUCT_NO         VARCHAR(15)         --手机号码
        ,CUST_NAME          VARCHAR(200)        --学生姓名
        ,SEX_ID             SMALLINT            --学生性别
        ,OPERATOR           SMALLINT            --运营商归属
        ,BRAND_ID           BIGINT                  --品牌
        ,WORK_DEPT          VARCHAR(120)            --院系
        ,GRADE              SMALLINT                --班级
        ,HOME_ADDRESS       VARCHAR(200)            --住址
        ,IDEN_NBR           VARCHAR(40)         --身份证号
        ,EDUCATION_ID       SMALLINT                --学历
        ,VALID_DATE         integer           --入学年份
        ,POSITION           SMALLINT                --职务
        ,EMAIL              VARCHAR(64)         --电子邮件地址
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( PRODUCT_NO,GROUPCODE )
 USING HASHING
select * from syscat.tables where lower(tabname) like '%dw_cust_2010%'

create nickname  XZBASS2.dw_cust_201011  for db25.bass2.dw_cust_201011 
select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
from (select user_id  , a.cell_id 
from XZBASS2.dw_call_cell_201011 a 
where a.lac_id = '8940' 
and a.cell_id in ('27ED','27F7')
group by user_id ,a.cell_id  having count(0) >= 10) a 
join DWD_NG2_P99905_20101130 b on a.cell_id = b.cellcode 
join DWD_NG2_P99906_20101130 c on b.SCHMARKER = c.SCHMARKER 
join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
join XZBASS2.dw_cust_201011 e on d.cust_id = e.CUST_ID
union 
select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
from (select user_id  , a.cell_id 
from XZBASS2.dw_call_cell_201011 a 
where a.lac_id = '8940' 
and a.cell_id in ('2739')
group by user_id ,a.cell_id  having count(0) >= 10) a 
join DWD_NG2_P99905_20101130 b on a.cell_id = b.cellcode 
join DWD_NG2_P99906_20101130 c on b.SCHMARKER = c.SCHMARKER 
join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
join XZBASS2.dw_cust_201011 e on d.cust_id = e.CUST_ID



select * from DWD_NG2_M99906_201011


CREATE TABLE "BASS2   "."DWD_NG2_M99906_YYYYMM"  (
                  "REGION" VARCHAR(8) , 
                  "COUNTY_CODE" VARCHAR(8) , 
                  "SCHMARKER" VARCHAR(32) , 
                  "ORG_ID" VARCHAR(200) , 
                  "ORG_TYPE" VARCHAR(8) , 
                  "GROUPCODE" VARCHAR(32) )   
                 DISTRIBUTE BY HASH("SCHMARKER")   
                   IN "TBS_3H" INDEX IN "TBS_INDEX" 
select * from DWD_NG2_M99905_201011

/**
select * from DWD_NG2_M99912_YYYYMM

select * from DWD_DSMP_mnet_user_201011

select * from DWD_DSMP_sp_company_code_201011

select * from DWD_DSMP_sp_oper_code_201011

drop table DWD_DSMP_MNET_USER_201011
CREATE TABLE "BASS2   "."DWD_DSMP_MNET_USER_201011"  (
                  "MSISDN" VARCHAR(20) , 
                  "REG_MOD_ID" VARCHAR(2) , 
                  "REG_TIME" TIMESTAMP , 
                  "REG_MOD" CHAR(1) , 
                  "DESTROY_TIME" TIMESTAMP , 
                  "MNT_USER_STATU_ID" CHAR(1) , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("MSISDN")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS2   "."DWD_DSMP_MNET_USER_201011" LOCKSIZE TABLE

drop table DWD_DSMP_SP_COMPANY_CODE_201011
CREATE TABLE "BASS2   "."DWD_DSMP_SP_COMPANY_CODE_201011"  (
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_TYPE_ID" VARCHAR(2) , 
                  "SP_NAME" VARCHAR(12) , 
                  "CONNECT_MOD" CHAR(1) , 
                  "STATU_TYPE_ID" CHAR(1) , 
                  "VALID_TIME" DATE , 
                  "SMS_CONNECT_NO" VARCHAR(12) , 
                  "SP_ATTRIBUTE" VARCHAR(2) , 
                  "MISC_ID" VARCHAR(4) , 
                  "ELSE_CONNECT_NO" VARCHAR(12) , 
                  "PROVINCE_ID" VARCHAR(3) , 
                  "MST_CONNECT_ID" CHAR(1) , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("SP_CODE")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS2   "."DWD_DSMP_SP_COMPANY_CODE_201011" LOCKSIZE TABLE

drop table DWD_DSMP_SP_OPER_CODE_201011
CREATE TABLE "BASS2   "."DWD_DSMP_SP_OPER_CODE_201011"  (
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_CODE" VARCHAR(10) , 
                  "SP_OPER_NAME" VARCHAR(64) , 
                  "OPER_KIND_ID" INTEGER , 
                  "SERV_STATU_ID" CHAR(1) , 
                  "SERV_OPER_ID" INTEGER , 
                  "OPER_BEAR_ID" CHAR(1) , 
                  "ON_DEMANDFLAG" CHAR(1) , 
                  "SERV_ATTRIBUTE" CHAR(1) , 
                  "MISC_ID" VARCHAR(4) , 
                  "CONNECT_EPMENT_ID" VARCHAR(10) , 
                  "EPM_KIND_ID" INTEGER , 
                  "WAP_ABET_VER" CHAR(1) , 
                  "BILLING_TYPE_ID" VARCHAR(2) , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("SP_CODE")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS2   "."DWD_DSMP_SP_OPER_CODE_201011" LOCKSIZE TABLE






/**
drop table DWD_DSMP_WAP_MENU_DS_201012
CREATE TABLE "BASS2   "."DWD_DSMP_WAP_MENU_DS_201012"  (
                  "MENU_ID" VARCHAR(8) , 
                  "DOOR_TYPE_ID" INTEGER , 
                  "MENU_NAME" VARCHAR(64) , 
                  "MENU_STATUS_ID" VARCHAR(2) , 
                  "STARTUP_DATE" TIMESTAMP , 
                  "FATHER_MENU_ID" VARCHAR(8) , 
                  "MENU_TYPE_ID" VARCHAR(3) , 
                  "SP_CODE" VARCHAR(12) , 
                  "SP_OPER_CODE" VARCHAR(10) , 
                  "CREATE_DATE" TIMESTAMP , 
                  "ESP_MENU_TYPE_ID" VARCHAR(2) , 
                  "BIG_KIND_ID" INTEGER , 
                  "SMALL_KIND_ID" INTEGER , 
                  "WAP_DOOR_TYPE_ID" INTEGER , 
                  "SUP_MENU_TYPE_ID" VARCHAR(8) , 
                  "MENU_LEVEL" INTEGER , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("MENU_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
                   

drop table DWD_DSMP_EQUIPMENT_DS_201012
CREATE TABLE "BASS2   "."DWD_DSMP_EQUIPMENT_DS_201012"  (
                  "EPM_ID" VARCHAR(10) , 
                  "EPM_NAME" VARCHAR(20) , 
                  "EPM_KIND_ID" INTEGER , 
                  "BILL_DATE" VARCHAR(10) )   
                 DISTRIBUTE BY HASH("EPM_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS2   "."DWD_DSMP_EQUIPMENT_DS_201012" LOCKSIZE TABLE

                   
                   

select count(0) from DWD_DSMP_equipment_DS_201011

select BILL_DATE , count(0) from DWD_DSMP_equipment_DS_201010
group by BILL_DATE

select * from DWD_DSMP_equipment_DS_201010


ls -l *99014*201012* *99008*201012*
cp *99014*201012* *99008*201012* /bassdb1/etl/L/dsmp/

  SELECT TABNAME
   FROM SYSCAT.TABLES
   WHERE TBSPACEID=10 AND TABLEID=23840
   

select * from syscat.tables where tabname like '%VGOP_14303_2010%'

select * from VGOP_14303_20101213

select * from VGOP_15202_20101213

select * from BASS2.DWD_DSMP_EQUIPMENT_DS_201012

select * from BASS2.DWD_DSMP_WAP_MENU_DS_201012



select * from DWD_MGAME_USER_20101213

select * from DWD_MGAME_KPIFEEUSER_20101212

select * from DWD_DSMP_WTBSPUSH_OPER_LOG_DM_201010

select * from DIM_DEAL_RESULT_LKP



select * from app.SCH_CONTROL_TASK where cc_flag = 1 

select * from app.sch_control_task where control_code = 'TR1_L_I43001'
select * from usys_table_maintain

select * from etl_load_table_map where table_name_templet like '%DWD_MGAME_KPIFEEUSER_YYYYMMDD%'

select * from nj_qyq_1214

select * from etl_load_table_map where task_id like '%M999%%'

delete from etl_load_table_map where task_id = 'M99905'

 --DROP TABLE DWD_NG2_M99905_201012				
CREATE TABLE DWD_NG2_P99905_20101130 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称        
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING		
 
 select * from dwd_ng2_M99905_201011
 
 
 

insert into etl_load_table_map values('M99912','DWD_NG2_M99912_YYYYMM','终端配置信息',0,'BASS2.DIM_DEVICE_PROFILE')


select count(0) from DWD_NG2_M99912_201011


drop table DWD_NG2_M99912_20101202

CREATE TABLE "BASS2   "."DWD_NG2_M99912_YYYYMM"  (
                  "TIME_ID" VARCHAR(8) , 
                  "DEVICE_ID" VARCHAR(16) , 
                  "PROPERTY_ID" VARCHAR(16) , 
                  "VALUE" VARCHAR(16) , 
                  "VALUE_DESC" VARCHAR(1024) )   
                 DISTRIBUTE BY HASH("DEVICE_ID",  
                 "PROPERTY_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX"  

		

/**	2010-12-9 17:30	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99905_YYYYMMDD				
CREATE TABLE DWD_NG2_P99905_YYYYMMDD (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING
--DROP TABLE DWD_NG2_P99905_20101202				
CREATE TABLE DWD_NG2_P99905_20101202 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          #VALUE!             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING
select * from DWD_NG2_P99906_20101202

delete from etl_load_table_map  where  task_id = 'P99903'
insert into etl_load_table_map values('P99906','DWD_NG2_P99906_YYYYMMDD','学校渠道信息表',0,'SO.SCHOOL_MARKET_DATE')
/**	2010-12-9 16:06	added by  panzhiwei		**/
--DROP TABLE DWD_NG2_P99903_20101202				
CREATE TABLE DWD_NG2_P99906_yyyymmdd(
   REGION VARCHAR(8) --地市标识
   ,
   COUNTY_CODE VARCHAR(8) --区县标识
   ,
   SCHMARKER VARCHAR(32) --学校标识
   ,
   ORG_ID VARCHAR(200) --营业厅标识
   ,
   ORG_TYPE VARCHAR(8) --营业厅类型标识
   ,
   GROUPCODE	 VARCHAR(32)	 	 	 	 	 --企业代码
) DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    SCHMARKER 
 ) USING HASHING
select * from DWD_NG2_P99902_20101202

insert into etl_load_table_map values('P99902','DWD_NG2_P99902_YYYYMMDD','终端配置信息',0,'BASS2.DIM_DEVICE_PROFILE')
select time_id ,count(0)
from DIM_DEVICE_PROFILE
group by time_id


CREATE TABLE "BASS2   "."DWD_NG2_P99902_20101202"  (
                  "TIME_ID" VARCHAR(8) , 
                  "DEVICE_ID" VARCHAR(16) , 
                  "PROPERTY_ID" VARCHAR(16) , 
                  "VALUE" VARCHAR(16) , 
                  "VALUE_DESC" VARCHAR(1024) )   
                 DISTRIBUTE BY HASH("DEVICE_ID",  
                 "PROPERTY_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" 
select * from syscat.tables  where tabname like '%DIM%'


create table bass2.ETL_LOAD_TABLE_MAP_MTEST
like bass2.ETL_LOAD_TABLE_MAP
in TBS_ODS_OTHER 
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( task_id )
 USING HASHING
create nickname XZBASS2.CDR_ISMG_20101202
 for db25.BASS2.CDR_ISMG_20101202
 
   create nickname XZBASS2.dw_product_20101202
 for db25.BASS2.dw_product_20101202
 
 
 
select * from syscat.tables where tabname like '%ODS_CHANNEL_LOCAL_BUSI%'
select 'drop table '||tabname || '' from syscat.tables where tabname like '%DWD%NG2%%'
AND tabname  like '%20101202%'
order by 1

select	*	from	etl_task_log	
where	task_id	in	('I14108')	



select * from ETL_LOAD_TABLE_MAP where task_id = 'A05022'


select * from ODS_CHANNEL_LOCAL_BUSI_20101202

insert into etl_load_table_map values('I14108','ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD','资源销售信息表',0,'channel.channel_local_busi')
INSERT INTO USYS_TABLE_MAINTAIN 
VALUES(
   10176,
   '资源销售信息表',
   'ODS_CHANNEL_LOCAL_BUSI',
   '1',
   'day',
   255,
   '0',
   '',
   '',
   'BASS2',
   'ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD',
   'TBS_3H',
   'TBS_INDEX',
   'obj_id',
   1
   )
INSERT INTO app.sch_control_task 
VALUES(
   'TR1_L_14108',
   1,
   2,
   'ODS_CHANNEL_LOCAL_BUSI_YYYYMMDD',
   0,
   - 1,
   '资源销售信息表',
   '-',
   'TR1_BOSS',
   2,
   '-'
   )
/**	2010-12-3 10:40	added by  panzhiwei		**/
--DROP TABLE ODS_CHANNEL_LOCAL_BUSI_20101202				
CREATE TABLE ODS_CHANNEL_LOCAL_BUSI_20101203(
   ENTITY_TYPE INTEGER --ENTITY_TYPE
   ,
   OBJ_ID INTEGER --OBJ_ID
   ,
   product_no VARCHAR(20) --product_no
   ,
   city_id VARCHAR(8) --地市
   ,
   county_id BIGINT --县区
   ,
   channel_type_class INTEGER --channel_type_class
   ,
   channel_child_type INTEGER --渠道子类型
   ,
   channel_name VARCHAR(40) --渠道名称
   ,
   card_value BIGINT --card_value
   ,
   channel_id BIGINT --channel_id
   ,
   REC_STATUS SMALLINT --REC_STATUS
   ,
   OP_ID BIGINT --OP_ID
   ,
   ORG_ID BIGINT --ORG_ID
   ,
   DONE_CODE BIGINT --DONE_CODE
   ,
   DONE_DATE TIMESTAMP --DONE_DATE
) DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    obj_id 
 ) USING HASHING
SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN

select * from app.sch_control_before where BEFORE_control_code like '%0502%'

SELECT count(0) FROM Dwd_pm_sp_info_20101203

select count(0) from Dw_newbusi_gprs_gxz_20101203


select * from app.sch_control_task where control_code like '%14108%'









SELECT * FROM SYSCAT.TABLES WHERE TABNAME LIKE '%DWD_PM_SP_INFO%'
 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 and alarmtime >=  timestamp('20101130'||'000000') 
 order by alarmtime desc
 
 
select * from channel.channel_local_busi

/**
drop table ODS_CHANNEL_INFO_20101202
drop table ODS_CHANNEL_INFO_20101203
drop table ODS_CHANNEL_INFO_20101204
drop table ODS_CHANNEL_INFO_20101205
drop table ODS_CHANNEL_INFO_20101206
drop table ODS_CHANNEL_INFO_20101207
drop table ODS_CHANNEL_INFO_20101208
drop table ODS_CHANNEL_INFO_20101209
drop table ODS_CHANNEL_INFO_20101210
drop table ODS_CHANNEL_INFO_20101211
drop table ODS_CHANNEL_INFO_20101212
drop table ODS_CHANNEL_INFO_20101213
drop table ODS_CHANNEL_INFO_20101214
drop table ODS_CHANNEL_INFO_20101215
drop table ODS_CHANNEL_INFO_20101216
drop table ODS_CHANNEL_INFO_20101217
drop table ODS_CHANNEL_INFO_20101218
drop table ODS_CHANNEL_INFO_20101219
drop table ODS_CHANNEL_INFO_20101220
drop table ODS_CHANNEL_INFO_20101221
drop table ODS_CHANNEL_INFO_20101222
drop table ODS_CHANNEL_INFO_20101223
drop table ODS_CHANNEL_INFO_20101224
drop table ODS_CHANNEL_INFO_20101225
drop table ODS_CHANNEL_INFO_20101226
drop table ODS_CHANNEL_INFO_20101227
drop table ODS_CHANNEL_INFO_20101228
drop table ODS_CHANNEL_INFO_20101229
drop table ODS_CHANNEL_INFO_20101230
drop table ODS_CHANNEL_INFO_20101231

**/


select count(0) from ODS_CHANNEL_INFO_20101202

select * from ODS_CHANNEL_INFO_20101201 where shop_number is not null 
select 'drop table '||tabname||'' from syscat.tables  where  tabname like 'ODS_CHANNEL_INFO_201012%'
order by 1

select count(0) from ODS_CHANNEL_INFO_20101201

alter table ODS_CHANNEL_INFO_YYYYMMDD add column shop_number varchar(256)
alter table ODS_CHANNEL_INFO_20101201 add column shop_number varchar(256)


select * from syscat.tables where tabname like '%ODS_CHANNEL_INFO_2010%'
order by tabname



select count(0) from app.SCH_CONTROL_TASK_20101129

select * from app.SCH_CONTROL_NICK


select * from ETL_TASK_RUNNING
select * from ETL_TASK_LOG_XZ48

select * from syscat.tables where tabname like 'USYS_%'
order by 2

select * from ETL_ROLLBACK_TASK_MAP

select * from ETL_SEND_MESSAGE

select count(0) from ETL_TASK_LOG

select * from syscat.tables where tabname like 'ETL%'
order by 2

select * from app.sch_control_task 
where control_code in (
'TR1_L_11034'
,'TR1_L_11035'
,'TR1_L_11036'
,'TR1_L_05022'
)


select * from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101201
select * from app.sch_control_task where control_code = 'TR1_L_11036'


select * from USYS_TABLE_MAINTAIN
where table_id in (10173,
10172,
10174,
10175
)
select * from etl_load_table_map 
 where (
 task_id like '%A11034%'
 or 
 task_id like '%A11035%'
 or 
 task_id like '%M11036%'
 or 
 task_id like '%A05022%'
 or 
 table_name_templet like '%ENTERPRISE_MANAGER_RELA%'
 or 
 table_name_templet like '%CUST_MANAGER_INFO%'
 )
 
 TR1_L_11035
TR1_L_11034
TR1_L_05022



/**
select * from 

 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 and alarmtime >=  timestamp('20101020'||'000000') 
 order by alarmtime desc
 
A05021
--insert into etl_load_table_map values('A05021','ODS_CDR_GPRS_GXZ_YYYYMMDD','GPRS清单（GXZ）',0,'XZJF.DR_GGPRS_GXZ_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10175,'GPRS清单（GXZ）','ODS_CDR_GPRS_GXZ','1','day',255,'0','','','BASS2','ODS_CDR_GPRS_GXZ_YYYYMMDD','TBS_CDR_VOICE','TBS_INDEX','USER_NUMBER,START_TIME',1)
insert into app.sch_control_task values ('TR1_L_05022',1,2,'ODS_CDR_GPRS_GXZ_YYYYMMDD',0,-1,'GPRS清单（GXZ）','-','TR1_BOSS',2,'-')

select count(0),count(distinct table_id) from USYS_TABLE_MAINTAIN
select count(0),count(distinct task_id) from etl_load_table_map
select count(0),count(distinct control_code) from app.sch_control_task

select * from app.sch_control_task where control_code ='TR1_L_05021'

select * from etl_load_table_map where task_id ='A05022'

select * from USYS_TABLE_MAINTAIN where table_id = 10175

select a.*
from USYS_TABLE_MAINTAIN a 
join (select table_id , count(0) from  USYS_TABLE_MAINTAIN  group by table_id having count(0) > 1 ) b on a.table_id = b.table_id

select a.*
from etl_load_table_map a 
join (select task_id , count(0) from  etl_load_table_map  group by task_id having count(0) > 1 ) b on a.task_id = b.task_id


select 

--delete 
--delete from USYS_TABLE_MAINTAIN where table_name like 'DWD_NG2%'

select * from etl_load_table_map where task_id='A05021'
--delete from app.sch_control_TASK WHERE control_code='TR1_L_05022'
and cmd_line='ODS_CDR_GPRS_GXZ_YYYYMMDD'

select count(0),count(distinct table_id) from USYS_TABLE_MAINTAIN

select count(0) from ODS_CDR_GPRS_GXZ_20101130


select count(0) from ODS_PM_SP_INFO_20101201


select count(0) from ODS_CDR_GPRS_GXZ_20101201






select count(0) from etl_load_table_map 

SELECT * FROM etl_load_table_map WHERE TASK_ID = 'A05022'


select * from USYS_TABLE_MAINTAIN where table_id = 10175

select * from ODS_CDR_GPRS_GXZ_YYYYMMDD

select * from BASS2.ODS_CDR_GPRS_GXZ_20101201
select * from USYS_TABLE_MAINTAIN

select * from syscat.tables where tabname like '%ODS_PM_SP_INFO%'

SELECT * FROM app.sch_control_task where control_code = 'TR1_L_05022'
SELECT * FROM app.sch_control_task where control_code = 'TR1_L_05021'

SELECT * FROM SYSCAT.TABLES WHERE TABNAME LIKE '%ODS_CDR_GPRS_GXZ_201012%'

select count(0),count(distinct control_code) from app.sch_control_task 

select a.*
from app.sch_control_task  a 
join (select control_code , count(0) from  app.sch_control_task  group by control_code having count(0) > 1 ) b on a.control_code = b.control_code

TR1_L_06020	1	2	DWD_NG2_I06020_YYYYMMDD	0	-1	彩铃结算清单	-	TR1_BOSS	2	-
TR1_L_06020	1	2	DWD_NG2_I06020_YYYYMMDD	0	-1	彩铃结算清单	-	TR1_BOSS	2	-

TR1_L_05021	1	2	ODS_CDR_GPRS_GXZ_YYYYMMDD	0	-1	GPRS清单（GXZ）	-	TR1_BOSS	2	-

--delete from app.sch_control_task  where cmd_line LIKE  'ODS_CDR_GPRS_GXZ_YYYYMMDD%'
and control_code = 'TR1_L_05021'


select * from app.sch_control_task where cmd_line LIKE  'DWD_NG2%'


select * from app.sch_control_task where control_code = 'TR1_L_06020' 



insert into etl_load_table_map values('M06026','DWD_NG2_M06026_YYYYMM','跨省集团短信结算',0,'BASS2.CDR_ISMG_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10192,'跨省集团短信结算','DWD_NG2_M06026','1','month',255,'0','','','BASS2','DWD_NG2_M06026_YYYYMM','TBS_3H','TBS_INDEX','SEQUENCE_NO',1)
insert into app.sch_control_task values ('TR1_L_06026',1,2,'DWD_NG2_M06026_YYYYMM',0,-1,'跨省集团短信结算','-','TR1_BOSS',2,'-')

select * from etl_load_table_map where task_id='M06026'
select * from USYS_TABLE_MAINTAIN where table_id=10192

CREATE TABLE DWD_NG2_M06026_201005 (				
        ROW_ID              INTEGER             --话单记录标记
        ,SEQUENCE_NO        VARCHAR(25)         --短消息序列号
        ,DR_TYPE            INTEGER          --短消息话单类型
        ,USER_TYPE          SMALLINT         --用户类型
        ,USER_NUMBER        VARCHAR(15)         --计费用户号码
        ,EC_CD              VARCHAR(16)           --EC代码
        ,SI_CD              INTEGER            --SI代码
        ,THIRD_NUM          VARCHAR(16)         --第三方号码
        ,SERVICE_CODE       VARCHAR(10)         --服务代码
        ,OPERATOR_CODE      VARCHAR(20)         --业务代码
        ,SEND_STATE         VARCHAR(7)          --短消息发送状态
        ,SEND_PRIORITY      CHARACTER(1)        --短消息发送优先级
        ,MESSAGE_LENGTH     SMALLINT         --信息长度
        ,ISMG_ID            VARCHAR(6)          --网关代码
        ,FORWARD_ISMG_ID    VARCHAR(6)          --前转网关代码
        ,SMSC_ID            VARCHAR(11)         --短消息中心代码
        ,INPUT_TIME         TIMESTAMP       --申请时间
        ,PROCESS_TIME       TIMESTAMP       --处理结束时间
        ,RESERVE            VARCHAR(32)             --预留
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( SEQUENCE_NO )				
 USING HASHING	
 
 
select * from syscat.tables where tabname like '%CDR_ISMG%' 
ORDER BY 1
CDR_ISMG_20101031



select * from CDR_ISMG_20101031

select * from 

select 
 ROW_ID				
,SEQUENCE_NO        
,DRTYPE_ID DR_TYPE            
,USERTYPE_ID USER_TYPE          
,PRODUCT_NO USER_NUMBER        
,PLAN_ID EC_CODE         
,CONDITION_ID SI_CODE
,PRODUCT_NO THIRD_NUM          
,SERVICE_CODE       
,OPERATOR_CODE		
,value(SEND_STATE,'0')
,value(SEND_PRIORITY,'0')
,MESSAGE_LENGTH     
,ISMG_ID            
,FORWARD_ISMG_ID    
,value(SMSC_ID,'0') SMSC_ID            
,substr(replace(replace(char(INPUT_TIME),'-',''),'.',''),1,14) INPUT_TIME
,substr(replace(replace(char(PROCESS_TIME),'-',''),'.',''),1,14) PROCESS_TIME  
,value(RESERVE1,'-')			 
from CDR_ISMG_20100610


select count(*) from bass2.CDR_ISMG_20101128
where sp_code in 
('901052'
,'901128'
,'902118'
,'901091'
,'901053'
,'901010'
,'901068'
,'914017'
,'901134'
,'901079'
,'901168'
,'931079'
,'918056'
,'901081'
,'901057'
,'901008'
,'901859'
,'901082'
,'800089'
,'901017'
,'801168'
,'931075')



select * from DWD_NG2_I06024_20100610


drop table DWD_NG2_I06024_20100610
CREATE TABLE DWD_NG2_I06024_20100610 (		
ROW_NO                  DECIMAL(12)         	--话单记录标记
,SEQUENCE_NO             VARCHAR(22)         	--话单序列号
,PRODUCT_NO              VARCHAR(15)         	--计费用户号码
,IMSI                    VARCHAR(15) 			--用户的IMSI号码
,TERMINAL_ID             VARCHAR(15)         	--终端设备标识
,BRAND_ID                VARCHAR(1)          	--用户类型
,ACCEPT_NO               VARCHAR(15)         	--接收方号码
,MRING_CODE              VARCHAR(7)          	--多媒体彩铃平台代码
,CENTER_CODE             VARCHAR(7)          	--中央平台代码
,CP_CODE                 VARCHAR(10)         	--版权方代码/全网SP代码
,MSOURCE_ID              VARCHAR(12)         	--音源ID
,MRING_ID                VARCHAR(18)         	--彩铃ID
,BILLTYPE_ID             VARCHAR(2)          	--计费类型
,RING_PRICE              VARCHAR(6)          	--铃音费用
,RING_PRICE_disc              VARCHAR(6)          	--优惠后铃音费用
,START_DATE              DATE					--开始时间
,ORDER_TYPE              VARCHAR(3)          	--定购方式
,BILL_TOUCH_TYPE			VARCHAR(2)			--	计费触发方式
,MEMBER_ATTRIBUTE        VARCHAR(1)   			--会员属性
,RESERVE                 VARCHAR(20)  			--保留字段
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SEQUENCE_NO )
 USING HASHING


CREATE TABLE DWD_NG2_I06024_20100610 (		
ROW_NO                  DECIMAL(12)         	--话单记录标记
,SEQUENCE_NO             VARCHAR(22)         	--话单序列号
,PRODUCT_NO              VARCHAR(15)         	--计费用户号码
,IMSI                    VARCHAR(15) 			--用户的IMSI号码
,TERMINAL_ID             VARCHAR(15)         	--终端设备标识
,BRAND_ID                VARCHAR(1)          	--用户类型
,ACCEPT_NO               VARCHAR(15)         	--接收方号码
,MRING_CODE              VARCHAR(7)          	--多媒体彩铃平台代码
,CENTER_CODE             VARCHAR(7)          	--中央平台代码
,CP_CODE                 VARCHAR(10)         	--版权方代码/全网SP代码
,MSOURCE_ID              VARCHAR(12)         	--音源ID
,MRING_ID                VARCHAR(18)         	--彩铃ID
,BILLTYPE_ID             VARCHAR(2)          	--计费类型
,RING_PRICE              VARCHAR(6)          	--铃音费用
,RING_PRICE              VARCHAR(6)          	--优惠后铃音费用
,START_DATE              DATE					--开始时间
,ORDER_TYPE              VARCHAR(3)          	--定购方式
,BILL_TOUCH_TYPE			VARCHAR(2)			--	计费触发方式
,MEMBER_ATTRIBUTE        VARCHAR(1)   			--会员属性
,RESERVE                 VARCHAR(20)  			--保留字段
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SEQUENCE_NO )
 USING HASHING




insert into etl_load_table_map values('I06024','DWD_NG2_I06024_YYYYMMDD','多媒体彩铃结算清单',0,'BASS2.VGOP_15202_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10191,'多媒体彩铃结算清单','DWD_NG2_I06024','1','day',255,'0','','','BASS2','DWD_NG2_I06024_YYYYMMDD','TBS_3H','TBS_INDEX','SEQUENCE_NO',1)
insert into app.sch_control_task values ('TR1_L_06024',1,2,'DWD_NG2_I06024_YYYYMMDD',0,-1,'多媒体彩铃结算清单','-','TR1_BOSS',2,'-')




  create nickname XZBASS2.VGOP_15202_20100610
 for db25.BASS2.VGOP_15202_20101020
 
 
 select * from DW_PRODUCT_20100610
 
 
select * from syscat.tables where tabname like '%CDR_ISMG%' 
ORDER BY 2



select b.product_no,b.imsi
from BASS2.VGOP_15202_20101128 a
 join DW_PRODUCT_20101129 b on a.product_no = b.product_no

select count(0) from  BASS2.VGOP_15202_20101120



SELECT  IMSI,PRODUCT_NO FROM DW_PRODUCT_20101128


select * from syscat.tables where tabname like 'TMP_'

select * from VGOP_15202_20101020

select * from vgop_15202_20101124

select * from 
select * from DWD_NG2_I06020_20100610
select values(SMSC_ID,'0') from CDR_ISMG_20100610

rel

select * from DWD_NG2_I06020_20100610




select * from etl_load_table_map where task_id = 'I06020'
select * from USYS_TABLE_MAINTAIN where table_id >= 10180
select * from app.sch_control_task where  control_code = 'TR1_L_06020'

delete from USYS_TABLE_MAINTAIN where table_id = 10190


select * from syscat.tables where tabname like '%VGOP_15202%'

order by 2


select count(0) from CDR_ISMG_20100610 


 select * from bass2.CDR_ISMG_20101116  
 
 
select count(*) from bass2.CDR_ISMG_20101116  
where sp_code in ( '800089'
,'801168'
,'901008'
,'901010'
,'901017'
,'901052'
,'901053'
,'901057'
,'901068'
,'901079'
,'901081'
,'901082'
,'901091'
,'901128'
,'901134'
,'901168'
,'901859'
,'902118'
,'914017'
,'918056'
,'931075'
,'931079')



java ETLMain 20101128 taskList_tmp_pzw.properties
java ETLMain 20101127 taskList_tmp_pzw.properties
java ETLMain 20101126 taskList_tmp_pzw.properties

insert into etl_load_table_map values('M06025','DWD_NG2_M06025_YYYYMM','TD终端租机业务结算',0,'._YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10189,'TD终端租机业务结算','DWD_NG2_M06025','1','month',255,'0','','','BASS2','DWD_NG2_M06025_YYYYMM','TBS_3H','TBS_INDEX','settle_info_id,REGION_ID',1)
insert into app.sch_control_task values ('TR1_L_06025',1,2,'DWD_NG2_M06025_YYYYMM',0,-1,'TD终端租机业务结算','-','TR1_BOSS',2,'-')
delete from USYS_TABLE_MAINTAIN where 

select * from USYS_TABLE_MAINTAIN where table_id >= 10180
I06020
insert into etl_load_table_map values('I06020','DWD_NG2_I06020_YYYYMMDD','彩铃结算清单',0,'BASS2.CDR_ISMG_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10190,'彩铃结算清单','DWD_NG2_I06020','1','day',255,'0','','','BASS2','DWD_NG2_I06020_YYYYMMDD','TBS_3H','TBS_INDEX','SEQUENCE_NO',1)
insert into app.sch_control_task values ('TR1_L_06020',1,2,'DWD_NG2_I06020_YYYYMMDD',0,-1,'彩铃结算清单','-','TR1_BOSS',2,'-')

select * from etl_load_table_map where task_id = 'M06020'


select * from syscat.tables where tabname like '%CDR_ISMG_%'

insert into etl_load_table_map values('I06020','DWD_NG2_I06020_YYYYMMDD','彩铃结算清单',0,'BASS2.CDR_ISMG_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10190,'彩铃结算清单','DWD_NG2_I06020','1','day',255,'0','','','BASS2','DWD_NG2_I06020_YYYYMMDD','TBS_3H','TBS_INDEX','SEQUENCE_NO',1)
insert into app.sch_control_task values ('TR1_L_06020',1,2,'DWD_NG2_I06020_YYYYMMDD',0,-1,'彩铃结算清单','-','TR1_BOSS',2,'-')

select * from app.sch_control_task where  control_code like 'TR1_L_0602%'


delete from app.sch_control_task where  control_code = 'TR1_L_06020' 
select date(INPUT_TIME,'yyyymmddhh24miss') from 

CREATE TABLE DWD_NG2_I06020_20100610 (				
        SEQUENCE_NO         VARCHAR(25)         --短消息序列号
        ,DR_TYPE            INTEGER             --短消息话单类型
        ,USER_TYPE          SMALLINT            --用户类型
        ,USER_NUMBER        VARCHAR(15)         --计费用户号码
        ,SP_CODE            VARCHAR(6)            --SP代码
        ,THIRD_NUM          VARCHAR(16)         --第三方号码
        ,SERVICE_CODE       VARCHAR(10)         --服务代码
        ,OPERATOR_CODE      VARCHAR(20)         --业务代码
        ,BILL_FLAG          SMALLINT            --用户计费类别
        ,CHARGE1            INTEGER               --信息费
        ,CHARGE2            INTEGER               --包月费
        ,SEND_STATE         VARCHAR(7)          --短消息发送状态
        ,SEND_PRIORITY      CHAR(1)             --短消息发送优先级
        ,MESSAGE_LENGTH     SMALLINT            --信息长度
        ,PROVINCE_ID        VARCHAR(7)          --计费用户号码归属省
        ,ISMG_ID            VARCHAR(6)          --网关代码
        ,FORWARD_ISMG_ID    VARCHAR(6)          --前转网关代码
        ,SMSC_ID            VARCHAR(11)         --短消息中心代码
        ,INPUT_TIME         TIMESTAMP           --申请时间
        ,PROCESS_TIME       TIMESTAMP           --处理结束时间
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( SEQUENCE_NO )
 USING HASHING


select * from app.sch_control_task where control_code = 'TR1_L_06025'
update 
app.sch_control_task 
set deal_time = 2 
where control_code = 'TR1_L_06025'

select	*	from	etl_task_log
where	task_id	in	('A02027')	


 select * from etl_task_log where  task_id like '%I02027%' 
 order by 2 desc 
 
 where 
  (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 and alarmtime >=  timestamp('20101114'||'000000') 
 order by alarmtime desc




select * from syscat.tables where tabname like 'ODS_AGENT_OPER_%'
order by 2



SELECT * FROM ODS_AGENT_OPER_20101129








select * from app.sch_control_task where cmd_line like '%ODS_AGENT_OPER_%'


select * from ODS_AGENT_OPER_20101125


SELECT count(0) FROM ODS_AGENT_OPER_20101120
SELECT count(0) FROM ODS_AGENT_OPER_20101121
SELECT count(0) FROM ODS_AGENT_OPER_20101122
SELECT count(0) FROM ODS_AGENT_OPER_20101123
SELECT count(0) FROM ODS_AGENT_OPER_20101124
SELECT count(0) FROM ODS_AGENT_OPER_20101125
SELECT count(0) FROM ODS_AGENT_OPER_20101126
SELECT count(0) FROM ODS_AGENT_OPER_20101127
SELECT count(0) FROM ODS_AGENT_OPER_20101128



select * from app.sch_control_before where before_control_code like '%02002%'












select 
trim(char(rank()over(order by mo_group_desc)))
,char(row_number()over(partition by b.mo_group_desc order by function_desc)) rn 
,trim(char(rank()over(order by mo_group_desc)))||'.'||
char(row_number()over(partition by b.mo_group_desc order by function_desc)) rn 
, b.mo_group_desc,function_desc
,
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(upper(cmd_line),'DS ','')
,' YESTERDAY()','')
,' LASTMONTH()-01','')
,'CRT_BASETAB.SH','')
,'MAIN.SH ','')
,'LASTMONTH()','')
,'CRT_BASETAB_OLAP.SH -','')
,'INT -S ','')
,'BASS1_EXPORT ','')
,'CALL ','')
cmd_line
from app.sch_control_task a
 join  app.SCH_CONTROL_MOGRPINFO b on a.MO_GROUP_CODE = b.MO_GROUP_CODE
 where function_desc is not null 
and mo_group_desc in ('139邮箱平台接口加载'
,'BOSS数据加载'
,'DSMP数据加载'
,'DWD程序'
,'IMEI数据加载'
,'KPI程序'
,'MIS上报'
,'NGBASS1本地报表'
,'NGBASS1集团客户'
,'NGBASS1渠道管理'
,'NGBASS1客户服务'
,'NGBASS1客户发展与收益'
,'NGBASS1专题'
,'OTA数据加载'
,'VGOP平台接口'
,'VGOP仓库数据'
,'绑定程序'
,'报表程序(财务部)'
,'报表程序(电子客服中心)'
,'报表程序(互联互通办)'
,'报表程序(集团客户中心)'
,'报表程序(计划建设部)'
,'报表程序(市场部)'
,'报表程序(数据中心)'
,'报表程序(网络管理中心)'
,'报表程序(营销渠道管理中心)'
,'报表程序(营业部)'
,'本地需求'
,'彩铃数据加载'
,'多维分析数据加载'
,'多维分析数据生成'
,'二经程序'
,'飞信数据加载'
,'公网平台接口'
,'行业应用终端加载'
,'集团价值评估'
,'客户服务'
,'片区化程序'
,'深运平台数据加载'
,'手机游戏平台接口加载'
,'手机支付接口'
,'数据集市_本地需求'
,'数据集市_产品策划'
,'数据集市_大客户经理'
,'数据集市_集团客户经理'
,'数据集市_经营分析师'
,'数据集市_渠道管理'
,'数据集市_片区经理'
,'数据业务深度运营'
,'网管数据加载'
,'新业务营销体验加载'
,'一经程序'
,'营销管理')
--and a.cmd_line not like '%CONNECT TO%'
and a.FUNCTION_DESC not like '%作废%'
and a.FUNCTION_DESC not like '%告警%'
order by 
4,5







select * from app.SCH_CONTROL_MOGRPINFO

select dense_rank()over(partition by mo_group_desc order by mo_group_desc) rn ,b.mo_group_desc
from app.sch_control_task a
 join  app.SCH_CONTROL_MOGRPINFO b on a.MO_GROUP_CODE = b.MO_GROUP_CODE
 

select 
--trim(char(rank()over(partition by b.mo_group_desc order by mo_group_desc)))||'.'||
--char(row_number()over(partition by b.mo_group_desc order by function_desc)) rn 
trim(char(dense_rank()over(order by mo_group_desc)))
,char(row_number()over(partition by b.mo_group_desc order by function_desc)) rn 
,trim(char(dense_rank()over(order by mo_group_desc)))||'.'||
char(row_number()over(partition by b.mo_group_desc order by function_desc)) rn 
, b.mo_group_desc,function_desc
,
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(upper(cmd_line),'DS ','')
,' YESTERDAY()','')
,' LASTMONTH()-01','')
,'CRT_BASETAB.SH','')
,'MAIN.SH ','')
,'LASTMONTH()','')
,'CRT_BASETAB_OLAP.SH -','')
,'INT -S ','')
,'BASS1_EXPORT ','')
,'CALL ','')
cmd_line
from app.sch_control_task a
 join  app.SCH_CONTROL_MOGRPINFO b on a.MO_GROUP_CODE = b.MO_GROUP_CODE
 where function_desc is not null 
and mo_group_desc in ('139邮箱平台接口加载'
,'BOSS数据加载'
,'DSMP数据加载'
,'DWD程序'
,'IMEI数据加载'
,'KPI程序'
,'MIS上报'
,'NGBASS1本地报表'
,'NGBASS1集团客户'
,'NGBASS1渠道管理'
,'NGBASS1客户服务'
,'NGBASS1客户发展与收益'
,'NGBASS1专题'
,'OTA数据加载'
,'VGOP平台接口'
,'VGOP仓库数据'
,'绑定程序'
,'报表程序(财务部)'
,'报表程序(电子客服中心)'
,'报表程序(互联互通办)'
,'报表程序(集团客户中心)'
,'报表程序(计划建设部)'
,'报表程序(市场部)'
,'报表程序(数据中心)'
,'报表程序(网络管理中心)'
,'报表程序(营销渠道管理中心)'
,'报表程序(营业部)'
,'本地需求'
,'彩铃数据加载'
,'多维分析数据加载'
,'多维分析数据生成'
,'二经程序'
,'飞信数据加载'
,'公网平台接口'
,'行业应用终端加载'
,'集团价值评估'
,'客户服务'
,'片区化程序'
,'深运平台数据加载'
,'手机游戏平台接口加载'
,'手机支付接口'
,'数据集市_本地需求'
,'数据集市_产品策划'
,'数据集市_大客户经理'
,'数据集市_集团客户经理'
,'数据集市_经营分析师'
,'数据集市_渠道管理'
,'数据集市_片区经理'
,'数据业务深度运营'
,'网管数据加载'
,'新业务营销体验加载'
,'一经程序'
,'营销管理')
--and a.cmd_line not like '%CONNECT TO%'
and a.FUNCTION_DESC not like '%作废%'
and a.FUNCTION_DESC not like '%告警%'
order by 
4,5







select dense_rank()over( order by mo_group_desc ) grp_no,mo_group_desc 
from app.SCH_CONTROL_MOGRPINFO a 
where mo_group_desc in ('139邮箱平台接口加载'
,'BOSS数据加载'
,'DSMP数据加载'
,'DWD程序'
,'IMEI数据加载'
,'KPI程序'
,'MIS上报'
,'NGBASS1本地报表'
,'NGBASS1集团客户'
,'NGBASS1渠道管理'
,'NGBASS1客户服务'
,'NGBASS1客户发展与收益'
,'NGBASS1专题'
,'OTA数据加载'
,'VGOP平台接口'
,'VGOP仓库数据'
,'绑定程序'
,'报表程序(财务部)'
,'报表程序(电子客服中心)'
,'报表程序(互联互通办)'
,'报表程序(集团客户中心)'
,'报表程序(计划建设部)'
,'报表程序(市场部)'
,'报表程序(数据中心)'
,'报表程序(网络管理中心)'
,'报表程序(营销渠道管理中心)'
,'报表程序(营业部)'
,'本地需求'
,'彩铃数据加载'
,'多维分析数据加载'
,'多维分析数据生成'
,'二经程序'
,'飞信数据加载'
,'公网平台接口'
,'行业应用终端加载'
,'集团价值评估'
,'客户服务'
,'片区化程序'
,'深运平台数据加载'
,'手机游戏平台接口加载'
,'手机支付接口'
,'数据集市_本地需求'
,'数据集市_产品策划'
,'数据集市_大客户经理'
,'数据集市_集团客户经理'
,'数据集市_经营分析师'
,'数据集市_渠道管理'
,'数据集市_片区经理'
,'数据业务深度运营'
,'网管数据加载'
,'新业务营销体验加载'
,'一经程序'
,'营销管理')



select b.mo_group_desc,function_desc
,
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(
replace(upper(cmd_line),'DS ','')
,' YESTERDAY()','')
,' LASTMONTH()-01','')
,'CRT_BASETAB.SH','')
,'MAIN.SH ','')
,'LASTMONTH()','')
,'CRT_BASETAB_OLAP.SH -','')
,'INT -S ','')
,'BASS1_EXPORT ','')
,'CALL ','')
cmd_line
from app.sch_control_task a
 join  app.SCH_CONTROL_MOGRPINFO b on a.MO_GROUP_CODE = b.MO_GROUP_CODE
where function_desc is not null 
and mo_group_desc in ('139邮箱平台接口加载'
,'BOSS数据加载'
,'DSMP数据加载'
,'DWD程序'
,'IMEI数据加载'
,'KPI程序'
,'MIS上报'
,'NGBASS1本地报表'
,'NGBASS1集团客户'
,'NGBASS1渠道管理'
,'NGBASS1客户服务'
,'NGBASS1客户发展与收益'
,'NGBASS1专题'
,'OTA数据加载'
,'VGOP平台接口'
,'VGOP仓库数据'
,'绑定程序'
,'报表程序(财务部)'
,'报表程序(电子客服中心)'
,'报表程序(互联互通办)'
,'报表程序(集团客户中心)'
,'报表程序(计划建设部)'
,'报表程序(市场部)'
,'报表程序(数据中心)'
,'报表程序(网络管理中心)'
,'报表程序(营销渠道管理中心)'
,'报表程序(营业部)'
,'本地需求'
,'彩铃数据加载'
,'多维分析数据加载'
,'多维分析数据生成'
,'二经程序'
,'飞信数据加载'
,'公网平台接口'
,'行业应用终端加载'
,'集团价值评估'
,'客户服务'
,'片区化程序'
,'深运平台数据加载'
,'手机游戏平台接口加载'
,'手机支付接口'
,'数据集市_本地需求'
,'数据集市_产品策划'
,'数据集市_大客户经理'
,'数据集市_集团客户经理'
,'数据集市_经营分析师'
,'数据集市_渠道管理'
,'数据集市_片区经理'
,'数据业务深度运营'
,'网管数据加载'
,'新业务营销体验加载'
,'一经程序'
,'营销管理')
--and a.cmd_line not like '%CONNECT TO%'
and a.FUNCTION_DESC not like '%作废%'
and a.FUNCTION_DESC not like '%告警%'
order by 1,2

select * from app.sch_control_task where function_desc like '%告警%'


select b.mo_group_desc,count(0) cnt
from app.sch_control_task a
 join  app.SCH_CONTROL_MOGRPINFO b 
on a.MO_GROUP_CODE = b.MO_GROUP_CODE
group by b.mo_group_desc 
order by 1 desc




select count(0) from  app.sch_control_task


select * from BASS2.DWD_CM_BUSI_RADIUS_BOSS3_BAKNGGJ


select * from  app.SCH_CONTROL_MOGRPINFO



select 


select count(0),count(distinct task_id) from etl_load_table_map

select * from CDR_ISMG_20100501 where service_code='12530'

select          
                        rtrim(tabschema)
        ||'.'||         tabname
        ,               card
        ,               colcount
        ,               npages
        ,               fpages
        ,               create_time
        ,               stats_time
        ,               tbspace 
from            syscat.tables
where tabname like '%DWD_CM_BUSI_RADIUS%'
order by 1
with ur


select * from BASS2.CDR_ISMG_20100501


/**
insert into etl_load_table_map values('M06025','DWD_NG2_M06025_YYYYMM','TD终端租机业务结算',0,'._YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10189,'TD终端租机业务结算','DWD_NG2_M06025','1','month',255,'0','','','BASS2','DWD_NG2_M06025_YYYYMM','TBS_3H','TBS_INDEX','settle_info_id,REGION_ID',1)
insert into app.sch_control_task values ('TR1_L_06025',1,2,'DWD_NG2_M06025_YYYYMM',0,-1,'TD终端租机业务结算','-','TR1_BOSS',2,'-')


select * from 

select * from DWD_NG2_M06025_201010



select * from syscat.tables where tabname like '%M06020%'


rename DWD_NG2_M06020_201010 to DWD_NG2_M06025_201010
rename DWD_NG2_M06020_yyyymm to DWD_NG2_M06025_yyyymm


select * from DWD_NG2_M06020_201010

select left(1000*rand(),2) from sysibm.sysdummy1


SELECT A.REGION_ID
 ,trim(b.termprod_name) termprod_name
 ,b.term_model
 ,trim(b.termprod_name)||'通讯代理公司' vendor_name
 ,a.VALID_TYPE settle_type
 ,a.OFFER_INST_PROD_INST_ID
 ,a.OFFER_INSTANCE_ID 
 ,1 settle_status
 ,a.DONE_CODE 
 ,a.PRODUCT_INSTANCE_ID
 ,a.VALID_DATE
 ,a.EXPIRE_DATE
 ,a.CREATE_DATE
 ,a.DONE_DATE
 ,2000+round(rand()*1000,0) settle_amt
 ,2000 is_settle_amt
 ,0 not_settle_amt
 from (
select t.*,row_number()over(partition by t.op_id ) rn 
from xzbass2.dw_product_ins_off_ins_prod_201010 t
 where offer_id in (
	 111090009344
	,111089150014
	,111089250014
	,111089350014
	,111089450014
	,111089550014
	,111089650014
	,111089750014
	)
    ) a
left join bass2.DIM_TERM_TAC  b on a.rn = b.id

	
	

select * from bass2.DIM_TERM_TAC  where termprod_name like '%诺基%'
 where id = 100

(
select t.*,row_number()over(partition by t.op_id ) rn 
from xzbass2.dw_product_ins_off_ins_prod_201010 t
 where offer_id in (
	 111090009344
	,111089150014
	,111089250014
	,111089350014
	,111089450014
	,111089550014
	,111089650014
	,111089750014
	)
    ) a

select count(0) from xzbass2.dw_product_ins_off_ins_prod_201010



    
select row_number()over()
select op_id,count(0) 
from  xzbass2.dw_product_ins_off_ins_prod_201010 a
group by op_id


	
select * from  bass2.DIM_TERM_TAC where net_type='2' 

    
select  from DWD_NG2_M06020_201010


select count(0) ,count(distinct id ) from bass2.DIM_TERM_TAC where net_type='2'

select max(id)  from bass2.DIM_TERM_TAC where net_type='2' 





select TERM_TYPE,count(0)
from  bass2.DIM_TERM_TAC 
group by  TERM_TYPE


/**
select * from syscat.tables where owner = 'APP'
select * from app.sch_control_task 

select * from dim_boss_staff

select count(0) 
,count(distinct OPERATOR_ID) 
,count(distinct OP_ID) 
,count(distinct staff_id) 
from ods_dim_sys_operator_20101124 
where code is null

dim_boss_staff

alter table dim_boss_staff add column code varchar(20)

select * from sys

ods_dim_sys_operator_20101124

select OPERATOR_ID from ods_dim_sys_operator_20101124


dim_boss_staff


select count(0) ,count(distinct OP_ID) from dim_boss_staff 


select count(0)
from dim_boss_staff a 
join ods_dim_sys_operator_20101124 b on a.OP_ID = b.OPERATOR_ID


select op_id,operator_id,code,staff_id from ods_dim_sys_operator_20101124
select op_id,code,staff_id from dim_boss_staff


update dim_boss_staff a , ods_dim_sys_operator_20101124 b
set a.code = b.code 
where a.op_id = b.OPERATOR_ID

update dim_boss_staff a 
set a.code = (select b.code from ods_dim_sys_operator_20101124 b where a.op_id = b.staff_id)


select count(0),count(distinct code) from dim_boss_staff
where 

select count(0) from dim_boss_staff



select count(0),count(distinct a.op_id),count(distinct b.OPERATOR_ID)
from dim_boss_staff a , ods_dim_sys_operator_20101124 b where a.op_id = b.OPERATOR_ID

select count(0) from ods_dim_sys_operator_20101124


select count(0),count(distinct a.op_id),count(distinct b.OPERATOR_ID)
from dim_boss_staff a , ods_dim_sys_operator_20101124 b where a.op_id = b.staff_id



select * from dim_boss_staff


select a.op_id,a.code,b.code,b.operator_id ,b.staff_id
from dim_boss_staff a , ods_dim_sys_operator_20101124 b
where a.op_id = b.staff_id






select * from DWD_NG2_M06020_201010

/**	2010-11-22 16:36	added by  panzhiwei		**/													
--DROP TABLE DWD_NG2_M06020_201010																	
CREATE TABLE DWD_NG2_M06020_201010 (																	
        REGION_ID           VARCHAR(7)          --地域标识
        ,manufacture_name   VARCHAR(100)        --终端厂商
        ,term_model         VARCHAR(20)         --终端型号
        ,vendor_name        VARCHAR(100)        --终端供货商
        ,settle_type        SMALLINT            --结算类型
        ,settle_info_id     BIGINT              --结算通知单编号
        ,pay_acct_id        BIGINT              --支付凭证号
        ,pay_state          SMALLINT            --支付状态
        ,done_code          BIGINT              --租机订单编号
        ,contract_id        BIGINT              --租机合同编号
        ,valid_date         date           --生效日期
        ,expire_date        date           --截止日期
        ,info_date          date           --通知日期
        ,settle_date        date           --结算周期
        ,settle_amt         DECIMAL(12,0)       --结算总金额
        ,is_settle_amt      DECIMAL(12,0)       --已结算金额
        ,not_settle_amt     DECIMAL(12,0)       --未结算金额
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( settle_info_id,REGION_ID )
 USING HASHING
select replace(char(CREATE_DATE,iso),'-','')||'000000'  from xzbass2.dw_product_ins_off_ins_prod_201010

select date('2010-11-01') from sysibm.sysDUMMY1



select * from DWD_NG2_M06020_201010

select * from DWD_NG2_M06020_201010


/**
insert into app.sch_control_task values ('TR1_L_06020',1,2,'DWD_NG2_M06020_YYYYMM',0,-1,'TD终端租机业务结算','-','TR1_BOSS',2,'-')



insert into etl_load_table_map values('M06020','DWD_NG2_M06020_YYYYMM','TD终端租机业务结算',0,'._YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10189,'TD终端租机业务结算','DWD_NG2_M06020','1','month',255,'0','','','BASS2','DWD_NG2_M06020_YYYYMM','TBS_3H','TBS_INDEX','settle_info_id,REGION_ID',1)


/**	2010-11-22 16:36	added by  panzhiwei		**/													
--DROP TABLE DWD_NG2_M06020_YYYYMMDD																	
CREATE TABLE DWD_NG2_M06020_201010 (																	
        REGION_ID           VARCHAR(7)          --地域标识
        ,manufacture_name   VARCHAR(100)        --终端厂商
        ,term_model         VARCHAR(20)         --终端型号
        ,vendor_name        VARCHAR(100)        --终端供货商
        ,settle_type        SMALLINT            --结算类型
        ,settle_info_id     BIGINT              --结算通知单编号
        ,pay_acct_id        BIGINT              --支付凭证号
        ,pay_state          SMALLINT            --支付状态
        ,done_code          BIGINT              --租机订单编号
        ,contract_id        BIGINT              --租机合同编号
        ,valid_date         TIMESTAMP           --生效日期
        ,expire_date        TIMESTAMP           --截止日期
        ,info_date          TIMESTAMP           --通知日期
        ,settle_date        TIMESTAMP           --结算周期
        ,settle_amt         DECIMAL(12,0)       --结算总金额
        ,is_settle_amt      DECIMAL(12,0)       --已结算金额
        ,not_settle_amt     DECIMAL(12,0)       --未结算金额
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( settle_info_id,REGION_ID )
 USING HASHING
select count(0) from  xzbass2.dw_product_ins_off_ins_prod_201010 
 where offer_id in (
	 111090009344
	,111089150014
	,111089250014
	,111089350014
	,111089450014
	,111089550014
	,111089650014
	,111089750014
	)
    
    

SELECT COUNT(0) , COUNT(DISTINCT PRODUCT_INSTANCE_ID ) 
FROM     xzbass2.dw_product_ins_off_ins_prod_201010 

PRODUCT_INSTANCE_ID


select * from syscat.tables where tabname like '%%'
order by tabname

select * from ODS_PRODUCT_INS_OFF_INS_PROD_20101116 fetch first 10 rows only 

create table ODS_PRODUCT_INS_OFF_INS_PROD_20101115 like ODS_PRODUCT_INS_OFF_INS_PROD_YYYYMMDD in TBS_3H index in tbs_index partitioning key ( OFFER_INST_PROD_INST_ID,REGION_ID ) using hashing not logged initially


select count(

select * from app.sch_control_task where control_code = 'BASS2_Drop_ods_table_ds.tcl'

select * from  bass2.drop_ods_table_log order by 2 desc 



  create nickname xzbass2.dw_product_ins_off_ins_prod_201010
 for db25.bass2.dw_product_ins_off_ins_prod_201010 
 
 select * from xzbass2.dw_product_ins_off_ins_prod_201010
 
--db2 "drop nickname XZBASS2.DW_ACCT_PRE_PAY_DM_201006"

create nickname 

 SELECT A.REGION_ID
 ,'' termprod_name 
 ,'' term_model
 ,'' vendor
 ,'' settle_type
 ,a.OFFER_INST_PROD_INST_ID
 ,a.OFFER_INSTANCE_ID 
 ,1 settle_status
 ,a.DONE_CODE 
 ,a.PRODUCT_INSTANCE_ID
 ,a.VALID_DATE
 ,a.EXPIRE_DATE
 ,a.CREATE_DATE
 ,a.DONE_DATE
 ,3000 settle_amt
 ,3000 is_settle_amt
 ,0 not_settle_amt
 from bass2.dw_product_ins_off_ins_prod_201010 a
 
 
select null 
from bass2.dw_product_ins_off_ins_prod_201010 


select *
from ODS_PRODUCT_INS_OFF_INS_PROD_20101115 
where offer_id in (111090009344
,111089150014 
,111089250014 
,111089350014 
,111089450014 
,111089550014 
,111089650014 
,111089750014 
)


/**

select *
from bass2.dw_product_ins_off_ins_prod_201010 
where offer_id in (111090009344
,111089150014 
,111089250014 
,111089350014 
,111089450014 
,111089550014 
,111089650014 
,111089750014 
)

select * from bass2.DIM_TERM_TAC where net_type='2'

select * from ZD_VI_VENDOR_INFORMATION 

select *
from bass2.dw_product_ins_off_ins_prod_201010 
where offer_id in (111090009344
1.11089E+11
1.11089E+11
1.11089E+11
1.11089E+11
1.1109E+11
1.1109E+11
1.1109E+11
)


select * from DIM_CUST_SEX


select * from syscat.tables where tabname like '%DIM%SEX%'
order by tabname

select * from ODS_PRODUCT_INS_OFF_INS_PROD_20101109 fetch first 10 rows only

select count(0) from BASS2.ODS_PRODUCT_INS_OFF_INS_PROD_20101111

create table ODS_PRODUCT_INS_OFF_INS_PROD_20101111 like ODS_PRODUCT_INS_OFF_INS_PROD_YYYYMMDD in TBS_3H index in tbs_index partitioning key ( OFFER_INST_PROD_INST_ID,REGION_ID ) using hashing not logged initially

select * from dwd_ng2_M03021_20101020 where DEALER_ID				  is null 
select * from dwd_ng2_M03021_20101020 where DEALER_NAME       is null 
select * from dwd_ng2_M03021_20101020 where GENDER            is null 
select * from dwd_ng2_M03021_20101020 where BIRTHDAY          is null 
select * from dwd_ng2_M03021_20101020 where CERT_TYPE         is null 
select * from dwd_ng2_M03021_20101020 where CERT_CODE         is null 
select * from dwd_ng2_M03021_20101020 where CONTACT_PHONE     is null 
select * from dwd_ng2_M03021_20101020 where CONTACT_ADDRESS   is null 
select * from dwd_ng2_M03021_20101020 where EMAIL             is null 
select * from dwd_ng2_M03021_20101020 where CREATE_DATE       is null 
select * from dwd_ng2_M03021_20101020 where CREATE_OP_ID      is null 
select * from dwd_ng2_M03021_20101020 where DONE_DATE         is null 
select * from dwd_ng2_M03021_20101020 where OP_ID             is null 
select * from dwd_ng2_M03021_20101020 where STATE             is null 



SELECT * FROM DWD_NG2_I01002_YYYYMMDD

select * from syscat.tables where tabname like '%DIM%BRAND%'
AND OWNER= 'BASS2'

select * from DWD_NG2_M03021_20101020
where gender = 0


select * from DIM_IRC_BRAND 

select * from bass2.DIM_PUB_BRAND
DIM_PUB_BRAND

select * from dim_app_cell_brand



select * from DWD_NG2_M03021_20101020 where 

select count(0) , count(distinct enterprise_id ) from stat_enterprise_snapshot

select count(0) , length( enterprise_id ) from stat_enterprise_snapshot
group by  length( enterprise_id ) 




insert into tmp_pzw_stat_enterprise_snapshot
select a.enterprise_id , b.group_name , value(value(d.employee_num , e.employee_num),f.employee_num) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 
left join stat_enterprise_snapshot_bak20101118 f on a.enterprise_id = f.enterprise_id          



select * from tmp_pzw_stat_enterprise_snapshot 
where employee_num is null 

ALTER TABLE tmp_pzw_stat_enterprise_snapshot ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE
insert into tmp_pzw_stat_enterprise_snapshot
select a.enterprise_id , b.group_name , value(value(d.employee_num , e.employee_num),f.employee_num) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 
left join stat_enterprise_snapshot_bak20101118 f on a.enterprise_id = f.enterprise_id          
 

delete from stat_enterprise_snapshot where employee_num is null 

select count(0)
from stat_enterprise_snapshot


891910006950

select * from stat_enterprise_snapshot where enterprise_id = '89103000821329'

 select * from bass2.TRANS_ENTERPRISE_ID_20100625  where enterprise_id = '891910006950'
 
 89103000821329
 
 

insert into stat_enterprise_snapshot
select * from 
stat_enterprise_snapshot_bak20101118
where enterprise_id not in (select enterprise_id from stat_enterprise_snapshot)



delete from stat_enterprise_snapshot 
insert into stat_enterprise_snapshot
select * from  tmp_pzw_stat_enterprise_snapshot


select count(0) , count(distinct enterprise_id||grp_nam) from tmp_pzw_2


DROP TABLE bass2.tmp_pzw_21
CREATE TABLE bass2.tmp_pzw_21(
    enterprise_id               varchar(20),        --集团ID 
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY

insert into tmp_pzw_21
select enterprise_id , sum(employee_num) employee_num 
from (select distinct * from tmp_pzw_2 ) a 
group by enterprise_id 


select count(0),count(distinct enterprise_id ) from tmp_pzw_21


ALTER TABLE tmp_pzw_stat_enterprise_snapshot ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select  length(enterprise_id) , count(0) from tmp_pzw_2
group by length(enterprise_id)
order by 1

select count(0) , count(distinct enterprise_id) from tmp_pzw_2

select count(0) , count(distinct enterprise_id||grp_nam) from tmp_pzw_2

select count(0) , count(distinct enterprise_id||grp_nam||id) from tmp_pzw_2



insert into tmp_pzw_stat_enterprise_snapshot
select a.enterprise_id , b.group_name , value(value(d.employee_num , e.employee_num),0) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 



select a.*
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam namid  , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam   = b.namid
order by 1

108



select count(0) from stat_enterprise_snapshot

select count(0) from tmp_pzw_1
3542
select count(0) from tmp_pzw_2
3540

select * From stat_enterprise_snapshot a, stat_enterprise_snapshot_bak20101118 b
where a.enterprise_id = b.enterprise_id and a.employee_num = 0


delete from stat_enterprise_snapshot 
insert into stat_enterprise_snapshot
select * from  tmp_pzw_stat_enterprise_snapshot

select * from 
stat_enterprise_snapshot_bak20101118
where enterprise_id not in (select enterprise_id from stat_enterprise_snapshot)




insert into stat_enterprise_snapshot
select * from 
stat_enterprise_snapshot_bak20101118
where enterprise_id not in (select enterprise_id from stat_enterprise_snapshot)



select count(0),count(distinct enterprise_id ) from stat_enterprise_snapshot


select count(0),count(distinct a.ENTERPRISE_ID) , count(distinct b.ENTERPRISE_ID)
,sum( case when  a.ENTERPRISE_ID is null and b.ENTERPRISE_ID is not null  then 1 else 0 end ) lost
,sum( case when  b.ENTERPRISE_ID is null and a.ENTERPRISE_ID is not null  then 1 else 0 end ) new
from tmp_pzw_stat_enterprise_snapshot a 
full join stat_enterprise_snapshot_bak20101118 b on a.ENTERPRISE_ID = b.ENTERPRISE_ID



select count(0) from stat_enterprise_snapshot
where employee_num = 0 

select sum(employee_num) from stat_enterprise_snapshot
select sum(employee_num) from tmp_pzw_stat_enterprise_snapshot

(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 

select count(0)
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 
where d.EMPLOYEE_NUM is  null and e.EMPLOYEE_NUM is  null


select * from syscat.tables where tabname like '%STAT_ENTERPRISE_SNAPSHOT%'

select * from tmp_pzw_stat_enterprise_snapshot

select 
from tmp_pzw_stat_enterprise_snapshot a 
full join stat_enterprise_snapshot b on a.ENTERPRISE_ID = b.ENTERPRISE_ID
where a.EMPLOYEE_NUM = 0


select count(0),count(distinct a.ENTERPRISE_ID) , count(distinct b.ENTERPRISE_ID)
,sum( case when  a.ENTERPRISE_ID is null and b.ENTERPRISE_ID is not null  then 1 else 0 end ) lost
,sum( case when  b.ENTERPRISE_ID is null and a.ENTERPRISE_ID is not null  then 1 else 0 end ) new
from tmp_pzw_stat_enterprise_snapshot a 
full join stat_enterprise_snapshot b on a.ENTERPRISE_ID = b.ENTERPRISE_ID


select *
from dw_enterprise_member_mid_201004
where enterprise_id in 
(
select a.enterprise_id
from tmp_pzw_stat_enterprise_snapshot a 
join stat_enterprise_snapshot b on a.ENTERPRISE_ID = b.ENTERPRISE_ID
where a.EMPLOYEE_NUM = 0
)
insert into tmp_pzw_stat_enterprise_snapshot
select a.enterprise_id , b.group_name , value(value(d.employee_num , e.employee_num),0) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 



ALTER TABLE tmp_pzw_stat_enterprise_snapshot ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select * from  bass2.DWD_ENTERPRISE_MSG_20101117
where enterprise_id in (
select enterprise_id from tmp_pzw_stat_enterprise_snapshot  where employee_num is null
)


89203001394239

select * from stat_enterprise_snapshot
where enterprise_id = '89203001394239'


select * from 
dw_enterprise_member_mid_201010
where enterprise_id = '89203001394239'

select * from  bass2.DWD_ENTERPRISE_MSG_20101117 where enterprise_id = '89203001394239'

select count(0),count(distinct enterprise_id ) from tmp_pzw_stat_enterprise_snapshot

select count(0) from (
select a.enterprise_id , b.group_name , d.employee_num  employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
) t
 where employee_num is null

insert into tmp_pzw_stat_enterprise_snapshot
select a.enterprise_id , b.group_name , value(d.employee_num , e.employee_num) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 
         
         

create table tmp_pzw_stat_enterprise_snapshot like  stat_enterprise_snapshot


select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id
         

select a.enterprise_id , b.group_name , value(d.employee_num , e.employee_num) employee_num
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 
left join (select b.enterprise_id,count(distinct a.user_id) employee_num
         from dw_enterprise_member_mid_201004 a,dw_enterprise_msg_201004 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id) e on a.enterprise_id = e.enterprise_id 

         

select  length(enterprise_id) , count(0) from tmp_pzw_2
group by length(enterprise_id)

 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_1 a
 left join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
  select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_1 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 

select  length(enterprise_id) , count(0) from tmp_pzw_1
group by length(enterprise_id)
order by 1


select count(0)
 from tmp_pzw_1 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
 
 CREATE TABLE bass2.tmp_pzw_2(
    enterprise_id               varchar(20),        --集团ID 
    id													varchar(20),
    grp_nam													varchar(200),
    org_typ											varchar(10),
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY

select count(0)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_21 b on b.enterprise_id = a.new_enterprise_id 


select count(0) , count(distinct enterprise_id),count(distinct new_enterprise_id)
from TRANS_ENTERPRISE_ID_20100625
9967	9967	9967

 select count(0) , count(distinct enterprise_id)  from bass2.DWD_ENTERPRISE_MSG_20101117 
 

select a.enterprise_id , b.group_name , d.employee_num 
from 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 


select a.enterprise_id , b.group_name , d.employee_num 
(select distinct enterprise_id from bass2.tmp_pzw_1 ) a 
join  bass2.DWD_ENTERPRISE_MSG_20101117 b on a.enterprise_id = b.enterprise_id 
left join bass2.TRANS_ENTERPRISE_ID_20100625 c on b.enterprise_id = c.new_enterprise_id 
left join bass2.tmp_pzw_21 d on c.enterprise_id = d.enterprise_id 

SELECT * from tmp_pzw_stat_enterprise_snapshot where ENTERPRISE_NAME is null


select tabname from syscat.tables where tabname like '%DW_ENTERPRISE_MEMBER%'
select count(0) , count(distinct enterprise_id||grp_nam) from tmp_pzw_2


--DROP TABLE bass2.tmp_pzw_21
CREATE TABLE bass2.tmp_pzw_21(
    enterprise_id               varchar(20),        --集团ID 
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY

select count(0),count(distinct b.enterprise_id)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_21 b on a.enterprise_id = b.enterprise_id 



 insert into tmp_pzw_21
select enterprise_id , sum(employee_num) employee_num 
from (select distinct * from tmp_pzw_2 ) a 
group by enterprise_id 



CREATE TABLE bass2.tmp_pzw_1(
    enterprise_id               varchar(20)        --集团ID
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY

create table stat_enterprise_snapshot_bak20101118 like  stat_enterprise_snapshot

insert into stat_enterprise_snapshot_bak20101118
select * from stat_enterprise_snapshot

select * from stat_enterprise_snapshot_bak20101118

CREATE TABLE bass2.tmp_pzw_1(
    enterprise_id               varchar(20)        --集团ID
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY


select count(0),count(distinct enterprise_id) from tmp_pzw_1 



/**

select count(0),count(distinct b.enterprise_id)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_21 b on a.enterprise_id = b.enterprise_id 



ALTER TABLE tmp_pzw_2a ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

insert into tmp_pzw_2a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID, a.employee_num
 from tmp_pzw_21 a
 left  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 


select count(0),count(distinct NEW_ENTERPRISE_ID) from tmp_pzw_2a  


select count(0) , count(distinct enterprise_id ) from tmp_pzw_21

select count(0)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_21 b on b.enterprise_id = a.new_enterprise_id 



insert into tmp_pzw_21
select enterprise_id , sum(employee_num) employee_num 
from (select distinct * from tmp_pzw_2 ) a 
group by enterprise_id 



DROP TABLE bass2.tmp_pzw_21
CREATE TABLE bass2.tmp_pzw_21(
    enterprise_id               varchar(20),        --集团ID 
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY



DROP TABLE bass2.tmp_pzw_21
CREATE TABLE bass2.tmp_pzw_21(
    enterprise_id               varchar(20),        --集团ID 
    id													varchar(20),
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY


insert into tmp_pzw_21
select enterprise_id , sum(employee_num) employee_num 
from (select distinct * from tmp_pzw_2 ) a 
group by enterprise_id 






select count(0) from (select distinct * from tmp_pzw_2) a

select count(0) from  tmp_pzw_2
 
 select a.*,c.cnt
 from tmp_pzw_2 a
 join (select enterprise_id , count(0) cnt from tmp_pzw_2 a group by enterprise_id having count(0) > 1 ) b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
 join TRANS_ENTERPRISE_ID_20100625 b1 on b.ENTERPRISE_ID = b1.ENTERPRISE_ID
 join  bass2.tmp_pzw_001cnt_nk c  on b1.new_ENTERPRISE_ID = c.ENTERPRISE_ID
 where a.ENTERPRISE_ID in (select distinct enterprise_id
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam namid  , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam   = b.namid)
order by 1
 
 
 
 

select distinct enterprise_id
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam namid  , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam   = b.namid
order by 1





--导入职员数
DROP TABLE bass2.tmp_pzw_2
CREATE TABLE bass2.tmp_pzw_2(
    enterprise_id               varchar(20),        --集团ID 
    id													varchar(20),
    grp_nam													varchar(200),
    org_typ											varchar(10),
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY

select * from tmp_pzw_2 where employee_num  is null 

select distinct a.*
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam namid  , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam   = b.namid



select enterprise_id||grp_nam   , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1
 

select count(0) , count(distinct enterprise_id||id) from tmp_pzw_2

select count(0) , count(distinct enterprise_id||grp_nam) from tmp_pzw_2

select a.*
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam   , count(0) cnt from tmp_pzw_2 a
 group by enterprise_id||grp_nam  having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam namid  = b.namid


select a.*
from tmp_pzw_2 a 
join (select enterprise_id||grp_nam namid x , count(0) from tmp_pzw_2 group by enterprise_id||grp_nam namid having  count(0) > 1 ) b 
on a.enterprise_id||a.grp_nam namid  = b.x

select count(0) , count(distinct enterprise_id||grp_nam||id) from tmp_pzw_2


ALTER TABLE tmp_pzw_2a ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 

insert into tmp_pzw_2a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID, a.employee_num
 from tmp_pzw_2 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 

select * from tmp_pzw_2a




select count(0)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_2 b on a.enterprise_id = b.enterprise_id 

select count(0)
from TRANS_ENTERPRISE_ID_20100625 a 
join tmp_pzw_2 b on b.enterprise_id = a.new_enterprise_id 


DROP TABLE bass2.tmp_pzw_2
CREATE TABLE bass2.tmp_pzw_2(
    enterprise_id               varchar(20),        --集团ID 
    id													varchar(20),
    grp_nam													varchar(200),
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY

select * from tmp_pzw_2


drop table  bass2.tmp_pzw_2a 
CREATE TABLE bass2.tmp_pzw_2a(
    enterprise_id               varchar(20)        --集团ID
   , NEW_ENTERPRISE_ID						VARCHAR(20)
   , employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY


insert into tmp_pzw_2a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID, a.employee_num
 from tmp_pzw_2 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
 select count(0) , count(distinct NEW_ENTERPRISE_ID ) from tmp_pzw_2a
 
 select count(0) , count(distinct enterprise_id) from tmp_pzw_2
 
 
 select * from tmp_pzw_1a
 
 select a.*,c.cnt
 from tmp_pzw_2 a
 join (select enterprise_id , count(0) cnt from tmp_pzw_2 a group by enterprise_id having count(0) > 1 ) b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
 join TRANS_ENTERPRISE_ID_20100625 b1 on b.ENTERPRISE_ID = b1.ENTERPRISE_ID
 join  bass2.tmp_pzw_001cnt_nk c  on b1.new_ENTERPRISE_ID = c.ENTERPRISE_ID
order by 1
 
 
  create nickname bass2.tmp_pzw_001cnt_nk
 for db25.bass2.tmp_pzw_001cnt 

drop nickname  bass2.tmp_pzw_001cnt_nk



select * from tmp_pzw_001cnt_nk


 select a.*
 from tmp_pzw_2 a
 join (select enterprise_id , count(0) cnt from tmp_pzw_2 a group by enterprise_id having count(0) > 1 ) b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
order by 1


891960005016

select * from dw_enterprise_msg_20101116 
where enterprise_id = '89602999058401'

select * from bass2.TRANS_ENTERPRISE_ID_20100625
where enterprise_id='891891000370'

select * from tmp_pzw_001cnt


create view tmp_pzw_001cnt
as
select b.enterprise_id,count(distinct a.user_id) cnt
         from dw_enterprise_member_mid_201009 a,dw_enterprise_msg_201009 b
         where a.enterprise_id = b.enterprise_id and a.dummy_mark = 0
         and b.enterprise_id not in ('89100000000682','89100000000659','89100000000656','89100000000651')
         --and b.enterprise_id = '89101560000370'
         group by  b.enterprise_id


select * from stat_enterprise_0032

/**

 create table tmp_pzw_1a as
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_1 a
 left join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
insert into tmp_pzw_1a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_1 a
 left join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
 select * from tmp_pzw_1
 
 select * from TRANS_ENTERPRISE_ID_20100625
 
insert into tmp_pzw_1a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_1 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 

select count(0),count(distinct enterprise_id) , count(distinct NEW_ENTERPRISE_ID) from tmp_pzw_2a

select * from tmp_pzw_2a


truncate  tmp_pzw_2a
ALTER TABLE tmp_pzw_2a ACTIVATE NOT  LOGGED INITIALLY WITH EMPTY TABLE 


select count(0) from tmp_pzw_2 where enterprise_id is null


insert into tmp_pzw_2a
 select distinct a.enterprise_id , b.NEW_ENTERPRISE_ID
 from tmp_pzw_2 a
  join bass2.TRANS_ENTERPRISE_ID_20100625 b on a.enterprise_id = b.enterprise_id
 
 
 
 
CREATE TABLE bass2.tmp_pzw_2a(
    enterprise_id               varchar(20)        --集团ID
   , NEW_ENTERPRISE_ID						VARCHAR(20)
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY



select * from tmp_pzw_1 
where length(enterprise_id) < 14



select count(0),count(distinct enterprise_id) , count(new_enterprise_id) 
from tmp_pzw_1a

 
CREATE TABLE bass2.TRANS_ENTERPRISE_ID_20100625(
    enterprise_id               varchar(20)        --集团ID
   , NEW_ENTERPRISE_ID						VARCHAR(20)
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY
 

CREATE TABLE bass2.tmp_pzw_1a(
    enterprise_id               varchar(20)        --集团ID
   , NEW_ENTERPRISE_ID						VARCHAR(20)
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY



 select count(0), count(distinct a.enterprise_id) , count(distinct b.enterprise_id) 
 from 
( select distinct enterprise_id from tmp_pzw_1 ) a 
left join (select distinct * from tmp_pzw_2) b on a.enterprise_id = b.enterprise_id



select count(0) from stat_enterprise_snapshot


--DROP TABLE bass2.tmp_pzw_2
CREATE TABLE bass2.stat_enterprise_snapshot(
    enterprise_id               varchar(20),        --集团ID 
    ENTERPRISE_NAME							VARCHAR(200),
    employee_num             		integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY



select * from syscat.tables where tabname like '%BAK%'
 
 
 select * from stat_enterprise_snapshot


select count(0) , count(distinct enterprise_id) from tmp_pzw_1 

select count(0) , count(distinct enterprise_id) from tmp_pzw_2 


select count(0) from tmp_pzw_1

--DROP TABLE bass2.tmp_pzw_2
CREATE TABLE bass2.tmp_pzw_2(
    enterprise_id               varchar(20),        --集团ID 
    employee_num             integer             --集团职员数
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX NOT LOGGED INITIALLY


LOAD CLIENT  FROM '/bassapp/bass2/panzw2/cnt.txt' OF DEL
INSERT INTO tmp_pzw_2
ALLOW NO ACCESS

  load client from "/bassapp/bass2/panzw2/load.txt" of del   modified by coldel$ timestampformat="YYYYMMDDHHMMSS"  fastparse anyorder warningcount 1000 messages /bassapp/bass2/load/boss/messages/db2load.msg  replace into tmp_pzw_1


 create table stat_enterprise_snapshot_bak like stat_enterprise_snapshot
 insert into stat_enterprise_snapshot_bak select * from stat_enterprise_snapshot
 
  select count(0), count(distinct a.enterprise_id) , count(distinct b.enterprise_id) 
 from 
( select distinct enterprise_id from tmp_pzw_1 ) a 
left join (select distinct * from tmp_pzw_2) b on a.enterprise_id = b.enterprise_id



select count(0) 
from tmp_pzw_2 a 
join stat_enterprise_snapshot b on a.enterprise_id = b.enterprise_id 

 
 
 
 select count(0), count(distinct a.enterprise_id) 
 , count(distinct b.enterprise_id) 
 , count(distinct c.GROUP_NAME) 
 from 
( select distinct enterprise_id from tmp_pzw_1 ) a 
left join bass2.DWD_ENTERPRISE_MSG_20100531 c on a.enterprise_id = c.enterprise_id
left join (select distinct * from tmp_pzw_2) b on a.enterprise_id = b.enterprise_id



 
 select count(0) 
from tmp_pzw_2 a 
join stat_enterprise_snapshot b on a.enterprise_id = b.enterprise_id 


select count(0) from DWD_ENTERPRISE_MSG_20100531

 select count(0), count(distinct a.enterprise_id) , count(distinct b.enterprise_id) 
 from 
( select distinct enterprise_id from tmp_pzw_1 ) a 
left join 
left join (select distinct * from tmp_pzw_2a) b on a.enterprise_id = b.new_enterprise_id




 select count(0), count(distinct a.enterprise_id) 
 , count(distinct b.new_enterprise_id) 
 , count(distinct c.GROUP_NAME) 
 from 
( select distinct enterprise_id from tmp_pzw_1 ) a 
left join (select distinct * from tmp_pzw_2a) b on a.enterprise_id = b.new_enterprise_id
left join bass2.DWD_ENTERPRISE_MSG_20100531 c on a.enterprise_id = c.enterprise_id





select tabname from syscat.tables  where tabname like '%DWD_ENTERPRISE_MSG_%'
  
 select length(enterprise_id) , count(0) 
 from tmp_pzw_2
 group by length(enterprise_id)
 order by 1
 
 
select 
from  bass2.TRANS_ENTERPRISE_ID_20100625

 
 select length(enterprise_id) , count(0) 
 from bass2.TRANS_ENTERPRISE_ID_20100625
 group by length(enterprise_id)
  order by 1
  
 
 select length(enterprise_id) , count(0) 
 from tmp_pzw_2
 group by length(enterprise_id)
  order by 1
  
 
 select * from tmp_pzw_1 where length(enterprise_id) = 13
 
 select count(0) from tmp_pzw_1
 
 
 
 select count(0) from tmp_pzw_2 where enterprise_id is null
 

 select count(0), count(distinct enterprise_id),count(distinct enterprise_id||char(employee_num)) from tmp_pzw_2

select count(0) from (select distinct * from stat_enterprise_snapshot) a

select TBSPACE ,count(0) from  syscat.tables group by TBSPACE order by 2

--drop table bass2.tmp_pzw_1 
CREATE TABLE bass2.tmp_pzw_1(
    enterprise_id               varchar(20)        --集团ID
)
PARTITIONING KEY (enterprise_id) 
USING HASHING IN tbs_ods_other  
INDEX IN TBS_INDEX 
NOT LOGGED INITIALLY


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20101116'||'000000') 
order by alarmtime desc 


 select * from 
 stat_enterprise_snapshot
 select * from 
stat_enterprise_0051


select * from syscat.tables where tabname like '%STAT_ENTERPRISE_SNAPSHOT%'

select gender, count(0) from DWD_NG2_M03021_20101020 
group by gender 

create table tmp_pzw_001 like DWD_NG2_M03021_20101020
select * from tmp_pzw_001


select * from DWD_NG2_M03021_20101020

insert into tmp_pzw_001
select * 
from DWD_NG2_M03021_20101020


delete from etl_load_table_map_mTEST where task_id = 'I03021'

M03021
insert into etl_load_table_map_mTEST values('M03021','DWD_NG2_M03021_YYYYMMDD','渠道负责人',0,'CHANNEL.CHANNEL_DEALER_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10188,'渠道负责人','DWD_NG2_M03021','1','day',255,'0','','','BASS2','DWD_NG2_M03021_YYYYMMDD','TBS_3H','TBS_INDEX','dealer_id',1)
insert into app.sch_control_task values ('TR1_L_03021',1,2,'DWD_NG2_M03021_YYYYMMDD',0,-1,'渠道负责人','-','TR1_BOSS',2,'-')



/**	2010-11-16 15:32	added by  panzhiwei		**/	
/**
--DROP TABLE DWD_NG2_M03021_YYYYMMDD					
CREATE TABLE DWD_NG2_M03021_20101020 (					
        DEALER_ID           BIGINT              --渠道负责人标识
        ,DEALER_NAME        VARCHAR(60)         --渠道负责人姓名
        ,gender             SMALLINT            --负责人性别
        ,BIRTHDAY           TIMESTAMP           --负责人出生日期
        ,CERT_TYPE          SMALLINT            --负责人证件类型
        ,CERT_CODE          VARCHAR(40)         --负责人证件号码
        ,CONTACT_PHONE      VARCHAR(40)         --负责人联系电话
        ,CONTACT_ADDRESS    VARCHAR(120)        --负责人住址
        ,EMAIL              VARCHAR(120)        --负责人邮件
        ,CREATE_DATE        TIMESTAMP           --建档日期
        ,CREATE_OP_ID       BIGINT              --建档员工
        ,DONE_DATE          TIMESTAMP           --操作日期
        ,OP_ID              BIGINT              --操作员工
        ,STATE              SMALLINT                --状态
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( dealer_id )
 USING HASHING
        


/**
select * from DWD_NG2_I08112_20101020

/**	2010-11-16 14:32	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I08111_YYYYMMDD				
/**	
CREATE TABLE DWD_NG2_I08111_20101020 (					
			OPT_TYP_CODE			VARCHAR(50)				--操作类型代码			
			,OPT_TYP_NAME			VARCHAR(255)			--操作类型名称			
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( OPT_TYP_CODE )
 USING HASHING



insert into etl_load_table_map values('I08111','DWD_NG2_I08111_YYYYMMDD','网上营业厅操作类型代码表',0,'product.UP_BUSINESS_TYPE_YYYYMMDD')
insert into USYS_TABLE_MAINTAIN values(10187,'网上营业厅操作类型代码表','DWD_NG2_I08111','1','day',255,'0','','','BASS2','DWD_NG2_I08111_YYYYMMDD','TBS_3H','TBS_INDEX','OPT_TYP_CODE',1)
insert into app.sch_control_task values ('TR1_L_08111',1,2,'DWD_NG2_I08111_YYYYMMDD',0,-1,'网上营业厅操作类型代码表','-','TR1_BOSS',2,'-')




insert into etl_load_table_map values('I08112','DWD_NG2_I08112_YYYYMMDD','网上营业厅业务类型代码表',0,'BASE.cfg_operation_YYYYMMDD')	
insert into USYS_TABLE_MAINTAIN values(10186,'网上营业厅业务类型代码表','DWD_NG2_I08112','1','day',255,'0','','','BASS2','DWD_NG2_I08112_YYYYMMDD','TBS_3H','TBS_INDEX','BUSI_CODE',1)	

select * from DWD_NG2_I08112_20101020


  /**	2010-11-16 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I08112_YYYYMMDD					
/**
CREATE TABLE DWD_NG2_I08112_20101020 (					
			BUSI_CODE				VARCHAR(8)	--业务类型代码			
			,BUSI_NAME			VARCHAR(60)	--业务类型名称			
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BUSI_CODE )
 USING HASHING



select * from  app.SCH_CONTROL_TASK
where cmd_line like '%enterprise_manager_rela%'

select CC_GROUP_CODE ,MO_GROUP_CODE,APP_DIR,CMD_TYPE  , count(0)
from app.SCH_CONTROL_TASK
group by CC_GROUP_CODE ,MO_GROUP_CODE,APP_DIR,CMD_TYPE 


select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20101114'||'000000') 
order by alarmtime desc 


select * from app.SCH_CONTROL_BEFORE
where control_code like '%Dwd_enterprise_manager%'

select * from app.SCH_CONTROL_MUTEX


select * from app.SCH_CONTROL_RUNLOG
where control_code like '%Dwd_enterprise_manager%'



 select distinct control_code  from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 )
 order by alarmtime desc



select * from app.sch_control_alarm order by alarmtime desc

select * from app.SYS_TASK_RUNLOG



 select * from app.sch_control_alarm
 where control_code  like '%BASS2%'
 and alarmtime >=  timestamp('20101114'||'000000') 
 order by alarmtime desc

select * from etl_task_log


CONTROL_CODE
BASS2_Dwd_cust_manager_info_ds.tcl
BASS2_Dwd_cust_manager_info_ds.tcl
BASS2_Dwd_enterprise_manager_rela_ds.tcl
BASS2_Dwd_enterprise_manager_rela_ds.tcl


select count(0) from Dwd_enterprise_manager_rela_20101115
select count(0) from Dwd_enterprise_manager_rela_2010111


select * from Dwd_enterprise_manager_rela_20101114
 where manager_id=100005816062 with ur

Select count(0) from Dwd_enterprise_manager_rela_20101114


 SELECT distinct  
     STAFF.STAFF_ID manager_id
     ,base.MANAGER_TYPE
  ,base.MANAGER_STATUS
  ,base.dept_id
  ,base.MEMBER_TYPE
  ,base.EXAM_ROLE_ID
  ,STAFF.STATE
  ,char(province_id)
  ,substr(city_id,2)
  ,int(base.region_type)
  ,base.REGION_DETAIL
  ,STAFF_NAME manager_NAME     
  ,staff.gender
  ,base.POLITICS_FACE
  ,base.EDUCATION_LEVEL
  ,STAFF.EMAIL
  ,STAFF.BILL_ID
  ,BASE.OFFICE_TEL
  ,base.HOME_TEL
  ,base.CONTACT_ADDRESS
  ,base.POSTCODE
  ,staff.job_position
  ,base.JOB_DESC
  ,base.begin_date
  ,base.DONE_DATE
  ,base.VALID_DATE
  ,base.expire_date
  ,OP.OPERATOR_ID op_id     
  ,ORG.ORGANIZE_ID ORG_ID     
  ,base.done_code
  FROM ODS_DIM_SYS_STAFF_20101114 staff    
  JOIN ODS_DIM_SYS_OPERATOR_20101114 OP     ON      STAFF.STAFF_ID = OP.STAFF_ID
  JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101114 RELA ON    STAFF.STAFF_ID = RELA.STAFF_ID  
  JOIN ODS_DIM_SYS_ORGANIZE_20101114 ORG         ON      RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
  JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101114 )  GG ON TRIM(CHAR(GG.MGR_ID)) = OP.CODE 
  LEFT JOIN ODS_ENT_EN_STAFF_20101114 base ON STAFF.STAFF_ID = BASE.PARTY_ROLE_ID  
  WHERE staff.STAFF_ID=100005816062
  
  select * from Dwd_cust_manager_info_20101114
 where manager_id=100005816062 with ur
 select * from ODS_DIM_SYS_STAFF_20101114
  where STAFF_ID=100005816062 with ur
  
SELECT * FROM Dwd_enterprise_manager_rela_20101114
Select * from Dwd_cust_manager_info_20101114

select count(0),count(distinct char(enterprise_id)||char(manager_id)) 
from Dwd_enterprise_manager_rela_20101114


select * from app.sch_control_alarm 
 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 )
 and alarmtime >=  timestamp('20101114'||'000000') 
 order by alarmtime desc



select * from app.sch_control_before 
where control_code like '%Dwd_cust_manager_info_ds.tcl%'


delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_cust_manager_info_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_cust_manager_info_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  



/**
select * from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds.tcl%'


delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  





/**
 * 
select count(0) from Dwd_enterprise_manager_rela_20101114
select count(0) from Dwd_cust_manager_info_20101115



delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_cust_manager_info_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_cust_manager_info_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  


select * from app.sch_control_before 
where control_code like '%Dwd_cust_manager_info_ds.tcl%'

BASS2_Dwd_cust_manager_info_ds.tcl	TR1_L_11203

select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  


select control_code,cmd_line from app.sch_control_task  
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  



select * from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds.tcl%'


delete from app.sch_control_before 
where control_code like '%BASS2_Dwd_enterprise_manager_rela_ds%'


insert into app.sch_control_before 
select 'BASS2_Dwd_enterprise_manager_rela_ds.tcl',control_code
from app.sch_control_task 
where 
cmd_line like '%ODS_DIM_SYS_STAFF_Y%' or 
cmd_line like '%ODS_DIM_SYS_OPERATOR_Y%' or 
cmd_line like '%ODS_DIM_SYS_STAFF_ORG_RELAT_Y%' or 
cmd_line like '%ODS_DIM_SYS_ORGANIZE_Y%' or 
cmd_line like '%ODS_ENT_CM_GROUP_CUSTOMER_Y%' or 
cmd_line like '%ODS_ENT_EN_STAFF_Y%'  


select * from app.sch_control_before where control_code like '%Dwd_enterprise_manager_rela%'
select * from app.sch_control_task  where cmd_line like '%ODS_DIM_SYS_STAFF%'

        select DISTINCT
                 GG.GROUP_CUST_ID
                ,base.dept_id CLUSTER_ID
                ,STAFF.STAFF_ID as manager_id
                ,1  OPER_TYPE 
                ,1      REL_TYPE
                ,case when gg.expire_date<TIMESTAMP('2010-05-31 00:00:00.00000') then 0 else 1 end REC_STATUS
                ,GG.done_date
                ,gg.create_date
                ,GG.valid_date
                ,GG.EXPIRE_DATE
                ,GG.OP_ID
                ,GG.ORG_ID
                ,GG.DONE_CODE
                FROM    ODS_DIM_SYS_STAFF_20100531      staff                           
                JOIN ODS_DIM_SYS_OPERATOR_20100531      OP      ON  STAFF.STAFF_ID = OP.STAFF_ID
                JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20100531       RELA    ON      STAFF.STAFF_ID  =       RELA.STAFF_ID           
                JOIN ODS_DIM_SYS_ORGANIZE_20100531      ORG   ON         RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
                JOIN ODS_ENT_CM_GROUP_CUSTOMER_20100531         GG ON TRIM(CHAR(GG.MGR_ID))     =       OP.CODE 
                LEFT    JOIN    ODS_ENT_EN_STAFF_20100531       base    ON      STAFF.STAFF_ID  = BASE.PARTY_ROLE_ID            
                WHERE   RELA.IS_BASE_ORG        =       'Y'                     
                AND     OP.STATE        =       1                       
                AND     STAFF.STATE     =       1                       
                AND     RELA.STATE      =       1                       
                AND     ORG.STATE       =       1

select * from Dwd_enterprise_manager_rela_20100531

select rec_status
,count(0)
from Dwd_enterprise_manager_rela_20100531
group by rec_status

select 
 count(GG.GROUP_CUST_ID)
, count(distinct GG.GROUP_CUST_ID)
, count(distinct STAFF.STAFF_ID)
, count(distinct GG.GROUP_CUST_ID||gg.GROUP_CUST_ID)
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN ODS_ENT_CM_GROUP_CUSTOMER_20101113 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
        
        
select count(0)
from (
select DISTINCT
 GG.GROUP_CUST_ID
,base.dept_id CLUSTER_ID
,STAFF.STAFF_ID as manager_id
,1  OPER_TYPE 
,1 	REL_TYPE
,case when gg.expire_date<TIMESTAMP('2010-11-13 00:00:00.00000') then 0 else 1 end REC_STATUS
,GG.CREATE_DATE
,GG.valid_date
,GG.EXPIRE_DATE
,GG.OP_ID
,GG.ORG_ID
,GG.DONE_CODE
		FROM	ODS_DIM_SYS_STAFF_20101113	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101113	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101113	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101113	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID ,GROUP_CUST_ID,EXPIRE_DATE,OP_ID,ORG_ID,DONE_CODE ,valid_date,CREATE_DATE FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101113 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101113	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1									
        ) a






SELECT	distinct 	
	    STAFF.STAFF_ID	manager_id
	    ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,base.POLITICS_FACE
		,base.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,STAFF.NOTES
		,base.HOME_TEL
		,base.CONTACT_ADDRESS
		,base.POSTCODE
		,staff.job_position
		,base.JOB_DESC
		,base.begin_date
		,base.DONE_DATE
		,base.VALID_DATE
		,base.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,base.done_code
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR	

select job_company from ODS_ENT_EN_STAFF_20101105



SELECT COUNT(0) , COUNT(DISTINCT STAFF_ID) FROM ODS_DIM_SYS_STAFF_20101105

SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105

SELECT DISTINCT CODE FROM ODS_DIM_SYS_OPERATOR_20101105


SELECT * FROM DWD_CUST_MANAGER_INFO_20101105
SELECT DISTINCT STAFF_ID FROM DWD_CUST_MANAGER_INFO_20101105

select REC_STATUS, count(0),count(distinct MANAGER_ID) from DWD_CUST_MANAGER_INFO_20101105
group by REC_STATUS 

SELECT		
	  STAFF.STAFF_ID	manager_id
	  ,base.MANAGER_TYPE
		,base.MANAGER_STATUS
		,base.dept_id
		,base.MEMBER_TYPE
		,base.EXAM_ROLE_ID
		,STAFF.STATE
		,char(province_id)
		,substr(city_id,2)
		,base.region_type
		,base.REGION_DETAIL
		,STAFF_NAME	manager_NAME					
		,staff.gender
		,value(staff.POLITICS_FACE,
		,staff.EDUCATION_LEVEL
		,STAFF.EMAIL
		,STAFF.BILL_ID
		,STAFF.NOTES
		,staff.HOME_TEL
		,staff.CONTACT_ADDRESS
		,staff.POSTCODE
		,staff.job_position
		,staff.JOB_DESC
		,base.begin_date
		,staff.DONE_DATE
		,staff.VALID_DATE
		,staff.expire_date
		,OP.OPERATOR_ID	op_id					
		,ORG.ORGANIZE_ID	ORG_ID					
		,staff.done_code	
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
			WITH	UR	



select value(POLITICS_FACE,0) from ODS_DIM_SYS_STAFF_20101105


select * from ODS_DIM_SYS_STAFF_20101105




SELECT COUNT(0) ,  COUNT(DISTINCT STAFF.STAFF_ID)
		FROM	ODS_DIM_SYS_STAFF_20101105	staff				
		JOIN ODS_DIM_SYS_OPERATOR_20101105	OP					ON     	STAFF.STAFF_ID = OP.STAFF_ID
		JOIN ODS_DIM_SYS_STAFF_ORG_RELAT_20101105	RELA	ON 			STAFF.STAFF_ID	=	RELA.STAFF_ID		
		JOIN ODS_DIM_SYS_ORGANIZE_20101105	ORG         ON 	    RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
		--JOIN (SELECT DISTINCT MGR_ID FROM  ODS_ENT_CM_GROUP_CUSTOMER_20101105 ) 	GG ON TRIM(CHAR(GG.MGR_ID))	=	OP.CODE	
		LEFT	JOIN	ODS_ENT_EN_STAFF_20101105	base	ON	STAFF.STAFF_ID	= 	BASE.PARTY_ROLE_ID		
		WHERE	RELA.IS_BASE_ORG	=	'Y'			
		AND	OP.STATE	=	1			
		AND	STAFF.STATE	=	1			
		AND	RELA.STATE	=	1			
		AND	ORG.STATE	=	1			
		--	and	to_number(op.code)	='100005819902'			
			WITH	UR	




ODS_ENT_EN_STAFF_20100626
SELECT COUNT(0),count(distinct STAFF_ID ) FROM ODS_DIM_SYS_STAFF_YYYYMMDD
SELECT COUNT(0),count(distinct ) FROM ODS_ENT_EN_STAFF_YYYYMMDD
SELECT COUNT(0),count(distinct ) FROM ODS_DIM_SYS_OPERATOR_YYYYMMDD 
SELECT COUNT(0),count(distinct ) FROM ODS_DIM_SYS_ORGANIZE_YYYYMMDD 
SELECT COUNT(0),count(distinct ) FROM ODS_DIM_SYS_STAFF_ORG_RELAT_YYYYMMDD
SELECT COUNT(0),count(distinct MGR_ID) FROM ODS_ENT_CM_GROUP_CUSTOMER_YYYYMMDD 

SELECT COUNT(0),count(distinct MGR_ID) FROM ODS_ENT_CM_GROUP_CUSTOMER_20101105



PARTY_ROLE_ID

SELECT COUNT(0),count(distinct PARTY_ROLE_ID ) FROM ODS_ENT_EN_STAFF_20101105


select * from syscat.tables where tabname like '%ODS_ENT_EN_STAFF%'


    select state , count(0) from ODS_ENT_EN_STAFF_20101130 group by state



SELECT COUNT(0)
  FROM ODS_DIM_SYS_STAFF_20101105           STAFF
   LEFT JOIN ODS_ENT_EN_STAFF_20101105 base ON STAFF.STAFF_ID =
                                             BASE.PARTY_ROLE_ID,
       ODS_DIM_SYS_OPERATOR_20101105        OP,
       ODS_DIM_SYS_ORGANIZE_20101105        ORG,
       ODS_DIM_SYS_STAFF_ORG_RELAT_20101105 RELA,
       ODS_ENT_CM_GROUP_CUSTOMER_20101105   GG
 WHERE OP.STAFF_ID = STAFF.STAFF_ID
   AND STAFF.STAFF_ID = RELA.STAFF_ID
   AND RELA.ORGANIZE_ID = ORG.ORGANIZE_ID
   AND RELA.IS_BASE_ORG = 'Y'
   AND OP.STATE = 1
   AND STAFF.STATE = 1
   AND RELA.STATE = 1
   AND ORG.STATE = 1
   

select * from  app.sch_control_task


select * from DWD_NG2_I03032_20101020
select * from DWD_NG2_I03034_20101020
select * from DWD_NG2_I03036_20101020
select * from DWD_NG2_I03037_20101020



select count(0) from DWD_NG2_I03037_20101020

DWD_NG2_I03027_YYYYMMDD
DWD_NG2_I03028_YYYYMMDD
DWD_NG2_I03032_YYYYMMDD
DWD_NG2_I03034_YYYYMMDD
DWD_NG2_I03036_YYYYMMDD
DWD_NG2_I03037_YYYYMMDD



insert into etl_load_table_map values('A05021','ODS_CDR_GPRS_GXZ_YYYYMMDD','GPRS清单（GXZ）',0,'XZJF.DR_GGPRS_GXZ_YYYYMMDD')	
insert into USYS_TABLE_MAINTAIN values(10175,'GPRS清单（GXZ）','ODS_CDR_GPRS_GXZ','1','day',255,'0','','','BASS2','ODS_CDR_GPRS_GXZ_YYYYMMDD','TBS_CDR_VOICE','TBS_INDEX','USER_ID',1)	
insert into app.sch_control_task values ('TR1_L_05021',1,2,'ODS_CDR_GPRS_GXZ_YYYYMMDD',0,-1,'GPRS清单（GXZ）','-','TR1_BOSS',2,'-')	


SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN


SELECT * FROM etl_load_table_map WHERE UPPER(BOSS_TABLE_NAME) LIKE '%XZJF%'
AND task_id like 'A%'
ORDER BY 1

select substr(task_id,2) from etl_load_table_map  	
where table_name_templet like '%_PRODUCT_%'	
order by 1	

select * from 

/**
 * 
 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc



select count(0) from DWD_NG2_I03013_20101020
select count(0) from DWD_NG2_M03023_201010


select * from DWD_NG2_I03028_20101020


select count(0) from DWD_NG2_I03028_yyyymmdd

select count(0) from DWD_NG2_I03028_20101020
insert into etl_load_table_map values('I03028','DWD_NG2_I03028_YYYYMMDD','渠道扩展信息',0,'channel.SCHEME_STORE_INFO')
insert into USYS_TABLE_MAINTAIN values(10185,'渠道扩展信息','DWD_NG2_I03028','1','day',255,'0','','','BASS2','DWD_NG2_I03028_YYYYMMDD','TBS_3H','TBS_INDEX','CHANNEL_ID',1)
insert into app.sch_control_task values ('TR1_L_03028',1,2,'DWD_NG2_I03028_YYYYMMDD',0,-1,'渠道扩展信息','-','TR1_BOSS',2,'-')


					
/**	2010-11-9 11:27	added by  panzhiwei		**/	
--DROP TABLE DWD_NG2_I03028_YYYYMMDD					
CREATE TABLE DWD_NG2_I03028_20101020 (					
        CHANNEL_ID          BIGINT              --渠道编码
        ,USED_SEAT_NUM      INTEGER             --台席数量
        ,EMPLOYEE_NUM       BIGINT              --员工数量
        ,BUILD_AREA         INTEGER             --建筑面积
        ,USE_AREA           INTEGER             --使用面积
        ,AD_AREA            INTEGER             --宣传区域面积
        ,EQUIP_NUM          BIGINT                --设备数
        ,LEGAL_PERSON       VARCHAR(20)           --联系人
        ,TEL_NUMBER         VARCHAR(20)         --联系电话
        ,BUSI_TYPE          VARCHAR(20)         --办理业务类型
        ,USAGE_TYPE         VARCHAR(20)         --功能界定
        ,HOUSE_EXPIRE       TIMESTAMP           --租房期限
        ,HOUSE_BUY_PRICE    BIGINT              --房屋购买金额
        ,HIRE_COST          BIGINT              --房屋租金
        ,BUY_EQUIP_SUM      INTEGER             --设备购买金额
        ,EQUIP_RENT_PRICE   BIGINT              --设备租金
        ,FITMENT_PRICE      INTEGER             --装修投资总额
        ,OFFICE_COST        BIGINT              --办公家具投资总额
        ,FITMENT_EXPIRE     SMALLINT            --装修投资折旧年限
        ,FURNITURE_EXPIRE   SMALLINT            --办公家具折旧年限
        ,FITMENT_VALID      TIMESTAMP           --装修投资生效时间
        ,FURNITURE_VALID    TIMESTAMP           --办公家具生效时间
        ,WATER_COST         BIGINT                  --水费
        ,WARM_FEE           INTEGER               --取暖费
        ,ELEC_COST          BIGINT                  --电费
        ,MANUAL_COST        BIGINT              --人工成本总额
        ,OTHER_COST         BIGINT              --其他日常费用总额
        ,BUSI_CONSULT_RATE  BIGINT              --咨询业务量占总业务量比值
        ,BUSI_COMPLAINT_RATE BIGINT              --投诉业务量占总业务量比值
        ,DOOR_HEIGHT        BIGINT              --门头高度
        ,DOOR_WIDTH         BIGINT              --门头长度
        ,WINDOW_AREA        INTEGER             --临街橱窗面积
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING
 
select * from DWD_NG2_M03023_201010

select	*	from	etl_task_running
where	task_id	in	('I03027')	
where	error_sheet_cnt	is	nu	
order	by	stime	desc	
 select * from bass2.DWD_PRODUCT_BUSI_20100601
 
 
 select * from DWD_NG2_I03027_20101020
 --drop table DWD_NG2_I03027_20101020
 select * from DWD_NG2_I03027_20101020
 
 select * from  SYSCAT.TABLES where tabname like '%DWD_CUST_MANAGER_INFO_%'
 

select count(0) from bass2.DWD_CUST_MANAGER_INFO_20100531

select * from bass2.DWD_CUST_MANAGER_INFO_20100531



/**

insert into etl_load_table_map values('I03027','DWD_NG2_I03027_YYYYMMDD','店员积分信息',0,'channel.CHNL_OPASSESS_DTL')
insert into USYS_TABLE_MAINTAIN values(10184,'店员积分信息','DWD_NG2_I03027','1','day',255,'0','','','BASS2','DWD_NG2_I03027_YYYYMMDD','TBS_3H','TBS_INDEX','OPASSESS_DTL_ID',1)
insert into app.sch_control_task values ('TR1_L_03027',1,2,'DWD_NG2_I03027_YYYYMMDD',0,-1,'店员积分信息','-','TR1_BOSS',2,'-')


DROP TABLE DWD_NG2_I03027_20101020
CREATE TABLE DWD_NG2_I03027_20101020 (				
        OPER_ID             BIGINT              		--员工标识
        ,ASSESS_CODE        VARCHAR(20)        		  --店员积分业务类型标识
        ,ASSESS_SCORE       INTEGER                 --积分
        ,DONE_DATE          TIMESTAMP               --时间
        ,OPASSESS_DTL_ID	  BIGINT									--积分明细编号
 )		
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( OPASSESS_DTL_ID )				
 USING HASHING


				
/**	2010-11-8 10:51	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I03027_20101020 (				
        OPER_ID             BIGINT              		--员工标识
        ,ASSESS_CODE        VARCHAR(20)        		  --店员积分业务类型标识
        ,ASSESS_SCORE       INTEGER                 --积分
        ,DONE_DATE          TIMESTAMP               --时间
 )		
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( OPER_ID )				
 USING HASHING
select * from CHNL_OPASSESS_DTL 


select	*	from	BASS2.ETL_TASK_RUNNING		
where	task_id	in	('M03023')		



 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc






select	*	from	etl_task_log		
where	task_id	in	('M03023')	


select count(0) , count(distinct channel_id) from DWD_NG2_M03023_201010

	
where	error_sheet_cnt	is	null	
order	by	stime	desc		
					
select	*	from	BASS2.ETL_TASK_RUNNING		
where	task_id	in	('M03023')		


insert into etl_load_table_map values('M03023','DWD_NG2_M03023_YYYYMM','渠道位置属性信息',0,'channel.self_channel')
INSERT INTO USYS_TABLE_MAINTAIN 
VALUES(
   10183,
   '渠道位置属性信息',
   'DWD_NG2_M03023',
   '1',
   'month',
   255,
   '0',
   '',
   '',
   'BASS2',
   'DWD_NG2_M03023_YYYYMM',
   'TBS_3H',
   'TBS_INDEX',
   'CHANNEL_ID',
   1
   )
INSERT INTO app.sch_control_task 
VALUES(
   'TR1_L_03023',
   2,
   2,
   'DWD_NG2_M03023_YYYYMM',
   0,
   - 1,
   '渠道位置属性信息',
   '-',
   'TR1_BOSS',
   2,
   '-'
   )
SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN

select * from syscat.tables where tabname like 'DWD_NG2_I03013%'




/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_YYYYMMDD (					
        CHANNEL_ID          BIGINT              --渠道标识:
        ,CHANNEL_NAME        VARCHAR(100)        --渠道名称:
        ,PARENT_CHANNEL_ID   INTEGER             --上级渠道标识:
        ,CHANNEL_LEVEL       INTEGER             --渠道级别:与“渠道级别”接口单元对应
        ,CHANNEL_DESC          VARCHAR(200)        --渠道描述:
        ,START_DATE          TIMESTAMP           --启用日期:
        ,EXPIRE_DATE         TIMESTAMP           --截止日期:缺省29991231
        ,REGION_CODE         VARCHAR(8)          --归属区域编号:与“地域08039”接口单元对应
        ,ORGANIZE_ID         BIGINT              --所属单位标识:与“单位”接口单元对应
        ,CHANNEL_TYPE        INTEGER             --渠道类别:与“渠道类型”接口单元对应
        ,PROPERTY_SRC_TYPE   INTEGER             --渠道物业形态:包括上市公司够建、存续企业购建、租赁、转租-店中店
        ,REG_FUND            BIGINT              --投资规模:
        ,OPEN_DATE           TIMESTAMP           --开业时间:
        ,RESPNSR_ID             INTEGER             --渠道负责人编号:与“渠道负责人”接口单元对应
        ,TEL_NUMBER          VARCHAR(20)         --渠道办公电话:
        ,FAX_NUMBER          VARCHAR(20)         --传真号码:
        ,POST_CODE           VARCHAR(20)            --邮编:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --营业时间:
        ,CHANNEL_STYLE       INTEGER             --渠道运营方式:"0-自建自营（自办）；1-自建他营（合作）；2-他建他营（特许）"
        ,STAR_LEVEL       INTEGER             --渠道星级:"0-一星级；1-二星级"
        ,CHANNEL_USER_ID     INTEGER             --渠道租赁人编号:与“渠道租赁人”接口单元对应
        ,OWNER_TYPE          SMALLINT            --物业购置方式:
        ,PWNER_PRICE         INTEGER             --物业购置价格:
        ,FITMENT_PRICE       INTEGER             --装修投入:
        ,TRANSFER_PRICE      INTEGER             --传输投入:
        ,INVESTOR            VARCHAR(20)         --投资主体:
        ,LICENSE_NUMBER      VARCHAR(20)          --工商号:
        ,INTERNET_MODE       INTEGER             --接入方式:"0-光缆；1-2M电缆；2-GPRS；3-CDS；4-拨号上网；5-无线网桥；6-无"
        ,CREATE_DATE         TIMESTAMP           --建档时间:
        ,CR_OP_ID            BIGINT              --建档员工:
        ,OP_ID               BIGINT              --操作员工:
        ,DONE_DATE           TIMESTAMP           --操作时间:
        ,CHANNEL_STATE       SMALLINT            --渠道状态:"0-正常；1-预解约；2-注销"
        ,NOTES               VARCHAR(200)            --备注
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING
/**	2010-11-5 15:21	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_M03023_201010(
   CHANNEL_ID BIGINT --渠道标识
   ,
   STORE_AREA INTEGER --营业厅面积
   ,
   GEOGRAPHY_PROPERTY INTEGER --地理位置属性
   ,
   LONGITUDE BIGINT --经度
   ,
   LATITUDE BIGINT --纬度
   ,
   COVER_BUSI_REGION_CODE VARCHAR(20) --渠道覆盖业务区
   ,
   COVER_radius INTEGER --服务半径
   ,
   POEPLE_NUM BIGINT --人口覆盖
   ,
   POPUL_DENSITY INTEGER --人口密度
   ,
   CUST_NUM BIGINT --客户覆盖
   ,
   EMPLOYEE_NUM INTEGER --雇员数目
   ,
   SALES_NUM INTEGER --营业员人数
   ,
   EQUIP_NUM INTEGER --终端数目
   ,
   EQUIP_STATUS INTEGER --终端状况
   ,
   CHANNEL_ADDRESS VARCHAR(120) --详细地址
   ,
   plane_graph INTEGER --营业厅平面结构图
   ,
   BOARD_HEIGHT INTEGER --店招高度（米）
   ,
   BOARD_WIDTH INTEGER --店招宽度（米）
   ,
   WALL_HEIGHT INTEGER --形象墙高度
   ,
   WALL_WIDTH INTEGER --形象墙宽度
   ,
   CREATE_DATE TIMESTAMP --建档日期
   ,
   CREATE_OP_ID BIGINT --建档员工
   ,
   DONE_DATE TIMESTAMP --操作日期
   ,
   OP_ID BIGINT --操作员工
) DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    CHANNEL_ID 
 ) USING HASHING
select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc

/**
 select * from etl_task_running 
where	task_id	in	('I03013')	

select * from etl_task_log  
where	task_id	in	('I03013')	

select * from DWD_NG2_I03013_20101020 with ur


select count(0) , count(distinct channel_id) from DWD_NG2_I03013_20101020 where channel_id is null


select 

select a.*
from DWD_NG2_I03013_20101020 a 
join (select channel_id , count(0) cnt from DWD_NG2_I03013_20101020 group by 
channel_id having count(0) >1)  b 
on a.channel_id = b.channel_id



select * from DWD_NG2_I03013_20101020

select * from etl_task_running 
where	task_id	in	('I03013')	

/**	2010/11/2 18:02	added by  panzhiwei		**/	
-- drop table DWD_NG2_I03013_20101020 

CREATE TABLE DWD_NG2_I03013_20101020 (					
        CHANNEL_ID          BIGINT              --渠道标识:
        ,CHANNEL_NAME        VARCHAR(100)        --渠道名称:
        ,PARENT_CHANNEL_ID   INTEGER             --上级渠道标识:
        ,CHANNEL_LEVEL       INTEGER             --渠道级别:与“渠道级别”接口单元对应
        ,CHANNEL_DESC          VARCHAR(200)        --渠道描述:
        ,START_DATE          TIMESTAMP           --启用日期:
        ,EXPIRE_DATE         TIMESTAMP           --截止日期:缺省29991231
        ,REGION_CODE         VARCHAR(8)          --归属区域编号:与“地域08039”接口单元对应
        ,ORGANIZE_ID         BIGINT              --所属单位标识:与“单位”接口单元对应
        ,CHANNEL_TYPE        INTEGER             --渠道类别:与“渠道类型”接口单元对应
        ,PROPERTY_SRC_TYPE   INTEGER             --渠道物业形态:包括上市公司够建、存续企业购建、租赁、转租-店中店
        ,REG_FUND            BIGINT              --投资规模:
        ,OPEN_DATE           TIMESTAMP           --开业时间:
        ,RESPNSR_ID             INTEGER             --渠道负责人编号:与“渠道负责人”接口单元对应
        ,TEL_NUMBER          VARCHAR(20)         --渠道办公电话:
        ,FAX_NUMBER          VARCHAR(20)         --传真号码:
        ,POST_CODE           VARCHAR(20)            --邮编:
        ,BUSI_BEGIN_TIME     VARCHAR(100)         --营业时间:
        ,CHANNEL_STYLE       INTEGER             --渠道运营方式:"0-自建自营（自办）；1-自建他营（合作）；2-他建他营（特许）"
        ,STAR_LEVEL       INTEGER             --渠道星级:"0-一星级；1-二星级"
        ,CHANNEL_USER_ID     INTEGER             --渠道租赁人编号:与“渠道租赁人”接口单元对应
        ,OWNER_TYPE          SMALLINT            --物业购置方式:
        ,PWNER_PRICE         INTEGER             --物业购置价格:
        ,FITMENT_PRICE       INTEGER             --装修投入:
        ,TRANSFER_PRICE      INTEGER             --传输投入:
        ,INVESTOR            VARCHAR(20)         --投资主体:
        ,LICENSE_NUMBER      VARCHAR(20)          --工商号:
        ,INTERNET_MODE       INTEGER             --接入方式:"0-光缆；1-2M电缆；2-GPRS；3-CDS；4-拨号上网；5-无线网桥；6-无"
        ,CREATE_DATE         TIMESTAMP           --建档时间:
        ,CR_OP_ID            BIGINT              --建档员工:
        ,OP_ID               BIGINT              --操作员工:
        ,DONE_DATE           TIMESTAMP           --操作时间:
        ,CHANNEL_STATE       SMALLINT            --渠道状态:"0-正常；1-预解约；2-注销"
        ,NOTES               VARCHAR(200)            --备注
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING
select * from DWD_NG2_I01035_20101023
select tabname from syscat.tables where tabname like '%DWD_NG2_I01035%'
order by 1


/**
 * 
 * 
insert into etl_load_table_map values('I03013','DWD_NG2_I03013_YYYYMMDD','渠道信息表',0,'channel.channel_info')
insert into USYS_TABLE_MAINTAIN values(10182,'渠道信息表','DWD_NG2_I03013','1','day',255,'0','','','BASS2','DWD_NG2_I03013_YYYYMMDD','TBS_3H','TBS_INDEX','CHANNEL_ID',1)
insert into app.sch_control_task values ('TR1_L_03013',1,2,'DWD_NG2_I03013_YYYYMMDD',0,-1,'渠道信息表','-','TR1_BOSS',2,'-')

 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc



SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN

select * from etl_task_log  
where	task_id	in	('I04016')	
/**
select * from DWD_NG2_I03032_20101020
select * from DWD_NG2_I03037_20101020
select trade_id  , count(0) from DWD_NG2_I04016_20101020
group by trade_id 


I04016
insert into etl_load_table_map values('I04016','DWD_NG2_I04016_YYYYMMDD','号码资源表',0,'res.res_phone_num_origin_X')
insert into USYS_TABLE_MAINTAIN values(10181,'号码资源表','DWD_NG2_I04016','1','day',255,'0','','','BASS2','DWD_NG2_I04016_YYYYMMDD','TBS_3H','TBS_INDEX','BILL_ID',1)
insert into app.sch_control_task values ('TR1_L_04016',1,2,'DWD_NG2_I04016_YYYYMMDD',0,-1,'号码资源表','-','TR1_BOSS',2,'-')



/**2010-10-29 15:54	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I04016_20101020 (				
		 BILL_ID				VARCHAR(20)			--手机号码 		
		,USE_DATE				TIMESTAMP			--启用时间 		
		,DONE_DATE				TIMESTAMP			--入库时间 		
		,RES_STATUS				SMALLINT			--号码状态 如待分配（库存）、可使用（渠道铺号）、正常使用、保留期、冷冻期、回收待分配等		
		,TRADE_ID				BIGINT				--智能网号码标志 标志该号码是否是智能网用户号码。		
		,ORG_ID					BIGINT				--归属渠道标识 		
		,IMSI					VARCHAR(20)			--IMSI 
		,KEEP_DATE				TIMESTAMP			--保留截止时间 暂为空
		,VALID_DATE				TIMESTAMP			--有效时间 暂为空
		,OP_ID					BIGINT				--归属员工 员工标识
		,grpno					VARCHAR(30)			--分配批号 资源在进行资源划拨时的批号 暂为空
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BILL_ID )
 USING HASHING
--insert into etl_load_table_map values('I04026','DWD_NG2_I04026_YYYYMMDD','VIP卡资源表',0,'market.CM_VIP_INFO')
--insert into USYS_TABLE_MAINTAIN values(10180,'VIP卡资源表','DWD_NG2_I04026','1','day',255,'0','','','BASS2','DWD_NG2_I04026_YYYYMMDD','TBS_3H','TBS_INDEX','VIP_CARD_NO',1)
CREATE TABLE DWD_NG2_I04026_20101020(
   VIP_CARD_NO	 	 	 VARCHAR(30)	 	 	 	 --
   ,
   END_DATE	 	 	 	 TIMESTAMP	 	 	 	 --
   ,
   VIP_CARD_TYPE	 	 	 SMALLINT	 	 	 	 --
   ,
   OWN_ORG_ID	 	 	 	 BIGINT	 	 	 	 	 --
   ,
   CREATE_DATE	 	 	 TIMESTAMP	 	 	 	 --
   ,
   VIP_CARD_STATUS	 	 SMALLINT	 	 	 	 --1－－在用，,0－－失效
   ,
   PRODUCT_INSTANCE_ID BIGINT	 	 	 	 	 --
   ,
   CREATE_OP_ID	 	  BIGINT	 	 	 	 	 --新增该客户信息的操作员工号，如营业员或客户经理等。
   ,
   VALID_DATE	 	 	 	 TIMESTAMP	 	 	 	 --
   ,
   EXPIRE_DATE	 	 	 TIMESTAMP	 	 	 	 --
) DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    VIP_CARD_NO 
 ) USING HASHING
select * from dw_product_no_press_stop_yyyymmdd

SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN



select * from DIM_OPPOSITE_ORGAN


select * from dim_wire_organ


select * from etl_load_table_map  where task_id like '%110%'
order by 1

select max(substr(task_id,2)) from etl_load_table_map  
where table_name_templet like '%_PRODUCT_%'


select substr(task_id,2) from etl_load_table_map  
where table_name_templet like '%_PRODUCT_%'
order by 1


select tabname from syscat.tables where tabname like '%ODS_PRODUCT_ORD_CUST%'
order by 1

SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN


SELECT * FROM USYS_TABLE_MAINTAIN WHERE TABLE_ID = 10177


--insert into etl_load_table_map values('I04015','DWD_NG2_I04015_YYYYMMDD','IMSI资源表',0,'SO.RES_SIM_CARD_USED_X')
--insert into USYS_TABLE_MAINTAIN values(10179,'IMSI资源表','DWD_NG2_I04015','1','day',255,'0','','','BASS2','DWD_NG2_I04015_YYYYMMDD','TBS_3H','TBS_INDEX','IMSI',1)



/**	2010-10-25 17:14	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I04015_20101020 (				
		imsi					VARCHAR(20)				--		
		,icc_id					VARCHAR(25)				--		
		,bill_id				VARCHAR(20)				--服务号码
		,sim_type				VARCHAR(40)				--卡类型(0 普通sim卡、1 usim卡)	
		,state_date				TIMESTAMP				--y		
		,done_date				TIMESTAMP				--y		
		,use_done_code			VARCHAR(25)				--y
		,use_date				TIMESTAMP				--y
		,res_status				SMALLINT				--0已开通；1未开通；2换卡回收3挂失8销户退网
		,org_id					BIGINT					--
		,region_id				VARCHAR(6)				--
		,op_id					BIGINT					--
		,grpno					VARCHAR(30)				--
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( IMSI )
 USING HASHING
select	*	from	etl_task_log		
where	task_id	in	('I04016')		




select * from etl_task_log  
where task_id in ('M11036')


where error_sheet_cnt is nu
order by stime desc

select * from  BASS2.ETL_TASK_RUNNING
where task_id in ('I04026')

select * from  BASS2.ETL_TASK_LOG
where task_id in ('M11036')

select * from app.SCH_CONTROL_MOGRPINFO


select * from dim_pub_city



select * from etl_task_log  
where task_id='A11019'

select * from SCH_CONTROL_ALARM
where task_id='A11019'



SELECT FLAG,COUNT(0)
 FROM APP.SCH_CONTROL_ALARM 
 GROUP BY FLAG
 
 SELECT * FROM  APP.SCH_CONTROL_ALARM  WHERE CONTROL_CODE LIKE '%11019%'
 

--DROP table ODS_PRODUCT_ORD_CUST_YYYYMM
CREATE TABLE ODS_PRODUCT_ORD_CUST_YYYYMM (
		 CUSTOMER_ORDER_ID					BIGINT       				--客户订单ID
		,CUST_PARTY_ROLE_ID        VARCHAR(14)           --客户参与人角色ID
		,ORDER_CODE                VARCHAR(40)           --订单编码
		,ORDER_TYPE                SMALLINT              --订单类型
		,BUSINESS_ID               BIGINT                --业务操作编号
		,CUSTOMER_NAME             VARCHAR(80)           --客户名称
		,REGION_ID                 VARCHAR(6)            --管理区域标识
		,CEASE_REASON              VARCHAR(200)          --撤单原因
		,AGENCY_PERSON             VARCHAR(80)           --经办人
		,CERTIFI_CODE              VARCHAR(50)           --证件号
		,CERTIFI_TYPE_ID           SMALLINT              --证件类型
		,TEL                       VARCHAR(120)          --联系电话
		,PARTNER_ID                BIGINT                --合作伙伴ID
		,OLD_SO_ORDER_ID           BIGINT                --原订单ID
		,BOOKING_HOME_DT           TIMESTAMP             --预约施工上门施工时间
		,BOOKING_OPEN_DT           TIMESTAMP             --预约开通时间
		,WORKFLOW_START_DT         TIMESTAMP             --工作流启动时间
		,IS_FAST_CHANNEL           SMALLINT              --是否绿色通道
		,ORDER_LEVEL               SMALLINT              --订单级别
		,PRE_SO_ID                 VARCHAR(200)          --预受理工单编号
		,VALID_TYPE                SMALLINT              --生效类型
		,STATE                     SMALLINT              --状态
		,ORDER_STATE               SMALLINT              --订单状态
		,NOTES                     VARCHAR(255)          --备注
		,DONE_CODE                 VARCHAR(100)                --受理编号
		,CREATE_DATE               TIMESTAMP             --创建日期
		,DONE_DATE                 TIMESTAMP             --受理日期
		,VALID_DATE                TIMESTAMP             --生效日期
		,EXPIRE_DATE               TIMESTAMP             --失效日期
		,OP_ID                     BIGINT                --操作员
		,ORG_ID                    BIGINT                --组织单元
		,SOURCE_SYSTEM_ID          BIGINT                --来源系统
		,SALE_SOURCE               BIGINT                --销售来源
		,VERIFY_TYPE               VARCHAR(200)          --校验方式
		,PAY_TYPE                  SMALLINT              --缴费方式
		,CHANNEL_TYPE              CHAR(1)               --受理渠道
		,REP_FEE_PHONE_NO          VARCHAR(80)           --代扣目标手机号
		,WRRANT_NO                 VARCHAR(80)           --票据编号
		,PRODUCT_INSTANCE_ID       VARCHAR(20)                --PRODUCT_INSTANCE_ID
		,BILL_ID                   VARCHAR(120)          --BILL_ID
)
	IN TBS_3H
	partitioning key (CUSTOMER_ORDER_ID) using hashing
	not logged initially
SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN



--insert into etl_load_table_map values('M11036','ODS_PRODUCT_ORD_CUST_YYYYMM','客户订单表',0,'SO.ORD_CUST_F_X_YYMM')
--insert into USYS_TABLE_MAINTAIN values(10174,'客户订单表','ODS_PRODUCT_ORD_CUST','1','month',255,'0','','','BASS2','ODS_PRODUCT_ORD_CUST_YYYYMM','TBS_3H','TBS_INDEX','CUSTOMER_ORDER_ID',1)
--insert into app.sch_control_task values ('TR1_L_11036',1,2,'ODS_PRODUCT_ORD_CUST_YYYYMM',0,-1,'客户订单表','-','TR1_BOSS',2,'-')


select * from app.sch_control_task



          select 1 from etl_load_table_map where task_id = 'M11036'
union all select 1 from USYS_TABLE_MAINTAIN where table_id = 10174
union all select 1 from app.sch_control_task where control_code = 'TR1_L_11036'



--DROP table ODS_PRODUCT_ORD_CUST_201010
CREATE TABLE ODS_PRODUCT_ORD_CUST_201010 (
		 CUSTOMER_ORDER_ID					BIGINT       --客户订单ID
		,CUST_PARTY_ROLE_ID        VARCHAR(14)           --客户参与人角色ID
		,ORDER_CODE                VARCHAR(40)           --订单编码
		,ORDER_TYPE                SMALLINT              --订单类型
		,BUSINESS_ID               BIGINT                --业务操作编号
		,CUSTOMER_NAME             VARCHAR(80)           --客户名称
		,REGION_ID                 VARCHAR(6)            --管理区域标识
		,CEASE_REASON              VARCHAR(200)          --撤单原因
		,AGENCY_PERSON             VARCHAR(80)           --经办人
		,CERTIFI_CODE              VARCHAR(50)           --证件号
		,CERTIFI_TYPE_ID           SMALLINT              --证件类型
		,TEL                       VARCHAR(120)          --联系电话
		,PARTNER_ID                BIGINT                --合作伙伴ID
		,OLD_SO_ORDER_ID           BIGINT                --原订单ID
		,BOOKING_HOME_DT           TIMESTAMP             --预约施工上门施工时间
		,BOOKING_OPEN_DT           TIMESTAMP             --预约开通时间
		,WORKFLOW_START_DT         TIMESTAMP             --工作流启动时间
		,IS_FAST_CHANNEL           SMALLINT              --是否绿色通道
		,ORDER_LEVEL               SMALLINT              --订单级别
		,PRE_SO_ID                 VARCHAR(200)          --预受理工单编号
		,VALID_TYPE                SMALLINT              --生效类型
		,STATE                     SMALLINT              --状态
		,ORDER_STATE               SMALLINT              --订单状态
		,NOTES                     VARCHAR(255)          --备注
		,DONE_CODE                 VARCHAR(100)                --受理编号
		,CREATE_DATE               TIMESTAMP             --创建日期
		,DONE_DATE                 TIMESTAMP             --受理日期
		,VALID_DATE                TIMESTAMP             --生效日期
		,EXPIRE_DATE               TIMESTAMP             --失效日期
		,OP_ID                     BIGINT                --操作员
		,ORG_ID                    BIGINT                --组织单元
		,SOURCE_SYSTEM_ID          BIGINT                --来源系统
		,SALE_SOURCE               BIGINT                --销售来源
		,VERIFY_TYPE               VARCHAR(200)          --校验方式
		,PAY_TYPE                  SMALLINT              --缴费方式
		,CHANNEL_TYPE              CHAR(1)               --受理渠道
		,REP_FEE_PHONE_NO          VARCHAR(80)           --代扣目标手机号
		,WRRANT_NO                 VARCHAR(80)           --票据编号
		,PRODUCT_INSTANCE_ID       VARCHAR(20)                --PRODUCT_INSTANCE_ID
		,BILL_ID                   VARCHAR(120)          --BILL_ID
)
	IN TBS_3H
	partitioning key (CUSTOMER_ORDER_ID) using hashing
	not logged initially
--DROP table ODS_PRODUCT_ORD_CUST_201009
CREATE TABLE ODS_PRODUCT_ORD_CUST_201009(
   CUSTOMER_ORDER_ID	 	 	 	 	 BIGINT --客户订单ID
   ,
   CUST_PARTY_ROLE_ID VARCHAR(14) --客户参与人角色ID
   ,
   ORDER_CODE VARCHAR(40) --订单编码
   ,
   ORDER_TYPE SMALLINT --订单类型
   ,
   BUSINESS_ID BIGINT --业务操作编号
   ,
   CUSTOMER_NAME VARCHAR(80) --客户名称
   ,
   REGION_ID VARCHAR(6) --管理区域标识
   ,
   CEASE_REASON VARCHAR(200) --撤单原因
   ,
   AGENCY_PERSON VARCHAR(80) --经办人
   ,
   CERTIFI_CODE VARCHAR(50) --证件号
   ,
   CERTIFI_TYPE_ID SMALLINT --证件类型
   ,
   TEL VARCHAR(120) --联系电话
   ,
   PARTNER_ID BIGINT --合作伙伴ID
   ,
   OLD_SO_ORDER_ID BIGINT --原订单ID
   ,
   BOOKING_HOME_DT TIMESTAMP --预约施工上门施工时间
   ,
   BOOKING_OPEN_DT TIMESTAMP --预约开通时间
   ,
   WORKFLOW_START_DT TIMESTAMP --工作流启动时间
   ,
   IS_FAST_CHANNEL SMALLINT --是否绿色通道
   ,
   ORDER_LEVEL SMALLINT --订单级别
   ,
   PRE_SO_ID VARCHAR(200) --预受理工单编号
   ,
   VALID_TYPE SMALLINT --生效类型
   ,
   STATE SMALLINT --状态
   ,
   ORDER_STATE SMALLINT --订单状态
   ,
   NOTES VARCHAR(255) --备注
   ,
   DONE_CODE VARCHAR(100) --受理编号
   ,
   CREATE_DATE TIMESTAMP --创建日期
   ,
   DONE_DATE TIMESTAMP --受理日期
   ,
   VALID_DATE TIMESTAMP --生效日期
   ,
   EXPIRE_DATE TIMESTAMP --失效日期
   ,
   OP_ID BIGINT --操作员
   ,
   ORG_ID BIGINT --组织单元
   ,
   SOURCE_SYSTEM_ID BIGINT --来源系统
   ,
   SALE_SOURCE BIGINT --销售来源
   ,
   VERIFY_TYPE VARCHAR(200) --校验方式
   ,
   PAY_TYPE SMALLINT --缴费方式
   ,
   CHANNEL_TYPE CHAR(1) --受理渠道
   ,
   REP_FEE_PHONE_NO VARCHAR(80) --代扣目标手机号
   ,
   WRRANT_NO VARCHAR(80) --票据编号
   ,
   PRODUCT_INSTANCE_ID VARCHAR(20) --PRODUCT_INSTANCE_ID
   ,
   BILL_ID VARCHAR(120) --BILL_ID
) 	
	IN TBS_3H 	partitioning KEY(
   CUSTOMER_ORDER_ID
)USING hashing 	
	NOT logged initially
--DROP table ODS_PRODUCT_ORD_CUST_201007
CREATE TABLE ODS_PRODUCT_ORD_CUST_201007(
   CUSTOMER_ORDER_ID	 	 	 	 	 BIGINT --客户订单ID
   ,
   CUST_PARTY_ROLE_ID VARCHAR(14) --客户参与人角色ID
   ,
   ORDER_CODE VARCHAR(40) --订单编码
   ,
   ORDER_TYPE SMALLINT --订单类型
   ,
   BUSINESS_ID BIGINT --业务操作编号
   ,
   CUSTOMER_NAME VARCHAR(80) --客户名称
   ,
   REGION_ID VARCHAR(6) --管理区域标识
   ,
   CEASE_REASON VARCHAR(200) --撤单原因
   ,
   AGENCY_PERSON VARCHAR(80) --经办人
   ,
   CERTIFI_CODE VARCHAR(50) --证件号
   ,
   CERTIFI_TYPE_ID SMALLINT --证件类型
   ,
   TEL VARCHAR(120) --联系电话
   ,
   PARTNER_ID BIGINT --合作伙伴ID
   ,
   OLD_SO_ORDER_ID BIGINT --原订单ID
   ,
   BOOKING_HOME_DT TIMESTAMP --预约施工上门施工时间
   ,
   BOOKING_OPEN_DT TIMESTAMP --预约开通时间
   ,
   WORKFLOW_START_DT TIMESTAMP --工作流启动时间
   ,
   IS_FAST_CHANNEL SMALLINT --是否绿色通道
   ,
   ORDER_LEVEL SMALLINT --订单级别
   ,
   PRE_SO_ID VARCHAR(200) --预受理工单编号
   ,
   VALID_TYPE SMALLINT --生效类型
   ,
   STATE SMALLINT --状态
   ,
   ORDER_STATE SMALLINT --订单状态
   ,
   NOTES VARCHAR(255) --备注
   ,
   DONE_CODE VARCHAR(100) --受理编号
   ,
   CREATE_DATE TIMESTAMP --创建日期
   ,
   DONE_DATE TIMESTAMP --受理日期
   ,
   VALID_DATE TIMESTAMP --生效日期
   ,
   EXPIRE_DATE TIMESTAMP --失效日期
   ,
   OP_ID BIGINT --操作员
   ,
   ORG_ID BIGINT --组织单元
   ,
   SOURCE_SYSTEM_ID BIGINT --来源系统
   ,
   SALE_SOURCE BIGINT --销售来源
   ,
   VERIFY_TYPE VARCHAR(200) --校验方式
   ,
   PAY_TYPE SMALLINT --缴费方式
   ,
   CHANNEL_TYPE CHAR(1) --受理渠道
   ,
   REP_FEE_PHONE_NO VARCHAR(80) --代扣目标手机号
   ,
   WRRANT_NO VARCHAR(80) --票据编号
   ,
   PRODUCT_INSTANCE_ID VARCHAR(20) --PRODUCT_INSTANCE_ID
   ,
   BILL_ID VARCHAR(120) --BILL_ID
) 	
	IN TBS_3H 	partitioning KEY(
   CUSTOMER_ORDER_ID
)USING hashing 	
	NOT logged initially
/**
/**
/**
/**
/**
/**
/**
/**
/**
/**

/**
/**
/**
/**
/**
/**
/**

/**
--drop table ORD_BATCH_DATA_DETAIL_F_20101015
--drop table ORD_BATCH_DATA_DETAIL_F_yyyymmdd

-- 2010-10-18 16:41 PANZHIWEI ADD
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015 ( 
		 BATCH_DATA_DETAIL_ID		bigint		
		,BATCH_DATA_ID				bigint				
		,QUEUE_ID					varchar(20)				--	队列ID
		,BUSINESS_ID				bigint					--	操作ID
		,CONFIG_ID					bigint					--	配置ID
		,BUSI_ORDER_ID				bigint					--	操作订单ID
		,BILL_ID					varchar(30)				--	手机号码
		,FILE_CONTENT				VARCHAR(1024)			--	文件内容
		,SUCC_FLAG					smallint				--	成功标志
		,ERROR_MSG					VARCHAR(1000)			--	错误信息
		,ERROR_STACK				VARCHAR(4000)			--	错误栈信息
		,DONE_CODE					bigint					--	操作编码
		,DONE_DATE					TIMESTAMP				--	结束日志
		,CREATE_DATE				TIMESTAMP				--	创建时间
		,VALID_DATE					TIMESTAMP				--	生效时间
		,EXPIRE_DATE				TIMESTAMP				--	失效时间
		,OP_ID						bigint					--	操作员ID
		,ORG_ID						bigint					--	操作员组织ID
		,REGION_ID					VARCHAR(6)				--	地区编码
 )
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( BATCH_DATA_DETAIL_ID )
 USING HASHING
 
 
 insert into etl_load_table_map values('A11034','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','普通业务批量办理表',0,'SO.ORD_BATCH_DATA_DETAIL_F_YYMM')

 

select * from etl_load_table_map where task_id = 'A11034'



select * from etl_task_log  where task_id='I04142' order by stime desc



describe table app.sch_control_task


select * from app.sch_control_task


insert into app.sch_control_task values ('TR1_L_14101',1,2,'ODS_CHANNEL_INFO_YYYYMMDD',0,-1,'渠道','-','TR1_BOSS',2,'-')


delete from app.sch_control_task  where control_code='TR1_L_14101'




insert into app.sch_control_task values ('TR1_L_11035',1,2,'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD',0,-1,'梦网批量处理历史表','-','TR1_BOSS',2,'-')
insert into app.sch_control_task values ('TR1_L_11034',1,2,'ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD',0,-1,'普通业务批量办理表','-','TR1_BOSS',2,'-')



select * from app.sch_control_task where control_code like 'TR1_L_1103%'

select * from USYS_TABLE_MAINTAIN 
where table_id >= 10160
order by 1


select max(table_id) from USYS_TABLE_MAINTAIN



insert into USYS_TABLE_MAINTAIN values(10171,'普通业务批量办理表','ORD_BATCH_DATA_DETAIL_F','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','TBS_3H','TBS_INDEX','BATCH_DATA_DETAIL_ID',1)


insert into USYS_TABLE_MAINTAIN values(10173,'梦网批量处理历史表','ODS_ORD_DSMP_BAT_DATA_HIS','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD','TBS_3H','TBS_INDEX','DATA_ID',1)
insert into USYS_TABLE_MAINTAIN values(10172,'普通业务批量办理表','ORD_BATCH_DATA_DETAIL_F','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','TBS_3H','TBS_INDEX','BATCH_DATA_DETAIL_ID',1)



select * from app.sch_control_task where control_code like 'TR1_L_1103%'




select count(0) from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101014
--drop table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101014

CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101014 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd

CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101016
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101017
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101018
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015
CREATE TABLE ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015


select tabname from syscat.tables where tabname like 'ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20%' order by 1


create table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101001 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd in tbs_3h index in TBS_INDEX   PARTITIONING KEY ( BATCH_DATA_DETAIL_ID ) USING HASHING



select tabname from syscat.tables where tabname like 'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_201010%' order by 1

select tabname from syscat.tables where tabname like 'ETL_TASK%' order by 1

 
select count(0) from ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_20101017
select count(0) from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015


ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101015

select * from etl_task_log  where task_id IN ('A11034','A11034') order by stime desc


select * from etl_task_log  where task_id IN ('A11034','A11034') 
and error_sheet_cnt < 0
order by stime desc


select * from etl_task_log  
where task_id in ('A11034')
where error_sheet_cnt is nu
order by stime desc

select * from  BASS2.ETL_TASK_RUNNING
where task_id in ('I04142')



select task_id,cycle_id from etl_task_log  
where task_id IN ('A11034','A11035') 
and error_sheet_cnt <0
except
select task_id,cycle_id from etl_task_log  
where task_id IN ('A11034','A11034') 
and error_sheet_cnt = 0 


select * from etl_task_log where task_id = 'A11034'
and cycle_id = '20101011'


select count(0) from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101011


select count(0) from  where 


select *
from syscat.tables where 
tabname like '%SCH_CONTROL%'
or 
tabname like '%ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS%2010%'


create table ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101011 like ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_yyyymmdd in tbs_3h index in TBS_INDEX   PARTITIONING KEY ( BATCH_DATA_DETAIL_ID ) USING HASHING




select * from app.sch_control_alarm


select * from sy


select * from app.sch_control_runlog 



select * from dim_cust_agelev



select * from dim_cust_consumelev


select * from dim_cust_data_consumelev

select * from dim_cust_conduration

select * from dim_cust_callcounts
**/
/**	2010-10-21 9:47	added by  panzhiwei **/
/**
CREATE TABLE DWD_NG2_I03032_YYYYMMDD (		
		 MISTAKE_ID				INTEGER					--SEQ_PARTNER_MISTAKEID产生
		,MISTAKE_TYPE			SMALLINT				--
		,MISTAKE_DATE			TIMESTAMP				--
		,MISTAKE_DESP			VARCHAR(128)			--
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( MISTAKE_ID )
 USING HASHING
 
 select min(table_id)  from USYS_TABLE_MAINTAIN
 
 
 select count(0) from etl_load_table_map
 
 select count(0) from app.sch_control_task
 
 select count(0) from  USYS_TABLE_MAINTAIN 
 
 


select tabname from syscat.tables where tabname like 'ODS_PRODUCT_INS_PROD%'

select count(0) from ODS_PRODUCT_INS_PROD_20101019


select count(0) from ODS_PRODUCT_INS_PROD_20101018


insert into etl_load_table_map values('I03032','DWD_NG2_I03032_YYYYMMDD','违规信息表',0,'channel.channel_mistake_info')

insert into app.sch_control_task values ('TR1_L_03032',1,2,'DWD_NG2_I03032_YYYYMMDD',0,-1,'违规信息表','-','TR1_BOSS',2,'-')

delete from USYS_TABLE_MAINTAIN where table_id = 10174 

insert into USYS_TABLE_MAINTAIN values(10175,'违规信息表','DWD_NG2_I03032','1','day',255,'0','','','BASS2','DWD_NG2_I03032_YYYYMMDD','TBS_3H','TBS_INDEX','MISTAKE_ID',1)


select * from USYS_TABLE_MAINTAIN where table_id = 10174






/**	2010-10-21 9:47	added by  panzhiwei **/
/**
CREATE TABLE DWD_NG2_I03032_20101020 (		
		 MISTAKE_ID				INTEGER					--SEQ_PARTNER_MISTAKEID产生
		,MISTAKE_TYPE			SMALLINT				--
		,MISTAKE_DATE			TIMESTAMP				--
		,MISTAKE_DESP			VARCHAR(128)			--
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( MISTAKE_ID )
 USING HASHING
 
 **/
/**
select *
from syscat.tables where 
tabname like '%DWD_NG2_I03032_20101020%'


select count(0) from 
ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101021



 select * from etl_task_log  where task_id='I03032' order by stime desc
 



select * from DWD_NG2_I03032_20101020



**/
/**	2010-10-21 15:10	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I03034_YYYYMMDD(
   TOTAL_VALUE	  TIMESTAMP	  --根据每次成本明细累加		
   ,
   OP_ID	  BIGINT	  --		
   ,
   DONE_DATE	  TIMESTAMP	  --		
   ,
   COST_TYPE_ID	  INTEGER	  --		
   ,
   COST_ID	  BIGINT	  --		
   ,
   CHANNEL_ID	  BIGINT	  --		
)DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    COST_ID 
 )USING HASHING
/**	2010-10-21 15:10	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I03034_20101020(
   TOTAL_VALUE	  TIMESTAMP	  --根据每次成本明细累加		
   ,
   OP_ID	  BIGINT	  --		
   ,
   DONE_DATE	  TIMESTAMP	  --		
   ,
   COST_TYPE_ID	  INTEGER	  --		
   ,
   COST_ID	  BIGINT	  --		
   ,
   CHANNEL_ID	  BIGINT	  --		
)DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    COST_ID 
 )USING HASHING
select max(table_id) from USYS_TABLE_MAINTAIN
 
 
  /**	2010-10-21 15:10	added by  panzhiwei		**/
 -- drop table DWD_NG2_I03034_20101020
CREATE TABLE DWD_NG2_I03034_20101020 (				
		COST_ID					INTEGER	--
		,CHANNEL_ID				BIGINT	--
		,COST_TYPE_ID			INTEGER	--
		,OP_ID					BIGINT	--
		,DONE_DATE				TIMESTAMP	--
		,TOTAL_VALUE			decimal(12,2)	--根据每次成本明细累加
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( COST_ID )
 USING HASHING
/**
 
 insert into etl_load_table_map values('I03034','DWD_NG2_I03034_YYYYMMDD','渠道成本表',0,'channel.channel_cost')
 
insert into USYS_TABLE_MAINTAIN values(10175,'渠道成本表','DWD_NG2_I03034','1','day',255,'0','','','BASS2','DWD_NG2_I03034_YYYYMMDD','TBS_3H','TBS_INDEX','COST_ID',1)

select * from USYS_TABLE_MAINTAIN where table_id > 10170


delete from USYS_TABLE_MAINTAIN where table_id = 10174 

insert into USYS_TABLE_MAINTAIN values(10174,'违规信息表','DWD_NG2_I03032','1','day',255,'0','','','BASS2','DWD_NG2_I03032_YYYYMMDD','TBS_3H','TBS_INDEX','MISTAKE_ID',1)

**/




select * from etl_load_table_map where task_id like 'I0303%'



select count(0) from channel.channel_cost




select * from DWD_NG2_I03034_20101020

 
/**	2010-10-21 16:25	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I03036_20101020 (				
		CONTRACT_ID				BIGINT				--		
		,CONTRACT_KEY			VARCHAR(200)		--由于合同的具体文本信息不可能在数据库种记录，是以文本文件的形式保存于文件系统的。现为了便于模糊同关键字保存于此。
		,CONTRACT_EXPIRE_DATE	TIMESTAMP			--		
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CONTRACT_ID )
 USING HASHING
/**

insert into etl_load_table_map values('I03036','DWD_NG2_I03036_YYYYMMDD','渠道协议信息表',0,'channel.contract_info')
insert into USYS_TABLE_MAINTAIN values(10176,'渠道协议信息表','DWD_NG2_I03036','1','day',255,'0','','','BASS2','DWD_NG2_I03036_YYYYMMDD','TBS_3H','TBS_INDEX','CONTRACT_ID',1)




select * from etl_load_table_map where task_id = 'I03036'

select * from USYS_TABLE_MAINTAIN where table_id = 10176


select * from 

**/
/**	2010-10-21 17:50	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I03037_20101020(
   CHANNEL_ID	  BIGINT	  --		
   ,
   CONTRACT_ID	  BIGINT	  --		
   ,
   DEPOSIT_TYPE	  BIGINT	  --code=8010001.渠道保证金2.企业保证金3.人员保证金
   ,
   DEPOSIT_NUM	  BIGINT	  --以元为单位		
   ,
   OP_ID	  BIGINT	  --		
   ,
   DONE_DATE	  TIMESTAMP	  --		
)DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    CHANNEL_ID 
 )USING HASHING
/**

insert into etl_load_table_map values('I03037','DWD_NG2_I03037_YYYYMMDD','渠道保证金信息',0,'channel.channel_deposit_info')
insert into USYS_TABLE_MAINTAIN values(10177,'渠道保证金信息','DWD_NG2_I03037','1','day',255,'0','','','BASS2','DWD_NG2_I03037_YYYYMMDD','TBS_3H','TBS_INDEX','CHANNEL_ID',1)









/**	2010-10-22 15:48	added by  panzhiwei		**/
CREATE TABLE DWD_NG2_I04014_YYYYMMDD(
   icc_id	  VARCHAR(25)	  --
   ,
   imsi	  VARCHAR(20)	  --
   ,
   busi_code	  INTEGER	  --
   ,
   res_code	  VARCHAR(14)	  --sim卡对应的资源型号
   ,
   region_id	  VARCHAR(6)	  --
   ,
   res_status	  SMALLINT	  --0已开通；1未开通；2换卡回收3挂失8销户退网
   ,
   firmid	  VARCHAR(60)	  --
   ,
   sim_pre_date	  TIMESTAMP	  --预占资源 时，用时间做标记
   ,
   purpose	  CHAR(1)	  --0：表示指定用途1：积分兑换的资源2：赠品，礼品3：单独销售资源4：捆绑销售
   ,
   use_date	  TIMESTAMP	  --
   ,
   state_date	  TIMESTAMP	  --
   ,
   org_id	  BIGINT	  --
   ,
   sn	  VARCHAR(30)	  --sim卡序列号
   ,
   sim_type	  VARCHAR(40)	  --卡类型(0 普通sim卡、1 usim卡)需要远程写卡1	不需要远程写卡
   ,
   ki	  VARCHAR(40)	  --
   ,
   pin1	  VARCHAR(8)	  --
   ,
   pin2	  VARCHAR(8)	  --
   ,
   puk1	  VARCHAR(8)	  --
   ,
   puk2	  VARCHAR(8)	  --
   ,
   bill_id	  VARCHAR(20)	  --服务号码
   ,
   done_date	  TIMESTAMP	  --
   ,
   op_id	  BIGINT	  --
   ,
   grpno	  VARCHAR(30)	  --
)DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    IMSI 
 )USING HASHING
select * from DWD_NG2_I03037_20101020
select * from  BASS2.ETL_TASK_RUNNING
where task_id in ('I04142')

**/




select * from DWD_NG2_I04014_20101020








/**	2010-10-22 15:48	added by  panzhiwei		**/
DROP TABLE DWD_NG2_I04014_20101020 
CREATE TABLE DWD_NG2_I04014_20101020(
   icc_id	  VARCHAR(25)	  --
   ,
   imsi	  VARCHAR(20)	  --
   ,
   busi_code	  INTEGER	  --
   ,
   res_code	  VARCHAR(14)	  --sim卡对应的资源型号
   ,
   region_id	  VARCHAR(6)	  --
   ,
   res_status	  SMALLINT	  --0已开通；1未开通；2换卡回收3挂失8销户退网
   ,
   firmid	  VARCHAR(60)	  --
   ,
   sim_pre_date	  TIMESTAMP	  --预占资源 时，用时间做标记
   ,
   purpose	  CHAR(1)	  --0：表示指定用途1：积分兑换的资源2：赠品，礼品3：单独销售资源4：捆绑销售
   ,
   use_date	  TIMESTAMP	  --
   ,
   state_date	  TIMESTAMP	  --
   ,
   org_id	  BIGINT	  --
   ,
   sn	  VARCHAR(30)	  --sim卡序列号
   ,
   sim_type	  VARCHAR(40)	  --卡类型(0 普通sim卡、1 usim卡)需要远程写卡1	不需要远程写卡
   ,
   ki	  VARCHAR(40)	  --
   ,
   pin1	  VARCHAR(8)	  --
   ,
   pin2	  VARCHAR(8)	  --
   ,
   puk1	  VARCHAR(8)	  --
   ,
   puk2	  VARCHAR(8)	  --
   ,
   bill_id	  VARCHAR(20)	  --服务号码
   ,
   done_date	  TIMESTAMP	  --
   ,
   op_id	  BIGINT	  --
   ,
   grpno	  VARCHAR(30)	  --
)DATA CAPTURE NONE 
 IN TBS_3H INDEX 
 IN TBS_INDEX PARTITIONING KEY(
    IMSI 
 )USING HASHING
--DROP TABLE DWD_NG2_I04014_YYYYMMDD 



SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN

/**

--insert into etl_load_table_map values('I04014','DWD_NG2_I04014_YYYYMMDD','SIM卡明细表',0,'res.RES_SIM_CARD_USED_X')
--INSERT INTO USYS_TABLE_MAINTAIN 
VALUES(
   10178,
   'SIM卡明细表',
   'DWD_NG2_I04014',
   '1',
   'day',
   255,
   '0',
   '',
   '',
   'BASS2',
   'DWD_NG2_I04014_YYYYMMDD',
   'TBS_3H',
   'TBS_INDEX',
   'IMSI',
   1
   )
select * from  DWD_NG2_I04014_20101020


select count(0) from DWD_NG2_I05021_20101021




? 57016


select * from BASS2.ETL_TASK_log where task_id like 'I030%'



/**

select count(0) from ODS_PRODUCT_ORD_CUST_201010

select count(0) from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101013 

select * from syscat.tables where tabname like '%ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_201010%'

select count(0) from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20100916

 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 )
 and alarmtime >=  timestamp('20101101'||'000000') 
 order by alarmtime desc


 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 )
 order by alarmtime desc

/**
 select * from app.sch_control_alarm
 where control_code like 'TR%' order by alarmtime desc
 
 select * from app.SCH_CONTROL_TASK where control_code like '%11036%'
 
 select * from ODS_PRODUCT_ORD_CUST_201011
 
 select * from ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101101
 
 select * from syscat.tables where tabname = 'ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101101'
 
 
 select * from app.SCH_CONTROL_TASK where  control_code like '%11034%'

 select * from app.SCH_CONTROL_TASK where  control_code like '%11035%'

 ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD
 
SELECT 'DROP TABLE '|| TABNAME  FROM  USYS_TABLE_MAINTAIN WHERE TEMPLATE_TABLE = 'ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD'


SELECT  'DROP TABLE '|| TABNAME FROM SYSCAT.TABLES WHERE TABNAME LIKE '%ORD_BATCH_DATA_DETAIL_F%'


SELECT 'DROP TABLE '|| TABNAME  FROM  USYS_TABLE_MAINTAIN WHERE TEMPLATE_TABLE = '%ODS_ORD_BATCH_DATA_DETAIL_F_%'


create table ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_

SELECT COUNT(0) FROM ODS_ORD_DSMP_BAT_DATA_HIS_20101102


DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101101
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101102
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101103
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101104
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101105
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101106
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101107
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101108
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101109
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101110
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101111
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101112
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101113
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101114
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101115
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101116
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101117
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101118
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101119
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101120
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101121
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101122
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101123
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101124
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101125
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101126
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101127
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101128
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101129
DROP TABLE ODS_ORD_DSMP_BAT_DATA_HIS_20101130

select * from ODS_product_ORD_DSMP_BAT_DATA_HIS_20101101

SELECT * FROM USYS_TABLE_MAINTAIN WHERE TABLE_ID IN (10173,10172)

DELETE FROM USYS_TABLE_MAINTAIN
WHERE TABLE_ID IN (10173,10172)


insert into USYS_TABLE_MAINTAIN values(10173,'梦网批量处理历史表','ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_DSMP_BAT_DATA_HIS_YYYYMMDD','TBS_3H','TBS_INDEX','DATA_ID',1)
insert into USYS_TABLE_MAINTAIN values(10172,'普通业务批量办理表','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F','1','day',255,'0','','','BASS2','ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_YYYYMMDD','TBS_3H','TBS_INDEX','BATCH_DATA_DETAIL_ID',1)


SELECT  'DROP TABLE '|| TABNAME FROM SYSCAT.TABLES WHERE TABNAME LIKE 'ORD_BATCH_DATA_DETAIL_F%'



DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101101
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101102
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101103
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101104
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101105
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101106
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101107
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101108
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101109
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101110
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101111
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101112
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101113
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101114
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101115
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101116
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101117
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101118
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101119
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101120
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101121
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101122
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101123
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101124
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101125
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101126
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101127
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101128
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101129
DROP TABLE ORD_BATCH_DATA_DETAIL_F_20101130


SELECT COUNT(0) FROM ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_20101101

SELECT TABNAME FROM SYSCAT.TABLES WHERE TABNAME LIKE 'ODS_PRODUCT_ORD_BATCH_DATA_DETAIL_F_201011%'


select * from etl_load_table_map where task_id in ( 'A11034','A11035' )


以下两接口的自动建表配置表未建，请增加
select * from etl_load_table_map where task_id in ( 'A11034','A11035' )
select * from USYS_TABLE_MAINTAIN where table_name like 'ODS_PRODUCT_ORD_DSMP_BAT_DATA%'
select * from USYS_TABLE_MAINTAIN where table_name like 'ODS_PRODUCT_ORD_BATCH%'
 



select count(0) from ODS_CDR_GPRS_GXZ_20101110
truncate   ODS_CDR_GPRS_GXZ_20101110

select count(

/**	2010-11-9 17:52	added by  panzhiwei	**/
--DROP TABLE ODS_CDR_GPRS_GXZ_20101104		
CREATE TABLE ODS_CDR_GPRS_GXZ_20101104 (		

/**	2010-11-9 17:52	added by  panzhiwei	**/
--DROP TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD	
CREATE TABLE ODS_CDR_GPRS_GXZ_YYYYMMDD (		
         DR_TYPE            INTEGER                     --
        ,SERVICE_ID         INTEGER                     --
        ,BILL_MONTH         VARCHAR(8)                  --
        ,USER_NUMBER        VARCHAR(15)                 --
        ,VPLMN1             VARCHAR(7)                  --
        ,START_TIME         TIMESTAMP                   --
        ,DURATION           INTEGER                     --
        ,LAC_ID             VARCHAR(8)                  --
        ,CELL_ID            VARCHAR(16)                 --
        ,APN_NI             VARCHAR(64)                 --
        ,DATA_FLOW_UP1      INTEGER                     --
        ,DATA_FLOW_DOWN1    INTEGER                     --
        ,DURATION1          INTEGER                     --
        ,DATA_FLOW_UP2      INTEGER                     --
        ,DATA_FLOW_DOWN2    INTEGER                     --
        ,DURATION2          INTEGER                     --
        ,DURATION3          INTEGER                     --
        ,MNS_TYPE           SMALLINT                    --
        ,INPUT_TIME         TIMESTAMP                   --
 )		
  DATA CAPTURE NONE		
 IN TBS_CDR_VOICE		
 INDEX IN TBS_INDEX		
  PARTITIONING KEY		
   ( USER_NUMBER,START_TIME )		
 USING HASHING
select * from app.sch_control_task where control_code = 'TR1_L_91005'


select count(0) from DIM_DEVICE_PROFILE where value is not null and value_desc is not null


select * from dw_enterprise_msg_201011 where enterprise_id = '89402999773637'

select * from dw_enterprise_member_mid_201011 where enterprise_id = '89402999773637'




Select 'xxxxxx',substr(control_code,7,5)  from APP.SCH_CONTROL_RUNLOG where flag=1 

select * from APP.SCH_CONTROL_RUNLOG

select * from  BASS2.ETL_TASK_RUNNING

create table bass2.ETL_LOAD_TABLE_MAP_MTEST
like bass2.ETL_LOAD_TABLE_MAP
in TBS_ODS_OTHER 
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( task_id )
 USING HASHING
select * from syscat.tables where tabname like 'ETL_LOAD_TABLE_MAP'
in TBS_ODS_OTHER 
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( task_id )
 USING HASHING
select * from  bass2.ETL_LOAD_TABLE_MAP 
select * from  APP.SCH_CONTROL_ALARM
 
select * from  bass2.ETL_ROLLBACK_TASK_MAP

select * from  BASS2.ETL_TASK_RUNNING


select * from dim_call_roamtype

select ROAMTYPE_ID ,count(0)
from ODS_CDR_CALL_ROAMIN_20101207
group by ROAMTYPE_ID
order by 1

ROAMTYPE_ID	2
1	712415
6	1146
5	266
0	8162822
4	513407

select * from ODS_CDR_CALL_ROAMIN_20101207


select * from 
select imei from CDR_CALL_20101207
 select * from app.sch_control_task  where cmd_line like '%CDR_CALL%'

select * from dim_dr_type where drtype_name like '%话单%'
select * from etl_load_table_map where task_id = 'A05001'

select * from app.sch_control_before where before_control_code = 'TR1_L_05001'


 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%A05021%'
 or 
 content like '%ENTERPRISE_MANAGER_RELA%'
 or 
 content like '%CUST_MANAGER_INFO%'
 )
 and alarmtime >=  timestamp('20101130'||'000000') 
 order by alarmtime desc




select * from DMPM_SELLINGPLAN_201011







select op_time,count(0) from DW_TERM_USER_ANALY_MM
group by op_time
order by 1


select time_id,count(0) from BASSWEB.MART_KPI_monthLY
group by time_id
order by 1 desc



select * from REGIONCELL_USERTYPE_201010

select 1 from dual

select op_time , count(0)
from CDR_VCARD_DTL_DM_201011
group by op_time 
order by 1



cdr_vcard_dtl_dm_

select * from syscat.tables where tabname like '%DMPM_SELLINGPLAN_%'
order by 2


select count(0) from DWD_PRODUCT_FREE_20101103

select * from DWD_CHANNEL_RULE_INFO_201011




select * from DWD_NG2_A01001_20101202


select * from app.sch_control_task where cmd_line like '%DWD_NG%'

select * from USYS_TABLE_MAINTAIN 


select * from syscat.tablespaces 
select * from syscat.dbpartitiongroupdef
select * from syscat.dbpartitiongroups

select * from

\
select count(0) from  DWD_NG2_A01001_20101202
SELECT COUNT(0)
FROM DWD_NG2_A02024_20101202
SELECT COUNT(0)
FROM DWD_NG2_A02025_20101202
SELECT COUNT(0)
FROM DWD_NG2_A02026_20101202
SELECT COUNT(0)
FROM DWD_NG2_A02027_20101202
SELECT COUNT(0)
FROM DWD_NG2_I01002_20101202
SELECT COUNT(0)
FROM DWD_NG2_I01003_20101202
SELECT COUNT(0)
FROM DWD_NG2_I01035_20101202
SELECT COUNT(0)
FROM DWD_NG2_I03013_20101202
SELECT COUNT(0)
FROM DWD_NG2_M03021_20101202
SELECT COUNT(0)
FROM DWD_NG2_I03027_20101202
SELECT COUNT(0)
FROM DWD_NG2_I03028_20101202
SELECT COUNT(0)
FROM DWD_NG2_I05002_20101202
SELECT COUNT(0)
FROM DWD_NG2_I05037_20101202
SELECT COUNT(0)
FROM DWD_NG2_I05042_20101202
SELECT COUNT(0)
FROM DWD_NG2_I06020_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08059_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08103_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08104_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08105_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08106_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08108_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08109_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08110_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08111_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08112_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08115_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08117_20101202
select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%I14108%'
 )
 and alarmtime >=  timestamp('20101201'||'000000') 
 order by alarmtime desc
 
 
create nickname XZBASS2.CDR_ISMG_20101202 for db25.BASS2.CDR_ISMG_20101202
create nickname XZBASS2.VGOP_15202_20101202 for db25.BASS2.VGOP_15202_20101202
create nickname XZBASS2.dw_product_ins_off_ins_prod_201011 for db25.BASS2.dw_product_ins_off_ins_prod_201011
create nickname XZBASS2.CDR_ISMG_20101130 for db25.BASS2.CDR_ISMG_20101130


 xzbass2.dw_product_ins_off_ins_prod_201011

  BASS2.CDR_ISMG_$yyyymm31
  
  
BASS2.DW_PRODUCT_
 
select * from DWD_NG2_M06025_201011

select * from DWD_NG2_M06026_201011






 

--select count(0) from  DWD_NG2_I05002_20101202
--select count(0) from  DWD_NG2_I05037_20101202
--select count(0) from  DWD_NG2_I05042_20101202
--select count(0) from  DWD_NG2_I06020_20101202
--select count(0) from  DWD_NG2_I08059_20101202
select count(0) from  DWD_NG2_I08103_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08104_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08105_20101202
SELECT COUNT(0)
FROM DWD_NG2_I08106_20101202
alter  DWD_NG2_I05002_YYYYMMDD modify  SERVICE_TYPE varcar(8)
drop table DWD_NG2_I05002_20101202
 CREATE TABLE DWD_NG2_I05002_20101202
 (
DRTYPE_ID                 INTEGER       ,--记录类型               
PDP_ID                    VARCHAR(4)    ,--PDP上下文的标志        PDP_TYPE
PRODUCT_NO                VARCHAR(15)   ,--手机号码               
IMSI                      VARCHAR(15)   ,--IMSI号码               
SGSN_ADDRESS              VARCHAR(128)  ,--SGSN地址               
ADDUP_RES                 INTEGER       ,--网络能力               
CELL_ID                   VARCHAR(20)   ,--小区标识               
RAC                       VARCHAR(5)    ,--路由区域               
LAC_ID                    VARCHAR(8)    ,--基站标识               
CHARGING_ID               VARCHAR(10)   ,--计费标识               
GGSN_ID                   VARCHAR(20)   ,--GGSN地址               
APN_NI                    VARCHAR(64)   ,--网络标识               
APN_OI                    VARCHAR(64)   ,--运营商标识             
PDP_TYPE                  VARCHAR(4)    ,--PDP类型                
PDP_ADDRESS               VARCHAR(32)   ,--PDP地址                
SGSN_CHANGE               SMALLINT      ,--SGSN改变标志           
CAUSE_CLOSE               SMALLINT      ,--终止原因               
COMBINE_RESULT            CHARACTER(1)  ,--合并结果标志           
CITY_ID                   VARCHAR(7)    ,--归属地                 
ROAM_CITY_ID              VARCHAR(7)    ,--漫游地                 
USER_PROPERTY             INTEGER       ,--计费用户类型           
BILL_FLAG                 SMALLINT      ,--费用类型               
ROAMTYPE_ID               SMALLINT      ,--漫游类型               
SERVICE_TYPE              VARCHAR(8)    ,--计费业务类型           
START_TIME                TIMESTAMP     ,--通话时间               
DURATION                  INTEGER       ,--通话时长               
TARIFF1                   CHARACTER(1)  ,--第1种费率级别          
DATA_FLOW_UP1             INTEGER       ,--级别1的上行的数据流量，
DATA_FLOW_DOWN1           INTEGER       ,--级别1的下行的数据流量，
DURATION1                 INTEGER       ,--级别1的通话时长        
TARIFF2                   CHARACTER(1)  ,--第2种费率级别          
DATA_FLOW_UP2             INTEGER       ,--级别2的上行的数据流量，
DATA_FLOW_DOWN2           INTEGER       ,--级别2的下行的数据流量，
DURATION2                 INTEGER       ,--级别2的通话时长        
tariff3                   character(1)  ,--第3种费率级别          
data_flow_up3             integer       ,--级别3的上行的数据流量，
data_flow_down3           integer       ,--级别3的下行的数据流量，
duration3                 integer       ,--级别3的通话时长        
tariff4                   character(1)  ,--第4种费率级别          
data_flow_up4             integer       ,--级别4的上行的数据流量，
data_flow_down4           integer       ,--级别4的下行的数据流量，
duration4                 integer       ,--级别4的通话时长        
tariff5                   character(1)  ,--第5种费率级别          
data_flow_up5             integer       ,--级别5的上行的数据流量，
data_flow_down5           integer       ,--级别5的下行的数据流量，
duration5                 integer       ,--级别5的通话时长        
tariff6                   character(1)  ,--第6种费率级别          
data_flow_up6             integer       ,--级别6的上行的数据流量，
data_flow_down6           integer       ,--级别6的下行的数据流量，
duration6                 integer       ,--级别6的通话时长        
BASE_FEE                  INTEGER       ,--基本通信费             CHARGE1
CHARGE1                   INTEGER       ,--费用1                  
CHARGE2                   INTEGER       ,--费用2                  
CHARGE3                   INTEGER       ,--费用3                  
total_charge              integer       ,--总费用                 
CHARGE1_DISC              INTEGER       ,--基本通信费优惠n        
CHARGE4_DISC              INTEGER       ,--其他费用优惠n          
data_flow_disc            integer       ,--优惠流量n              
disc_type                 varchar(5)    ,--优惠类型n              
MNS_TYPE                  INTEGER        --承载网络类型           
 )
  DATA CAPTURE NONE
 IN TBS_CDR_VOICE
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (PRODUCT_NO
   ) USING HASHING
select * from VGOP_11202_201010


select * from syscat.tables where lower(tabname) like 'dw_product_ins_off_ins_prod_%'

 select * from app.sch_control_alarm 
 where control_code = 'TR1_L_40006'        


select * from dw_enterprise_member_mid_201004



select * from dim_comp_brand  


8940



select *
from bass2.cdr_call_dtl_20101208
where lac_id = '8940'
and cell_id = '27F7'



SELECT * FROM DWD_99912_YYYYMM



insert into etl_load_table_map values('M99912','DWD_NG2_M99912_YYYYMM','终端配置信息',0,'BASS2.DIM_DEVICE_PROFILE')
INSERT INTO etl_load_table_map 
VALUES(
   'M99906',
   'DWD_NG2_M99906_YYYYMM',
   '学校渠道信息表',
   0,
   'SO.SCHOOL_MARKET_DATE'
   )
INSERT INTO etl_load_table_map 
VALUES(
   'M99905',
   'DWD_NG2_M99905_YYYYMM',
   '学校基站信息',
   0,
   'SO.SCHOOL_MARKET_DATE'
   )
SELECT * FROM etl_load_table_map WHERE TASK_ID LIKE '%M99%'


select * from DWD_NG2_P99905_20101202



select year(valid_date) from dw_product_20101202


select * from syscat.tables where tabname like '%DWD_NG2_P999%'

select * from DWD_NG2_P99905_20101202

 select * from app.sch_control_alarm
 where control_code like 'TR%' 
 and (
 content like '%A11034%'
 or 
 content like '%A11035%'
 or 
 content like '%M11036%'
 or 
 content like '%A05021%'
 or 
 content like '%I14108%'
 )
 and alarmtime >=  timestamp('20101201'||'000000') 
 order by alarmtime desc
 

8940	27ED
8940	27F7
8940	2739

select * from XZBASS2.dw_call_cell_201011 where cell_id = '27ED'

select user_id  
from XZBASS2.dw_call_cell_201011 a 
where a.lac_id = '8940' and a.cell_id in ('27ED','27F7','2739')
--on a.lac_id = b.laccode and a.cell_id = b.CELLCODE
group by user_id having count(0) >= 20


select user_id from XZBASS2.dw_call_cell_201011
 

  create nickname XZBASS2.dw_call_cell_201011
 for db25.BASS2.dw_call_cell_201011


select * from DWD_NG2_P99905_20101202


select user_id from  bass2.cdr_call_dtl_20101201

select count(0),count(distinct user_id ) from dw_product_20101203


select count(0),count(distinct product_no)
select product_no,opp_number
from bass2.cdr_call_dtl_20101201
where lac_id = '8940'
and cell_id = '27F7'


select * from 
dw_call_cell_201011

select product_no,count(0) cnt 
from dw_call_cell_201011 a 
where lac_id = '8940'
and cell_id = '27F7'
group by product_no
order by 2 desc 



select * from 

select * from dw_enterprise_member_mid_201011 where enterprise_id = '89401560001792'

select * from DWD_NG2_P99905_20101202


select * from DWD_NG2_I08059_20101202


select enterprise_id , user_id from DW_ENTERPRISE_MEMBER_MID_201005 a
join (select distinct GROUPCODE from DWD_NG2_P99906_20101202 ) b on a.enterprise_id = b.groupcode 
union all 
select user_id  
from XZBASS2.dw_call_cell_201011 a 
join (select distinct LACCODE , CELLCODE from DWD_NG2_P99905_20101202 ) b on a.lac_id = b.laccode 
and a.cell_id = b.CELLCODE
group by user_id having count(0) > 10


select * from DWD_NG2_P99905_20101202

SCHMARKER	VARCHAR(32)	
GROUPCODE	VARCHAR(32)	
SCHNAME	VARCHAR(200)	
PRODUCT_NO	VARCHAR(15)	
CUST_NAME	VARCHAR(200)	
SEX_ID	SMALLINT	
OPERATOR	SMALLINT	
BRAND_ID	BIGINT	
WORK_DEPT	VARCHAR(120)	
GRADE	SMALLINT	
HOME_ADDRESS	VARCHAR(200)	
IDEN_NBR	VARCHAR(40)	
EDUCATION_ID	SMALLINT	
VALID_DATE	INTEGER	
POSITION	SMALLINT	
EMAIL	VARCHAR(64)	

select b.SCHMARKER,c.GROUPCODE,b.SCHNAME,d.product_no,e.CUST_NAME,e.SEX_ID,1 OPERATOR,d.BRAND_ID,1 WORK_DEPT
,1 grade ,e.HOME_ADDRESS,e.IDEN_NBR,e.EDUCATION_ID,year(d.valid_date) valid_date ,1 POSITION,E.EMAIL
from (select user_id  , a.cell_id 
from XZBASS2.dw_call_cell_201011 a 
where a.lac_id = '8940' 
--and a.cell_id in ('27ED','27F7')
and a.cell_id in ('2739')
group by user_id ,a.cell_id  having count(0) >= 10) a 
join DWD_NG2_P99905_20101130 b on a.cell_id = b.cellcode 
join DWD_NG2_P99906_20101130 c on b.SCHMARKER = c.SCHMARKER 
join XZBASS2.dw_product_201011 d on a.user_id = d.user_id 
join XZBASS2.dw_cust_20101130 e on d.cust_id = e.CUST_ID


select * from dw_cust_20101202

join 

         REGION
        ,COUNTY_CODE
        ,SCHMARKER
        ,ORG_ID
        ,ORG_TYPE
        ,GROUPCODE

select count(0) from (select distinct * from DWD_NG2_P99905_20101202) a

select * from DWD_NG2_P99905_20101202



                        select a.user_id 
                        from session.t_dwd_ng2_M99904_2 a 
                        join XZBASS2.dw_product_201011 b on a.user_id = b.user_id 
                        
                        
select * from DIM_ACCT_CREDIT
select * from DIM_ACCT_STATUS

select owner,tabname from syscat.tables where tabname like 'DIM%' and owner = 'BASS2'
order by 1

select * from DIM_ACCEPT_TYPE

select * from DIM_ACCT_ITEM_FEETYPE where item_name like '%视频%'
select * from dim_dr_type where drtype_name like '%留言%'


		SELECT 
		FLOW_NO
		,UCID
		,CALLING
		,CALLED
		,BEGIN_TIME - 2 seconds 
		,END_TIME + 5 seconds 
		,BEGIN_TIME
		,END_TIME +5 seconds
		,BEGIN_TIME + 2 seconds
		,END_TIME + 5 seconds + 2 seconds + int(rand(1)*100) seconds
		,OPERATION_TYPE
		,PART_ID
		,OPERATION_CODE
		,FLAG
		from ODS_CUSTSVC_IVR_LOG_20101231    where   BEGIN_TIME = end_time   
    
select op_time,count(0) from DW_HAVE_VALUE_MM_2010 group by op_time


select * from  app.sch_control_alarm 
where alarmtime >  timestamp('20110105'||'160000') 

select count(0),count(distinct flow_no) 
select * from ODS_CUSTSVC_IVR_LOG_20101231

select * from ODS_NUTS_IVR_201012

select * from etl_load_table_map

select * from app.sch_control_task where control_code like '%BASS2_Dw_vgop_15202_dm.tcl%'
                       
                       
select tabname , card from syscat.tables where tabname like '%CDR_CALL_DTL%' and card > 0

select count(0) from CDR_CALL_20110101
8903045
select count(0) from CDR_CALL_DTL_20110101
8902522
select a.*,b.*
from (
select drtype_id,count(0)
from CDR_CALL_20110101
group by drtype_id
order by 1
) a join dim_dr_type b on a.drtype_id = b.drtype_id

select * from DW_ACCT_SHOULD_TODAY_20100228

select * from DW_ACCT_BOOK_20101015

select * from DW_ACCT_MSG_20100228

select * from DW_ACCT_OWE_201010

select * 
select fee_item_id,count(0) 
from DW_ACCT_OWE_201010
group by fee_item_id

select 

select * from DIM_ACCT_ITEM

select * from DMPM_ACCT_CHARGE_201012

select * from DW_ACCT_BAD_201001

select * from DW_ACCT_BUSICHARGE_DM_201101

select * from DW_ACCT_SHOULD_DTL_TODAY_20091227


select tabname , card from syscat.tables where tabname like '%ACCT%' 

select tabname , card from syscat.tables where tabname like '%ACCT%' 

select * from DW_ACCT_PAYMENT_DM_201011


select * from cdr_call_20110101


select op_time,count(0) from BASS2.DW_VGOP_15210_DM_201101
group by op_time 


select * from vgop_15210_20110105

select * from BASS2.VGOP_15202_20110105
select * from BASS2.DW_VGOP_15202_DM_201101
select * from vgop_15209_20110105

select * from BASS2.DW_VGOP_15202_DM_201101

INSERT INTO etl_load_table_map_mtest(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I05045','DWD_NG2_I05045_YYYYMMDD','视频IVR业务清单', 0,null)
 
SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN


INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) values('I05045','DWD_NG2_I05045_YYYYMMDD','视频IVR业务清单',0,null)

INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I05045','DWD_NG2_I05045_YYYYMMDD','视频IVR业务清单', 0,null)
 
delete from etl_load_table_map where task_id = 'I05045'

INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) values(I05045,DWD_NG2_I05045_YYYYMMDD,视频IVR业务清单,0,null)

create table tmp_pzw_testnull
(
testnull varchar(200)
)
  DATA CAPTURE NONE
in tbs_3h
index in tbs_index
partitioning key (testnull) using hashing

insert into tmp_pzw_testnull values (null)


  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING

select * from 
select * from ODS_RES_CTMS_EXCHG_20101228



select * from DWD_DSMP_WWWPORTAL_OPER_LOG_DM_201011
select * from DWD_DSMP_WWWPORTAL_OPER_LOG_DM_201012

select * from syscat.tables where tabname like '%DSMP_WWWPORTAL_OPER_LOG%'

select * from dw_have_value_mm_2011

select * from etl_load_table_map
where TABLE_NAME_TEMPLET LIKE '%MM'
ORDER BY 1

select * from DWD_DSMP_WAP_MENU_DS_201101

select * from app.sch_control_task where control_code like '%99%'


SELECT * FROM DWD_40001_DM_201011


SELECT TIME_ID,COUNT(0)
FROM DWD_40001_DM_201011
GROUP BY TIME_ID


select * from BASS2.DW_HAVE_VALUE_MM_2011

select * from 

select * from  app.sch_control_alarm 
where alarmtime >  timestamp('20101231'||'000000') 
and dealtime is  null
and control_code like '%stat_cust_value_0007.tcl%'
order by alarmtime desc 


DW_HAVE_VALUE_MM_2011

select * from app.sch_control_task 
where upper(cmd_line) like '%DW_HAVE_VALUE%'



select * from  app.sch_control_alarm 
where alarmtime >  timestamp('20101231'||'000000') 
and dealtime is  null
order by alarmtime desc 


select * from syscat.tables where tabname like '%DW_PRODUCT_FUNC_%'

DW_PRODUCT_FUNC_201012

select * from syscat.indexes
where indname like '%I_P_FUNC_%_2%'

select * from stat_enterprise_0022_dm_201101
order by 1


select * from BASS2.DW_ENTERPRISE_INDUSTRY_APPLY_DM_201101

select * from BASS2.STAT_ENTERPRISE_0022_DM_201101

select * from app.sch_control_before where control_code like '%REP_stat_enterprise_0022.tcl%'

DW_HAVE_VALUE_MM_2011

select * from app.sch_control_before where control_code like '%DW_HAVE_VALUE_MM%'

select * from app.sch_control_task where lower(control_code) like '%dw_have_value_ms%'

select * from DW_HAVE_VALUE_MM_2010

select op_time , count(0) from DW_HAVE_VALUE_MM_2010
group by op_time
order by 1


select * from syscat.tables where tabname like '%DW_HAVE_VALUE%'

select * from app.sch_control_before
where control_code like '%BASS2_Dw_have_value_ms.tcl%'


select * from  app.sch_control_alarm 
where alarmtime >  timestamp('20101231'||'000000') 
and dealtime is  null
and control_code like '%_have_value_ms.tcl%'
order by alarmtime desc 


select * from app.sch_control_runlog where control_code like '%_have_value_ms.tcl%'

select * from app.sch_control_task where control_code like '%BASS2_Dw_have_value_ms.tcl%'
                       
select * from  app.sch_control_alarm 
where alarmtime >  timestamp('20101231'||'000000') 
order by alarmtime desc 


                       
select * from syscat.table where tabname like '%VGOP%16202%'

select * from ETL_LOAD_TABLE_MAP_MTEST where task_id like '%03021%'


SELECT * FROM DWD_NG2_I05042_20101202

I05042	DWD_NG2_I05042_YYYYMMDD	WAP网关日志	0	XZJF.dr_kj_$YYYYMMDD$

SELECT * FROM ETL_LOAD_TABLE_MAP_MTEST where task_id like '%05042%'


delete from ETL_LOAD_TABLE_MAP_MTEST where task_id like '%05042%'

insert into ETL_LOAD_TABLE_MAP_MTEST values('A05042','DWD_NG2_A05042_YYYYMMDD','WAP网关日志',0,'XZJF.dr_kj_$YYYYMMDD$')

DWD_NG2_I05042_YYYYMMDD


SELECT * FROM DWD_NG2_I05042_20101202


select DISTINCT row_id from bass2.cdr_call_20101220


select * from syscat.tables where tabname like '%NG2%05042%'

select * from DWD_NG2_M06025_201010


select row_id from DWD_NG2_M06026_201011

select * from DWD_NG2_I08059_20101202


select * from DWD_NG2_I08117_20101202
DWD_NG2_I08110_YYYYMMDD
DWD_NG2_I08105_YYYYMMDD

select * from DWD_NG2_I08103_20101202
DWD_NG2_I08111_YYYYMMDD
DWD_NG2_I08106_YYYYMMDD
DWD_NG2_I08104_YYYYMMDD

select * from DWD_NG2_I08104_YYYYMMDD

select * from DWD_NG2_I05002_20101202 where data_flow_disc > 0
DWD_NG2_I08108_YYYYMMDD
DWD_NG2_I08109_YYYYMMDD

select count(0) from DWD_NG2_I05002_20101202 


select * from syscat.tables where tabname like '%DWD_NG2_I08116%'
select * from DWD_NG2_I08116_20101023

select id,name,note,state,valid_date,expire_date from  NUTS.ng2_I08116

drop table DWD_NG2_I05002_20101202
 CREATE TABLE DWD_NG2_I05002_20101202
 (
DRTYPE_ID                 INTEGER       ,--记录类型               
PDP_ID                    VARCHAR(4)    ,--PDP上下文的标志        PDP_TYPE
PRODUCT_NO                VARCHAR(15)   ,--手机号码               
IMSI                      VARCHAR(15)   ,--IMSI号码               
SGSN_ADDRESS              VARCHAR(128)  ,--SGSN地址               
ADDUP_RES                 INTEGER       ,--网络能力               
CELL_ID                   VARCHAR(20)   ,--小区标识               
RAC                       VARCHAR(5)    ,--路由区域               
LAC_ID                    VARCHAR(8)    ,--基站标识               
CHARGING_ID               VARCHAR(10)   ,--计费标识               
GGSN_ID                   VARCHAR(20)   ,--GGSN地址               
APN_NI                    VARCHAR(64)   ,--网络标识               
APN_OI                    VARCHAR(64)   ,--运营商标识             
PDP_TYPE                  VARCHAR(4)    ,--PDP类型                
PDP_ADDRESS               VARCHAR(32)   ,--PDP地址                
SGSN_CHANGE               SMALLINT      ,--SGSN改变标志           
CAUSE_CLOSE               SMALLINT      ,--终止原因               
COMBINE_RESULT            CHARACTER(1)  ,--合并结果标志           
CITY_ID                   VARCHAR(7)    ,--归属地                 
ROAM_CITY_ID              VARCHAR(7)    ,--漫游地                 
USER_PROPERTY             INTEGER       ,--计费用户类型           
BILL_FLAG                 SMALLINT      ,--费用类型               
ROAMTYPE_ID               SMALLINT      ,--漫游类型               
SERVICE_TYPE              VARCHAR(8)    ,--计费业务类型           
START_TIME                TIMESTAMP     ,--通话时间               
DURATION                  INTEGER       ,--通话时长               
TARIFF1                   CHARACTER(1)  ,--第1种费率级别          
DATA_FLOW_UP1             INTEGER       ,--级别1的上行的数据流量，
DATA_FLOW_DOWN1           INTEGER       ,--级别1的下行的数据流量，
DURATION1                 INTEGER       ,--级别1的通话时长        
--TARIFF2                   CHARACTER(1)  ,--第2种费率级别          
--DATA_FLOW_UP2             INTEGER       ,--级别2的上行的数据流量，
--DATA_FLOW_DOWN2           INTEGER       ,--级别2的下行的数据流量，
--DURATION2                 INTEGER       ,--级别2的通话时长        
--tariff3                   character(1)  ,--第3种费率级别          
--data_flow_up3             integer       ,--级别3的上行的数据流量，
--data_flow_down3           integer       ,--级别3的下行的数据流量，
--duration3                 integer       ,--级别3的通话时长        
--tariff4                   character(1)  ,--第4种费率级别          
--data_flow_up4             integer       ,--级别4的上行的数据流量，
--data_flow_down4           integer       ,--级别4的下行的数据流量，
--duration4                 integer       ,--级别4的通话时长        
--tariff5                   character(1)  ,--第5种费率级别          
--data_flow_up5             integer       ,--级别5的上行的数据流量，
--data_flow_down5           integer       ,--级别5的下行的数据流量，
--duration5                 integer       ,--级别5的通话时长        
--tariff6                   character(1)  ,--第6种费率级别          
--data_flow_up6             integer       ,--级别6的上行的数据流量，
--data_flow_down6           integer       ,--级别6的下行的数据流量，
--duration6                 integer       ,--级别6的通话时长        
BASE_FEE                  INTEGER       ,--基本通信费             CHARGE1
CHARGE1                   INTEGER       ,--费用1                  
CHARGE2                   INTEGER       ,--费用2                  
CHARGE3                   INTEGER       ,--费用3                  
total_charge              integer       ,--总费用                 
CHARGE1_DISC              INTEGER       ,--基本通信费优惠n        
CHARGE4_DISC              INTEGER       ,--其他费用优惠n          
data_flow_disc            integer       ,--优惠流量n              
--disc_type                 varchar(5)    ,--优惠类型n              
MNS_TYPE                  INTEGER        --承载网络类型           
 )
  DATA CAPTURE NONE
 IN TBS_CDR_VOICE
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (PRODUCT_NO
   ) USING HASHING


select 
--max(case when partkeyseq = 1 then trim(colname) end )
--,max(case when partkeyseq = 2 then ','||trim(colname) end)
colname
from syscat.columns 
where tabname = 'DWD_NG2_A02025_YYYYMMDD' and partkeyseq >= 1 
--order by partkeyseq

select * from syscat.tables where tabname like '%DWD_NG2_I08110_YYYYMMDD%'

select * from syscat.columns where tabname like '%DWD_NG2_I08110_YYYYMMDD%'

select colname,'xxxxx'  from syscat.columns where tabname like '%DWD_NG2_A02025_YYYYMMDD%' and partkeyseq >= 1 order by partkeyseq



/**	2010/11/2 18:02	added by  panzhiwei		**/	
CREATE TABLE DWD_NG2_I03013_20101225 (					
        CHANNEL_ID          BIGINT              --渠道标识:
        ,CHANNEL_NAME        VARCHAR(100)        --渠道名称:
        ,PARENT_CHANNEL_ID   INTEGER             --上级渠道标识:
        ,CHANNEL_LEVEL       INTEGER             --渠道级别:与“渠道级别”接口单元对应
        ,CHANNEL_DESC          VARCHAR(200)        --渠道描述:
        ,START_DATE          TIMESTAMP           --启用日期:
        ,EXPIRE_DATE         TIMESTAMP           --截止日期:缺省29991231
        ,REGION_CODE         VARCHAR(8)          --归属区域编号:与“地域08039”接口单元对应
        ,ORGANIZE_ID         BIGINT              --所属单位标识:与“单位”接口单元对应
        ,CHANNEL_TYPE        INTEGER             --渠道类别:与“渠道类型”接口单元对应
        ,PROPERTY_SRC_TYPE   INTEGER             --渠道物业形态:包括上市公司够建、存续企业购建、租赁、转租-店中店
        ,REG_FUND            BIGINT              --投资规模:
        ,OPEN_DATE           TIMESTAMP           --开业时间:
        ,RESPNSR_ID             INTEGER             --渠道负责人编号:与“渠道负责人”接口单元对应
        ,TEL_NUMBER          VARCHAR(20)         --渠道办公电话:
        ,FAX_NUMBER          VARCHAR(20)         --传真号码:
        ,POST_CODE           VARCHAR(20)            --邮编:
        ,BUSI_BEGIN_TIME     VARCHAR(20)         --营业时间:
        ,CHANNEL_STYLE       INTEGER             --渠道运营方式:"0-自建自营（自办）；1-自建他营（合作）；2-他建他营（特许）"
        ,STAR_LEVEL       INTEGER             --渠道星级:"0-一星级；1-二星级"
        ,CHANNEL_USER_ID     INTEGER             --渠道租赁人编号:与“渠道租赁人”接口单元对应
        ,OWNER_TYPE          SMALLINT            --物业购置方式:
        ,PWNER_PRICE         INTEGER             --物业购置价格:
        ,FITMENT_PRICE       INTEGER             --装修投入:
        ,TRANSFER_PRICE      INTEGER             --传输投入:
        ,INVESTOR            VARCHAR(20)         --投资主体:
        ,LICENSE_NUMBER      VARCHAR(20)          --工商号:
        ,INTERNET_MODE       INTEGER             --接入方式:"0-光缆；1-2M电缆；2-GPRS；3-CDS；4-拨号上网；5-无线网桥；6-无"
        ,CREATE_DATE         TIMESTAMP           --建档时间:
        ,CR_OP_ID            BIGINT              --建档员工:
        ,OP_ID               BIGINT              --操作员工:
        ,DONE_DATE           TIMESTAMP           --操作时间:
        ,CHANNEL_STATE       SMALLINT            --渠道状态:"0-正常；1-预解约；2-注销"
        ,NOTES               VARCHAR(200)            --备注
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( CHANNEL_ID )
 USING HASHING


select * from DWD_NG2_M99903_201011



CREATE TABLE "BASS2   "."DWD_NG2_M99903_201011"  (
                  "SCHMARKER" VARCHAR(32) , 
                  "SCHNAME" VARCHAR(200) , 
                  "REGION" VARCHAR(8) , 
                  "STUDENT_NUM" INTEGER , 
                  "SCHADDRESS" VARCHAR(200) , 
                  "PARENT_SCHOOL_IND" INTEGER , 
                  "MARKETING_AREA" INTEGER , 
                  "GROUPCODE" VARCHAR(32) , 
                  "ADMIN_DEPT" VARCHAR(120) , 
                  "EMPLOYEE_NUM" INTEGER , 
                  "RUN_TYPE" VARCHAR(8) , 
                  "OP_TIME" BIGINT )   
                 DISTRIBUTE BY HASH("SCHMARKER")   
                   IN "TBS_3H" INDEX IN "TBS_INDEX"  




select * from bass2.ETL_LOAD_TABLE_MAP_MTEST 

CREATE TABLE "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST"  (
                  "TASK_ID" VARCHAR(10) NOT NULL , 
                  "TABLE_NAME_TEMPLET" VARCHAR(100) NOT NULL , 
                  "TASK_NAME" VARCHAR(100) , 
                  "LOAD_METHOD" SMALLINT NOT NULL WITH DEFAULT 0 , 
                  "BOSS_TABLE_NAME" VARCHAR(100) )   
                 DISTRIBUTE BY HASH("TASK_ID")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY 
                   

COMMENT ON TABLE "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST" IS 'ETL表映象'

COMMENT ON COLUMN "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST"."BOSS_TABLE_NAME" IS '对应BOSS的表'

COMMENT ON COLUMN "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST"."LOAD_METHOD" IS '入库方式，0LOAD，1IMPORT'

ALTER TABLE "BASS2   "."ETL_LOAD_TABLE_MAP_MTEST" 
        ADD CONSTRAINT "PK_ETL_LOAD_T_MAP" PRIMARY KEY
                ("TASK_ID")


                   

select * from stat_enterprise_snapshot

select * from BASS2.STAT_ENTERPRISE_SNAPSHOT_201011



select       
c.MO_GROUP_DESC,--模块名
count( case when  substr(char(date(current timestamp),ISO),1,8)<> substr(char(date(b.begintime),ISO),1,8)   and  b.flag=0 or b.flag=-2 then a.control_code end ) ,--未完成数
count( case when   b.flag=1  then a.control_code end ) ,--执行数
count( case when   b.flag=-1 then a.control_code end ) , --执行出错
count( case when   substr(char(date(current timestamp),ISO),1,8)= substr(char(date(b.begintime),ISO),1,8)    and  b.flag=0 then a.control_code end ) 	--完成数
from APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (2)
group by c.MO_GROUP_DESC,c.sort_id
order by c.sort_id
with ur



select * from bass2.ETL_LOAD_TABLE_MAP


select * from BASS2.ETL_TASK_RUNNING order by 2



CREATE NICKNAME XZBASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT
FOR DB25.BASS2.TMP_PZW_STAT_ENTERPRISE_SNAPSHOT

TABNAME
TMP_PZW_001CNT
TMP_PZW_1
TMP_PZW_2
TMP_PZW_21
TMP_PZW_STAT_ENTERPRISE_SNAPSHOT


select 
--'DROP TABLE '||TABNAME||'' 
TABNAME 
from syscat.tables where tabname like '%PZW%'
select * from VGOP_15202_20101219

select app.sch_control_task

select * from app.sch_control_before where control_code like '%15209%'

select * from app.sch_control_runlog where control_code like '%TR1_VGOP_D_15210%'

TR1_VGOP_D_15209

select tabname,CREATE_TIME from syscat.tables  where  lower(tabname) like '%15209%'
order by 2


select op_time,count(0) cnt  
from DW_VGOP_15202_DM_201012
group by op_time
order by 1


select * from app.sch_control_log where control_code like '%BASS2_Dw_vgop_15209_dm.tcl%'


 select * from app.sch_control_alarm
where alarmtime >=  timestamp('20101227'||'000000') 
order by alarmtime desc 
 
 
 

drop table tmp_pzw_001testdatatype
rename tmp_pzw_001testdatatype to tmp_pzw_001testdata
create table tmp_pzw_001testdatatype
(
        TABSCHEMA              VARCHAR(128)        
        ,TABNAME                VARCHAR(128)        
        ,OWNER                  VARCHAR(128)        
        ,TYPE                   CHARACTER(1)        
        ,STATUS                 CHARACTER(1)        
        ,BASE_TABSCHEMA         VARCHAR(128)        
        ,BASE_TABNAME           VARCHAR(128)        
        ,ROWTYPESCHEMA          VARCHAR(128)        
        ,ROWTYPENAME            VARCHAR(128)        
        ,CREATE_TIME            TIMESTAMP           
        ,INVALIDATE_TIME        TIMESTAMP           
        ,STATS_TIME             TIMESTAMP           
        ,COLCOUNT               SMALLINT            
        ,TABLEID                SMALLINT            
        ,TBSPACEID              SMALLINT            
        ,CARD                   BIGINT              
        ,NPAGES                 BIGINT              
        ,FPAGES                 BIGINT              
        ,OVERFLOW               BIGINT              
        ,TBSPACE                VARCHAR(128)        
        ,INDEX_TBSPACE          VARCHAR(128)        
        ,LONG_TBSPACE           VARCHAR(128)        
        ,PARENTS                SMALLINT            
        ,CHILDREN               SMALLINT            
        ,SELFREFS               SMALLINT            
        ,KEYCOLUMNS             SMALLINT            
        ,KEYINDEXID             SMALLINT            
        ,KEYUNIQUE              SMALLINT            
        ,CHECKCOUNT             SMALLINT            
        ,DATACAPTURE            CHARACTER(1)        
        ,CONST_CHECKED          CHARACTER(32)       
        ,PMAP_ID                SMALLINT            
        ,PARTITION_MODE         CHARACTER(1)        
        ,LOG_ATTRIBUTE          CHARACTER(1)        
        ,PCTFREE                SMALLINT            
        ,APPEND_MODE            CHARACTER(1)        
        ,REFRESH                CHARACTER(1)        
        ,REFRESH_TIME           TIMESTAMP           
        ,LOCKSIZE               CHARACTER(1)        
        ,VOLATILE               CHARACTER(1)        
        ,ROW_FORMAT             CHARACTER(1)        
        ,PROPERTY               VARCHAR(32)         
        ,STATISTICS_PROFILE     CLOB                
        ,COMPRESSION            CHARACTER(1)        
        ,ACCESS_MODE            CHARACTER(1)        
        ,CLUSTERED              CHARACTER(1)        
        ,ACTIVE_BLOCKS          BIGINT              
        ,DROPRULE               CHARACTER(1)        
        ,MAXFREESPACESEARCH     SMALLINT            
        ,AVGCOMPRESSEDROWSIZE   SMALLINT            
        ,AVGROWCOMPRESSIONRATIO REAL                
        ,AVGROWSIZE             SMALLINT            
        ,PCTROWSCOMPRESSED      REAL                
        ,LOGINDEXBUILD          VARCHAR(3)          
        ,CODEPAGE               SMALLINT            
        ,ENCODING_SCHEME        CHARACTER(1)        
        ,PCTPAGESSAVED          SMALLINT            
        ,LAST_REGEN_TIME        TIMESTAMP           
        ,SECPOLICYID            INTEGER             
        ,PROTECTIONGRANULARITY  CHARACTER(1)        
        ,DEFINER                VARCHAR(128)        
        ,REMARKS                VARCHAR(254)   
) 
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( TABNAME )
 USING HASHING
 
 

select count(0) from ODS_CUSTSVC_IVR_LOG_20101231   

select * from DWD_NG2_I05045_20101231

--DROP TABLE DWD_NG2_I05045_YYYYMMDD
 CREATE TABLE DWD_NG2_I05045_20101231 (
        FLOW_NO                 VARCHAR(20)        --呼叫唯一标志                                             
        ,UCID                     VARCHAR(30)       --话单轨迹          
        ,CALLING                  VARCHAR(24)       --主叫号码          
        ,CALLED                   VARCHAR(24)       --被叫号码          
        ,WAIT_BEGIN_TIME          TIMESTAMP         --等待开始时间      
        ,WAIT_END_TIME            TIMESTAMP         --等待结束时间      
        ,ANS_BEGIN_TIME           TIMESTAMP         --应答开始时间      
        ,ANS_END_TIME             TIMESTAMP         --应答结束时间      
        ,CMM_BEGIN_TIME           TIMESTAMP         --通话开始时间      
        ,CMM_END_TIME             TIMESTAMP         --通话结束时间      
        ,OPERATION_TYPE           VARCHAR(8)        --业务类型号        
        ,DEV_TYPE_ID                  SMALLINT          --设备类型          
        ,DEV_CODE           VARCHAR(8)        --设备号            
        ,CALL_TYPE                   SMALLINT            --呼叫类型    
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( FLOW_NO )
 USING HASHING
 
--DROP TABLE DWD_NG2_I05045_YYYYMMDD
 CREATE TABLE DWD_NG2_I05045_YYYYMMDD (
        FLOW_NO                 VARCHAR(20)         --呼叫唯一标志                                             
        ,PART_ID                 SMALLINT         
        ,UCID                    VARCHAR(30)         
        ,CALLING                 VARCHAR(24)         
        ,CALLED                  VARCHAR(24)         
        ,OPERATION_CODE          VARCHAR(8)          
        ,OPERATION_TYPE          SMALLINT         
        ,BEGIN_TIME              TIMESTAMP
        ,END_TIME                TIMESTAMP       
        ,RESULT_CODE             VARCHAR(4) 
 )
 DATA CAPTURE NONE 
 IN TBS_3H
 INDEX IN TBS_INDEX
 PARTITIONING KEY
 ( FLOW_NO )
 USING HASHING
 
select * from DWD_NG2_I08105_20101202



drop table DWD_NG2_I05042_20101202 
CREATE TABLE DWD_NG2_A05042_20101202 
(
DR_TIME	VARCHAR(14),	 --	话单记录时间戳
DR_TYPE	INTEGER,	 --	话单类型
WAP_GATEWAY_ID	VARCHAR(6),	 --	WAP网关标识
AGRMNT_TYPE	INTEGER,	 --	承载类型
USER_NUMBER	VARCHAR(15),	 --	用户手机号
URL_REQUEST_TIME	VARCHAR(14),	 --	URL请求时间
END_TIME	VARCHAR(14),	 --	访问终止时间
CONTENT_LENGTH	INTEGER,	 --	信息内容长度
PROVINCE_ID	VARCHAR(7),	 --	用户号码归属省代码
CONNECT_PROVINCE_ID	VARCHAR(7),	 --	WAP业务接入地省代码
WAP_CHARGE_TYPE	INTEGER,	 --	WAP信息计费类别
SP_DOMAIN	VARCHAR(100),	 --	SP域名
URL	VARCHAR(1000),	 --	访问的URL
IMEI	VARCHAR(50),	 --	终端类型
IMSI	VARCHAR(15),	 --	IMSI
SGSN_IP_ADDRESS	VARCHAR(100),	 --	SGSN的IP地址
AGRMNT_PROTO_TYPE	INTEGER,	 --	承载协议类型
UP_FLOW	INTEGER,	 --	上行流量
DOWN_FLOW	INTEGER,	 --	下行流量
PORT	INTEGER,	 --	目标端口
CAUSE_CLOSE	INTEGER,	 --	终止原因
DURATION	INTEGER,	 --	时长
PROTO_TYPE	VARCHAR(12),	 --	协议类型
REQUEST_TYPE	VARCHAR(12)	 --	请求的方法类型
)
  DATA CAPTURE NONE
 IN TBS_CDR_DATA
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_NUMBER
   ) USING HASHING


select * from app.sch_control_task 
select count(0),count(distinct table_id) from usys_table_maintain
 

select * from etl_load_table_map
select count(0),count(distinct task_id) from etl_load_table_map

select * from usys_table_maintain
where table_id > 10170
order by 1



select * from DWD_NG2_I05042_YYYYMMDD

select count(0) from VGOP_15304_20101116

select * from VGOP_15304_20101116

select * from VGOP_14201_20101229

select * from syscat.tableS where tabname like '%VGOP%14208%'
ORDER BY 2

select * from VGOP_14203_2010122923

select * from syscat.tableS where tabname like '%VGOP%15304%'
ORDER BY 1


select * from DWD_NG2_I03021_20101202




select distinct op_id from DWD_NG2_A01001_20101202

select * from DWD_NG2_A01001_20101202



select * from DWD_NG2_I01002_20101202
select * from DWD_NG2_I01003_20101202
select * from DWD_NG2_I01035_20101202
select * from DWD_NG2_A02024_20101202
select * from DWD_NG2_A02025_20101202
select * from DWD_NG2_A02026_20101202
select * from DWD_NG2_A02027_20101202

DWD_NG2_A01001_YYYYMMDD 
DWD_NG2_I01002_YYYYMMDD
DWD_NG2_I01003_YYYYMMDD
DWD_NG2_I01035_YYYYMMDD
DWD_NG2_A02024_YYYYMMDD
DWD_NG2_A02025_YYYYMMDD
DWD_NG2_A02026_YYYYMMDD
DWD_NG2_A02027_YYYYMMDD



select * from DWD_NG2_I03013_20101202
select * from DWD_NG2_I03027_20101202






DWD_NG2_I03021_YYYYMMDD
DWD_NG2_M03023_YYYYMM
DWD_NG2_I03027_YYYYMMDD




select count(0) 
select * from ODS_CDR_SMS_20100531



DWD_NG2_I05042_YYYYMMDD



select * from ODS_DIM_SYS_ORGANIZE_20101221 where organize_id = 91000728

sele



select * from tmp_pzw_M99904_2

select count(0)
from  DW_PRODUCT_201005 a,
  xzbass2.dw_cust_201011 b where  a.cust_id = b.cust_id
  with ur 
  
  
select * from XZBASS2.dw_product_201011  

select CALLTYPE_ID , count(0)
from BASS2.dw_call_cell_201011 
group by CALLTYPE_ID

CALLTYPE_ID	2
0	68214576
1	59060492
2	19740
3	91106

select * from dim_pub_calltype


select ROAMTYPE_ID , count(0)
from BASS2.dw_call_cell_201011 
group by ROAMTYPE_ID

ROAMTYPE_ID	2
0	106850515
1	12807968
4	7709922
5	635
6	14579
9	2295


select PLAN_ID , count(0)
from BASS2.dw_call_cell_201011 
group by PLAN_ID

PLAN_ID	2
89140007	10146365
89150010	6862087
89110012	5797925
89110007	5179204
89110005	4455928
89150007	4405042
89110013	3766266
89140003	3585941
89650010	3172955
89550010	2526151
89210005	2450228
89250010	2434473
89510012	2423635
89150012	2333829
89350010	2283827
89110001	2220959
89150006	2205472
89510005	2165570
89150003	2135203
89210012	2051239
89640007	1797924
89240007	1733729
89540007	1643902
89250011	1548282
89440007	1542535
89650007	1519739
89550011	1486866
89240003	1440409
89550007	1423530
89450010	1328848
89310012	1259142
89410012	1211733
89250007	1201738
89610013	1065245
89440003	1009642
89410005	970670
89310001	892711
89610007	891097
89150011	864553
89540003	835813
89110006	829273
89250012	827089
89410007	826689
89110011	810638
89310005	799551
89350007	788831
89550012	757112
89410006	741598
89450012	674281
89350012	618339
89640003	607802
89510013	604546
89650011	589746
89650006	575135
89650012	567392
89210007	562918
89410013	551569
89650003	513378
89250003	499926
89710005	489921
89740007	488188
89610005	485013
89550003	475399
89350011	467327
89610012	461926
89340007	453913
89310007	451446
89750012	445857
89450007	423362
89340003	420539
89310006	418276
89710013	407777
89750010	402697
89710012	381156
89510007	381000
89210013	378169
89550006	368689
89350003	361016
89710007	349680
89410011	335080
89740003	322382
89140004	309137
89110003	305933
89150009	303257
89350006	295741
89310013	278011
89450006	276245
89110010	246755
89450011	236721
89250009	234853
89250006	226763
89450003	223859
89150016	223007
89610006	222409
89140002	206429
89550009	178005
89250008	157116
89750011	153924
89350009	149969
89250013	114859
89750003	101320
89510006	99411
89410002	98376
89410003	97021
89140008	94152
89450013	83358
89350008	83169
89350013	80851
89210001	80076
89540008	76820
89550001	67448
89710006	63571
89510010	63025
89210006	62485
89610003	61798
89250001	61453
89240002	59607
89140005	59345
89410001	55363
89450009	52924
89750006	52599
89710003	50716
89250002	50129
89750007	48317
89150013	45736
89170001	40821
89450016	39622
89710001	39231
89510011	37800
89210003	35598
89180001	34874
89350001	33378
89610010	33240
89450008	29979
89640002	29523
89510001	28467
89650013	28434
89510003	25431
89310003	24962
89410010	24697
89340002	24387
89610011	23122
89550008	22827
89650008	21997
89740002	20380
89550013	20091
89170002	19132
89150001	18656
89210011	16999
89240008	16837
89240006	15994
89610001	15894
89440002	15250
89110002	14338
89440008	14253
89310010	14052
89240005	13961
89640008	13461
89340008	12789
89270002	11876
89710011	11771
89140006	11520
89150008	10642
89650001	10313
89540002	10029
89210010	9942
89450001	9525
89740008	9414
89710010	9324
89350002	9170
89710002	7965
89340005	7410
89670002	6618
89640006	6591
89670001	6261
89750009	5991
89240004	5890
89540006	5745
89570001	5725
89450002	5713
89310002	5686
89750013	5533
89270001	5236
89440006	4815
89440004	4630
89470001	4383
89610002	4339
89750016	4206
89550017	4036
89570002	3162
89140001	2960
89310011	2903
89640004	2773
89370001	2748
89250016	2690
89370002	2619
89650002	2568
89450017	2509
89340004	2327
89110009	2294
89440005	2285
89110008	2219
89740005	2122
89750001	1982
89540005	1804
89340006	1761
89640005	1734
89210002	1685
89470002	1676
89650016	1573
89650009	1571
89550002	1417
89550016	1402
89350016	1286
89250014	1220
89750002	1186
89510002	1089
89610008	1032
89770002	972
89550014	812
89770001	774
89640001	649
89540004	612
89410008	584
89710008	559
89150014	537
89240001	415
89210008	403
89450014	392
89740006	382
89650014	370
89740004	351
89350014	280
89340001	269
89740001	243
89750008	221
89750014	193
89150002	158
89650004	103
89410004	68
89510009	64
0	29
89440001	3
931243	2



create view tmp_pzw_M99904_2
as
select t2.*
from 
(
select t1.*,row_number()over(partition by t1.SCHMARKER order by user_id ) rn2
from 
    (
    select a.*,row_number()over(partition by user_id order by a.user_id ) rn1
    from tmp_pzw_M99904 a
    ) t1 where rn1 = 1 
) t2 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  t2.SCHMARKER = c.SCHOOL_ID 
where   t2.rn2 <= c.cnt      

select count(0),count(distinct user_id ) from tmp_pzw_M99904_2



drop table tmp_pzw_M99904_2
create table tmp_pzw_M99904_2
(

        USER_ID                 VARCHAR(20)         
        ,SCHMARKER               VARCHAR(32)         
        ,GROUPCODE               VARCHAR(20)         
        ,SCHNAME                 VARCHAR(200)        
        ,RN1                     BIGINT       
        ,RN2                     BIGINT           
)
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   (USER_ID)				
 USING HASHING
insert into tmp_pzw_M99904_2
select t2.*
from 
(
select t1.*,row_number()over(partition by t1.SCHMARKER order by user_id ) rn2
from 
    (
    select a.*,row_number()over(partition by user_id order by a.user_id ) rn1
    from tmp_pzw_M99904 a
    ) t1 where rn1 = 1 
) t2 
join  (select SCHOOL_ID,max(SCH_SIZE_STUS) cnt from  xzbass2.Dim_xysc_maintenance_info 
        group by SCHOOL_ID ) c on  t2.SCHMARKER = c.SCHOOL_ID 
where   t2.rn2 <= c.cnt     

                       

--rename  DW_ENTERPRISE_UNIPAY_DM_200802				to DW_ENTERPRISE_UNIPAY_DM_200802_BAK                 
--rename  DW_ENTERPRISE_UNIPAY_DM_200803_BAK          to DW_ENTERPRISE_UNIPAY_DM_200803_BAK_BAK          
rename  DW_ENTERPRISE_UNIPAY_DM_200804              to DW_ENTERPRISE_UNIPAY_DM_200804_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200805              to DW_ENTERPRISE_UNIPAY_DM_200805_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200805_BAK          to DW_ENTERPRISE_UNIPAY_DM_200805_BAK_BAK          
rename  DW_ENTERPRISE_UNIPAY_DM_200806              to DW_ENTERPRISE_UNIPAY_DM_200806_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200807              to DW_ENTERPRISE_UNIPAY_DM_200807_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200808              to DW_ENTERPRISE_UNIPAY_DM_200808_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200809              to DW_ENTERPRISE_UNIPAY_DM_200809_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200810              to DW_ENTERPRISE_UNIPAY_DM_200810_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200811              to DW_ENTERPRISE_UNIPAY_DM_200811_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200812              to DW_ENTERPRISE_UNIPAY_DM_200812_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200901              to DW_ENTERPRISE_UNIPAY_DM_200901_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200901_BAK          to DW_ENTERPRISE_UNIPAY_DM_200901_BAK_BAK          
rename  DW_ENTERPRISE_UNIPAY_DM_200902              to DW_ENTERPRISE_UNIPAY_DM_200902_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200903              to DW_ENTERPRISE_UNIPAY_DM_200903_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200904              to DW_ENTERPRISE_UNIPAY_DM_200904_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200905              to DW_ENTERPRISE_UNIPAY_DM_200905_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200906              to DW_ENTERPRISE_UNIPAY_DM_200906_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200907              to DW_ENTERPRISE_UNIPAY_DM_200907_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200908              to DW_ENTERPRISE_UNIPAY_DM_200908_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200909              to DW_ENTERPRISE_UNIPAY_DM_200909_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200910              to DW_ENTERPRISE_UNIPAY_DM_200910_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200911              to DW_ENTERPRISE_UNIPAY_DM_200911_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_200912              to DW_ENTERPRISE_UNIPAY_DM_200912_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201001              to DW_ENTERPRISE_UNIPAY_DM_201001_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201002              to DW_ENTERPRISE_UNIPAY_DM_201002_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201003              to DW_ENTERPRISE_UNIPAY_DM_201003_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201004              to DW_ENTERPRISE_UNIPAY_DM_201004_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201005              to DW_ENTERPRISE_UNIPAY_DM_201005_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201005_BAKNGGJ      to DW_ENTERPRISE_UNIPAY_DM_201005_BAKNGGJ_BAK      
rename  DW_ENTERPRISE_UNIPAY_DM_201006              to DW_ENTERPRISE_UNIPAY_DM_201006_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201006_BAKNGGJ      to DW_ENTERPRISE_UNIPAY_DM_201006_BAKNGGJ_BAK      
rename  DW_ENTERPRISE_UNIPAY_DM_201007              to DW_ENTERPRISE_UNIPAY_DM_201007_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201008              to DW_ENTERPRISE_UNIPAY_DM_201008_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201009              to DW_ENTERPRISE_UNIPAY_DM_201009_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201010              to DW_ENTERPRISE_UNIPAY_DM_201010_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201010_1020BAK      to DW_ENTERPRISE_UNIPAY_DM_201010_1020BAK_BAK      
rename  DW_ENTERPRISE_UNIPAY_DM_201011              to DW_ENTERPRISE_UNIPAY_DM_201011_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201011_1111BAK      to DW_ENTERPRISE_UNIPAY_DM_201011_1111BAK_BAK      
rename  DW_ENTERPRISE_UNIPAY_DM_201012              to DW_ENTERPRISE_UNIPAY_DM_201012_BAK              
rename  DW_ENTERPRISE_UNIPAY_DM_201101              to DW_ENTERPRISE_UNIPAY_DM_201101_BAK              



select * from DWD_NG2_I05002_20101101
select * from DWD_NG2_I05002_20101202

select * from syscat.tables where tabname like 'DWD_NG2_I05002_%'
                      

db2 "load client from /bassapp/bass2/panzw2/P99999.txt \
of del modified by coldel$ timestampformat=\"YYYYMMDDHHMMSS\" fastparse anyorder \
warningcount 1000    \
replace into DWD_NG2_I03028_20101020"
                      
                     

select count(0) from ODS_PRODUCT_UNITE_CANCEL_ORDER_20110124
select * from ODS_PRODUCT_UNITE_CANCEL_ORDER_yyyymmdd
select * from usys_table_maintain where table_id > 10110
update usys_table_maintain set PARTITION_KEY = 'SMS_ID,SP_ID' where table_id = 10182
and table_name = 'ODS_PRODUCT_UNITE_CANCEL_ORDER'

insert into USYS_TABLE_MAINTAIN values(10182,'统一退订业务情况','ODS_PRODUCT_UNITE_CANCEL_ORDER','1','day',255,'0','','','BASS2','ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD','TBS_ODS_OTHER','TBS_INDEX','sms_id,sp_id',1)

drop table "ODS_PRODUCT_UNITE_CANCEL_ORDER_yyyymmdd"
drop table "ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD"

create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110131 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially

insert into app.sch_control_task values ('TR1_L_02031',1,2,'ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD',0,-1,'统一退订业务情况','-','TR1_BOSS',2,'-')

select * from syscat.tables where tabname like 'ODS_PRODUCT_UNITE_CANCEL_ORDER%'

INSERT INTO etl_load_table_map(TASK_ID,TABLE_NAME_TEMPLET,TASK_NAME,LOAD_METHOD,BOSS_TABLE_NAME) 
 values( 'I02031','ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD','统一退订业务情况', 0,'kf.tongyi_tuiding')
 drop table bass2.ODS_PRODUCT_UNITE_CANCEL_ORDER_yyyymmdd
 
 CREATE TABLE bass2.ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD
 ("SMS_ID"       BIGINT,
  "PHONE_ID"     VARCHAR(20),
  "SP_ID"        BIGINT,
  "SP_CODE"      VARCHAR(50),
  "NAME"         VARCHAR(300),
  "CREATE_DATE"  timestamp,
  "OFFERID"      VARCHAR(40),
  "OFFERINSID"   VARCHAR(40),
  "STS"          SMALLINT
 )
  DATA CAPTURE NONE
 IN "TBS_ODS_OTHER"
 INDEX IN "TBS_INDEX"
  PARTITIONING KEY
   ( SMS_ID,SP_ID) USING HASHING
   
 
 
select * from ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD
SELECT MAX(TABLE_ID) FROM USYS_TABLE_MAINTAIN


create table t_DW_OW_LOADING_LOG_stats(
name varchar(100)
,node_id integer
,using_num integer
)
in tbs_3h
partitioning key (name,node_id)
using hashing


select a.node_id,b.name old ,b.USING_NUM old_num,a.name new,a.USING_NUM new_num
from (select  *  from t_DW_OW_LOADING_LOG_stats where name not like '%BAK') a
join 
(select  *  from t_DW_OW_LOADING_LOG_stats where name  like '%BAK') b
on a.name||'_BAK' = B.NAME  and a.node_id = b.node_id
order by 2,1


I02026

select * from app.sch_control_task where control_code like '%02031%'
select * from etl_load_table_map where task_id like '%I02031%'
select * from t_DW_OW_LOADING_LOG_stats

insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200801_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200801_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200802_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200802_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200805_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200805_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200806_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200806_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200807_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200807_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200808_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200808_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200809_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200809_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200810_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200810_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200811_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200811_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200812_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200812_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200901_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200901_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200903_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200903_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200904_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200904_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200905_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200905_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200906_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200906_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200907_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200907_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200908_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200908_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200909_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200909_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200910_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200910_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200911_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200911_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200912_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200912_BAK group by nodenumber(PRODUCT_NO) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_201001_BAK', nodenumber(PRODUCT_NO) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_201001_BAK group by nodenumber(PRODUCT_NO) 




insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200801', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200801 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200802', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200802 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200805', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200805 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200806', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200806 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200807', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200807 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200808', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200808 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200809', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200809 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200810', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200810 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200811', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200811 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200812', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200812 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200901', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200901 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200903', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200903 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200904', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200904 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200905', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200905 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200906', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200906 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200907', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200907 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200908', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200908 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200909', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200909 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200910', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200910 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200911', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200911 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_200912', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_200912 group by nodenumber(ID) 
insert into t_DW_OW_LOADING_LOG_stats select 'DW_OW_LOADING_LOG_201001', nodenumber(ID) ,count(*) as using_num from bass2.DW_OW_LOADING_LOG_201001 group by nodenumber(ID) 



create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110101 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110102 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110103 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110104 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110105 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110106 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110107 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110108 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110109 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110110 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110111 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110112 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110113 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110114 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110115 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110116 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110117 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110118 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110119 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110120 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110121 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110122 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110123 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110124 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110125 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110126 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110127 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110128 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110129 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110130 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially
create table ODS_PRODUCT_UNITE_CANCEL_ORDER_20110131 like ODS_PRODUCT_UNITE_CANCEL_ORDER_YYYYMMDD in TBS_ODS_OTHER index in tbs_index partitioning key ( SMS_ID,SP_ID ) using hashing not logged initially                     


select * from dim_boss_brand


select * from syscat.tables 

select * from DWD_NG2_P99905_20101202 WITH UR 

select * from dw_enterprise_member_mid_20101130

select * from syscat.tables where tabname like '%DW_ENTERPRISE_MEMBER_MID%' order by 2

describe table dw_enterprise_member_mid_20101130




/**	2010-12-9 17:30	added by  panzhiwei		**/
--
DROP TABLE DWD_NG2_P99905_YYYYMMDD				
CREATE TABLE DWD_NG2_P99905_YYYYMMDD (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE          decimal(10,2)             --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING		
 
 --DROP TABLE DWD_NG2_P99905_20101202				
CREATE TABLE DWD_NG2_P99905_20101202 (				
        LACCODE             VARCHAR(64)           --LAC_CD
        ,CELLCODE           VARCHAR(64)          --CELL_CD
        ,REGION             VARCHAR(8)          --归属地市
        ,SCHMARKER          VARCHAR(32)         --学校标识
        ,SCHLONG            VARCHAR(32)             --经度
        ,SCHLAT             VARCHAR(32)             --纬度
        ,COVERRATE         decimal(10,2)              --基站学校覆盖率
        ,EFF_DATE           TIMESTAMP           --生效日期
        ,EXP_DATE           TIMESTAMP           --失效日期
        ,SCHNAME			VARCHAR(200)		--学校名称        
 )				
  DATA CAPTURE NONE				
 IN TBS_3H				
 INDEX IN TBS_INDEX				
  PARTITIONING KEY				
   ( LACCODE,CELLCODE )				
 USING HASHING		
 




select * from DWD_NG2_P99905_20101202



select * from dw_cust_20101130

